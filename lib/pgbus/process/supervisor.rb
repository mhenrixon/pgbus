# frozen_string_literal: true

module Pgbus
  module Process
    class Supervisor
      include SignalHandler

      FORK_WAIT = 1 # seconds between fork checks

      attr_reader :config

      def initialize(config: Pgbus.configuration)
        @config = config
        @forks = {}
        @shutting_down = false
      end

      def run
        setup_signals
        start_heartbeat

        Pgbus.logger.info { "[Pgbus] Supervisor starting pid=#{::Process.pid}" }

        boot_processes
        monitor_loop
      ensure
        shutdown
      end

      def graceful_shutdown
        Pgbus.logger.info { "[Pgbus] Supervisor: graceful shutdown requested" }
        @shutting_down = true
        signal_children("TERM")
      end

      def immediate_shutdown
        Pgbus.logger.warn { "[Pgbus] Supervisor: immediate shutdown requested" }
        @shutting_down = true
        signal_children("QUIT")
      end

      private

      def boot_processes
        # Boot workers
        config.workers.each do |worker_config|
          fork_worker(worker_config)
        end

        # Boot dispatcher
        fork_dispatcher

        # Boot event consumers if configured
        boot_consumers
      end

      def fork_worker(worker_config)
        queues = worker_config[:queues] || worker_config["queues"] || [config.default_queue]
        threads = worker_config[:threads] || worker_config["threads"] || 5

        pid = fork do
          restore_signals
          setup_child_signals
          load_rails_app
          worker = Worker.new(queues: queues, threads: threads, config: config)
          worker.run
        end

        @forks[pid] = { type: :worker, config: worker_config }
        Pgbus.logger.info { "[Pgbus] Forked worker pid=#{pid} queues=#{queues.join(",")}" }
      end

      def fork_dispatcher
        pid = fork do
          restore_signals
          setup_child_signals
          load_rails_app
          dispatcher = Dispatcher.new(config: config)
          dispatcher.run
        end

        @forks[pid] = { type: :dispatcher }
        Pgbus.logger.info { "[Pgbus] Forked dispatcher pid=#{pid}" }
      end

      def boot_consumers
        return unless config.event_consumers

        config.event_consumers.each do |consumer_config|
          fork_consumer(consumer_config)
        end
      end

      def fork_consumer(consumer_config)
        topics = consumer_config[:topics] || consumer_config["topics"]
        threads = consumer_config[:threads] || consumer_config["threads"] || 3

        pid = fork do
          restore_signals
          setup_child_signals
          load_rails_app
          consumer = Consumer.new(topics: topics, threads: threads, config: config)
          consumer.run
        end

        @forks[pid] = { type: :consumer, config: consumer_config }
        Pgbus.logger.info { "[Pgbus] Forked consumer pid=#{pid} topics=#{topics.join(",")}" }
      end

      def monitor_loop
        loop do
          break if @shutting_down && @forks.empty?

          process_signals
          reap_children
          interruptible_sleep(FORK_WAIT)
        end
      end

      def reap_children
        loop do
          pid, status = ::Process.waitpid2(-1, ::Process::WNOHANG)
          break unless pid

          info = @forks.delete(pid)
          next unless info

          if @shutting_down
            Pgbus.logger.info { "[Pgbus] Child #{info[:type]} pid=#{pid} exited (status=#{status.exitstatus})" }
          else
            Pgbus.logger.warn do
              "[Pgbus] Child #{info[:type]} pid=#{pid} exited unexpectedly (status=#{status&.exitstatus}), restarting..."
            end
            restart_child(info)
          end
        rescue Errno::ECHILD
          break
        end
      end

      def restart_child(info)
        case info[:type]
        when :worker
          fork_worker(info[:config])
        when :dispatcher
          fork_dispatcher
        when :consumer
          fork_consumer(info[:config])
        end
      end

      def signal_children(sig)
        @forks.each_key do |pid|
          ::Process.kill(sig, pid)
        rescue Errno::ESRCH
          # Process already gone
        end
      end

      def setup_child_signals
        %w[INT TERM QUIT].each do |sig|
          trap(sig) { @shutting_down = true }
        end
      end

      def load_rails_app
        return unless defined?(Rails)

        Rails.application.eager_load! if Rails.application.respond_to?(:eager_load!)
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "supervisor",
          metadata: { pid: ::Process.pid, hostname: Socket.gethostname }
        )
        @heartbeat.start
      end

      def shutdown
        # Wait for all children with timeout
        deadline = Time.now + 30

        until @forks.empty? || Time.now > deadline
          reap_children
          interruptible_sleep(0.5)
        end

        # Force kill any remaining
        signal_children("KILL") unless @forks.empty?

        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Supervisor stopped" }
      end
    end
  end
end
