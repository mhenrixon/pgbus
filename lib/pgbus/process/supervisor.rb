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

        # Boot recurring scheduler if configured
        boot_scheduler

        # Boot event consumers if configured
        boot_consumers

        # Boot outbox poller if configured
        boot_outbox_poller
      end

      def fork_worker(worker_config)
        queues = worker_config[:queues] || worker_config["queues"] || [config.default_queue]
        threads = worker_config[:threads] || worker_config["threads"] || 5
        single_active = worker_config[:single_active_consumer] || worker_config["single_active_consumer"] || false
        priority = worker_config[:consumer_priority] || worker_config["consumer_priority"] || 0

        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          worker = Worker.new(
            queues: queues, threads: threads, config: config,
            single_active_consumer: single_active, consumer_priority: priority
          )
          worker.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork worker for queues=#{queues.join(",")}" }
          return
        end

        @forks[pid] = { type: :worker, config: worker_config }
        Pgbus.logger.info { "[Pgbus] Forked worker pid=#{pid} queues=#{queues.join(",")}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        Pgbus.logger.error { "[Pgbus] Fork failed for worker: #{e.message}" }
      end

      def fork_dispatcher
        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          dispatcher = Dispatcher.new(config: config)
          dispatcher.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork dispatcher" }
          return
        end

        @forks[pid] = { type: :dispatcher }
        Pgbus.logger.info { "[Pgbus] Forked dispatcher pid=#{pid}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        Pgbus.logger.error { "[Pgbus] Fork failed for dispatcher: #{e.message}" }
      end

      def boot_scheduler
        return if config.skip_recurring
        return unless recurring_tasks_configured?

        fork_scheduler
      end

      def fork_scheduler
        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          load_recurring_config
          scheduler = Recurring::Scheduler.new(config: config)
          scheduler.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork scheduler" }
          return
        end

        @forks[pid] = { type: :scheduler }
        Pgbus.logger.info { "[Pgbus] Forked scheduler pid=#{pid}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        Pgbus.logger.error { "[Pgbus] Fork failed for scheduler: #{e.message}" }
      end

      def recurring_tasks_configured?
        return true if config.recurring_tasks&.any?
        return true if config.recurring_tasks_file && File.exist?(config.recurring_tasks_file.to_s)

        # Check default location
        if defined?(Rails)
          default_path = Rails.root.join("config", "recurring.yml")
          return File.exist?(default_path.to_s)
        end

        false
      end

      def load_recurring_config
        return if config.recurring_tasks&.any?

        path = config.recurring_tasks_file
        path ||= defined?(Rails) ? Rails.root.join("config", "recurring.yml") : nil
        return unless path && File.exist?(path.to_s)

        config.recurring_tasks = Recurring::ConfigLoader.load(path)
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
          setup_child_process
          load_rails_app
          consumer = Consumer.new(topics: topics, threads: threads, config: config)
          consumer.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork consumer for topics=#{topics.join(",")}" }
          return
        end

        @forks[pid] = { type: :consumer, config: consumer_config }
        Pgbus.logger.info { "[Pgbus] Forked consumer pid=#{pid} topics=#{topics.join(",")}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        Pgbus.logger.error { "[Pgbus] Fork failed for consumer: #{e.message}" }
      end

      def boot_outbox_poller
        return unless config.outbox_enabled

        fork_outbox_poller
      end

      def fork_outbox_poller
        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          poller = Outbox::Poller.new(config: config)
          poller.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork outbox poller" }
          return
        end

        @forks[pid] = { type: :outbox_poller }
        Pgbus.logger.info { "[Pgbus] Forked outbox poller pid=#{pid}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        Pgbus.logger.error { "[Pgbus] Fork failed for outbox poller: #{e.message}" }
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
        when :scheduler
          fork_scheduler
        when :consumer
          fork_consumer(info[:config])
        when :outbox_poller
          fork_outbox_poller
        end
      end

      def signal_children(sig)
        @forks.each_key do |pid|
          ::Process.kill(sig, pid)
        rescue Errno::ESRCH
          # Process already gone
        end
      end

      def setup_child_process
        # Reset the PGMQ client so this forked process gets a fresh
        # PG::Connection instead of inheriting the parent's (which is
        # in undefined state post-fork and not thread-safe to share).
        Pgbus.reset_client!
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
