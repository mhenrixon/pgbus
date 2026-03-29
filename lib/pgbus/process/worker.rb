# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Worker
      include SignalHandler

      attr_reader :queues, :threads, :config, :stats

      def initialize(queues:, threads: 5, config: Pgbus.configuration)
        @queues = Array(queues)
        @threads = threads
        @config = config
        @shutting_down = false
        @stats = { jobs_processed: 0, jobs_failed: 0, started_at: Time.now }
        @executor = Pgbus::ActiveJob::Executor.new
        @pool = Concurrent::FixedThreadPool.new(threads)
      end

      def run
        setup_signals
        start_heartbeat
        Pgbus.logger.info { "[Pgbus] Worker started: queues=#{queues.join(",")} threads=#{threads} pid=#{::Process.pid}" }

        loop do
          break if @shutting_down
          break if recycle_needed?

          process_signals
          claim_and_execute
        end

        shutdown
      end

      def graceful_shutdown
        Pgbus.logger.info { "[Pgbus] Worker shutting down gracefully..." }
        @shutting_down = true
      end

      def immediate_shutdown
        Pgbus.logger.warn { "[Pgbus] Worker shutting down immediately!" }
        @shutting_down = true
        @pool.kill
      end

      private

      def claim_and_execute
        idle = @pool.max_length - @pool.queue_length
        return sleep(config.polling_interval) if idle <= 0

        messages = fetch_messages(idle)

        if messages.empty?
          sleep(config.polling_interval)
          return
        end

        messages.each do |message|
          queue_name = message.respond_to?(:queue_name) ? message.queue_name : queues.first
          @pool.post { process_message(message, queue_name) }
        end
      end

      def fetch_messages(qty)
        if queues.size == 1
          Pgbus.client.read_batch(queues.first, qty: qty) || []
        else
          # Multi-queue read: read from each queue proportionally
          per_queue = [(qty / queues.size.to_f).ceil, 1].max
          queues.flat_map do |q|
            Pgbus.client.read_batch(q, qty: per_queue) || []
          end.first(qty)
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Error fetching messages: #{e.message}" }
        []
      end

      def process_message(message, queue_name)
        result = @executor.execute(message, queue_name)
        @stats[:jobs_processed] += 1
        @stats[:jobs_failed] += 1 if result == :failed
      rescue StandardError => e
        @stats[:jobs_failed] += 1
        Pgbus.logger.error { "[Pgbus] Unhandled error processing message: #{e.message}" }
      end

      def recycle_needed?
        if config.max_jobs_per_worker && @stats[:jobs_processed] >= config.max_jobs_per_worker
          Pgbus.logger.info { "[Pgbus] Worker recycling: max_jobs reached (#{@stats[:jobs_processed]})" }
          return true
        end

        if config.max_memory_mb && current_memory_mb > config.max_memory_mb
          Pgbus.logger.info { "[Pgbus] Worker recycling: memory limit exceeded (#{current_memory_mb}MB > #{config.max_memory_mb}MB)" }
          return true
        end

        if config.max_worker_lifetime && (Time.now - @stats[:started_at]) > config.max_worker_lifetime
          Pgbus.logger.info { "[Pgbus] Worker recycling: lifetime exceeded" }
          return true
        end

        false
      end

      def current_memory_mb
        if RUBY_PLATFORM.include?("darwin")
          `ps -o rss= -p #{::Process.pid}`.to_i / 1024
        else
          File.read("/proc/#{::Process.pid}/statm").split[1].to_i * 4096 / (1024 * 1024)
        rescue Errno::ENOENT
          0
        end
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "worker",
          metadata: { queues: queues, threads: threads, pid: ::Process.pid }
        )
        @heartbeat.start
      end

      def shutdown
        Pgbus.logger.info { "[Pgbus] Worker draining thread pool..." }
        @pool.shutdown
        @pool.wait_for_termination(30)
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Worker stopped. Processed: #{@stats[:jobs_processed]}, Failed: #{@stats[:jobs_failed]}" }
      end
    end
  end
end
