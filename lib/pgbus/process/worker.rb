# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Worker
      include SignalHandler

      attr_reader :queues, :threads, :config

      def initialize(queues:, threads: 5, config: Pgbus.configuration)
        @queues = Array(queues)
        @threads = threads
        @config = config
        @shutting_down = false
        @jobs_processed = Concurrent::AtomicFixnum.new(0)
        @jobs_failed = Concurrent::AtomicFixnum.new(0)
        @started_at = Time.now
        @executor = Pgbus::ActiveJob::Executor.new
        @pool = Concurrent::FixedThreadPool.new(threads)
      end

      def stats
        { jobs_processed: @jobs_processed.value, jobs_failed: @jobs_failed.value, started_at: @started_at }
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
        return interruptible_sleep(config.polling_interval) if idle <= 0

        tagged_messages = fetch_messages(idle)

        if tagged_messages.empty?
          interruptible_sleep(config.polling_interval)
          return
        end

        tagged_messages.each do |queue_name, message|
          @pool.post { process_message(message, queue_name) }
        end
      end

      # Returns an array of [queue_name, message] pairs so we always know
      # which queue each message came from (PGMQ messages don't carry this).
      def fetch_messages(qty)
        if queues.size == 1
          queue = queues.first
          messages = Pgbus.client.read_batch(queue, qty: qty) || []
          messages.map { |m| [queue, m] }
        else
          per_queue = [(qty / queues.size.to_f).ceil, 1].max
          queues.flat_map do |q|
            (Pgbus.client.read_batch(q, qty: per_queue) || []).map { |m| [q, m] }
          end.first(qty)
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Error fetching messages: #{e.message}" }
        []
      end

      def process_message(message, queue_name)
        result = @executor.execute(message, queue_name)
        @jobs_processed.increment
        @jobs_failed.increment if result == :failed
      rescue StandardError => e
        @jobs_failed.increment
        Pgbus.logger.error { "[Pgbus] Unhandled error processing message: #{e.message}" }
      end

      def recycle_needed?
        exceeded_max_jobs? || exceeded_max_memory? || exceeded_max_lifetime?
      end

      def exceeded_max_jobs?
        return false unless config.max_jobs_per_worker && @jobs_processed.value >= config.max_jobs_per_worker

        Pgbus.logger.info { "[Pgbus] Worker recycling: max_jobs reached (#{@jobs_processed.value})" }
        true
      end

      def exceeded_max_memory?
        return false unless config.max_memory_mb && current_memory_mb > config.max_memory_mb

        Pgbus.logger.info { "[Pgbus] Worker recycling: memory limit (#{current_memory_mb}MB > #{config.max_memory_mb}MB)" }
        true
      end

      def exceeded_max_lifetime?
        return false unless config.max_worker_lifetime && (Time.now - @started_at) > config.max_worker_lifetime

        Pgbus.logger.info { "[Pgbus] Worker recycling: lifetime exceeded" }
        true
      end

      def current_memory_mb
        if RUBY_PLATFORM.include?("darwin")
          `ps -o rss= -p #{::Process.pid}`.to_i / 1024
        else
          begin
            File.read("/proc/#{::Process.pid}/statm").split[1].to_i * 4096 / (1024 * 1024)
          rescue Errno::ENOENT
            0
          end
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
        Pgbus.logger.info { "[Pgbus] Worker stopped. Processed: #{@jobs_processed.value}, Failed: #{@jobs_failed.value}" }
      end
    end
  end
end
