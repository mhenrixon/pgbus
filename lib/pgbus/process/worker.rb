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
        @lifecycle = Lifecycle.new
        @jobs_processed = Concurrent::AtomicFixnum.new(0)
        @jobs_failed = Concurrent::AtomicFixnum.new(0)
        @in_flight = Concurrent::AtomicFixnum.new(0)
        @rate_counter = RateCounter.new(:processed, :failed, :dequeued)
        @started_at = Time.now
        @executor = Pgbus::ActiveJob::Executor.new
        @pool = Concurrent::FixedThreadPool.new(threads)
        @circuit_breaker = Pgbus::CircuitBreaker.new(config: config)
      end

      def stats
        {
          jobs_processed: @jobs_processed.value,
          jobs_failed: @jobs_failed.value,
          in_flight: @in_flight.value,
          state: @lifecycle.state,
          rates: @rate_counter.rates,
          started_at: @started_at
        }
      end

      def run
        setup_signals
        start_heartbeat
        resolve_wildcard_queues
        @lifecycle.transition_to!(:running)
        Pgbus.logger.info { "[Pgbus] Worker started: queues=#{queues.join(",")} threads=#{threads} pid=#{::Process.pid}" }

        loop do
          process_signals
          check_recycle

          break if @lifecycle.stopped?
          break if @lifecycle.draining? && @pool.queue_length.zero?

          claim_and_execute if @lifecycle.can_process?
          interruptible_sleep(config.polling_interval) if @lifecycle.draining? || @lifecycle.paused?
        end

        shutdown
      end

      def graceful_shutdown
        Pgbus.logger.info { "[Pgbus] Worker shutting down gracefully..." }
        @lifecycle.transition_to(:draining)
      end

      def immediate_shutdown
        Pgbus.logger.warn { "[Pgbus] Worker shutting down immediately!" }
        @lifecycle.transition_to!(:stopped)
        @pool.kill
      end

      private

      def claim_and_execute
        idle = @pool.max_length - @pool.queue_length
        return interruptible_sleep(config.polling_interval) if idle <= 0

        if config.prefetch_limit
          available = config.prefetch_limit - @in_flight.value
          return interruptible_sleep(config.polling_interval) if available <= 0

          idle = [idle, available].min
        end

        tagged_messages = fetch_messages(idle)

        if tagged_messages.empty?
          interruptible_sleep(config.polling_interval)
          return
        end

        @rate_counter.increment(:dequeued, tagged_messages.size)
        tagged_messages.each do |queue_name, message, source_queue|
          @in_flight.increment
          @pool.post { process_message(message, queue_name, source_queue: source_queue) }
        end
      end

      # Returns an array of [queue_name, message] pairs so we always know
      # which queue each message came from (PGMQ messages don't carry this).
      def fetch_messages(qty)
        active_queues = queues.reject { |q| @circuit_breaker.paused?(q) }
        return [] if active_queues.empty?

        if priority_enabled?
          fetch_prioritized(active_queues, qty)
        elsif active_queues.size == 1
          queue = active_queues.first
          messages = Pgbus.client.read_batch(queue, qty: qty) || []
          messages.map { |m| [queue, m] }
        else
          per_queue = [(qty / active_queues.size.to_f).ceil, 1].max
          active_queues.flat_map do |q|
            (Pgbus.client.read_batch(q, qty: per_queue) || []).map { |m| [q, m] }
          end.first(qty)
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Error fetching messages: #{e.message}" }
        []
      end

      def fetch_prioritized(active_queues, qty)
        remaining = qty
        results = []

        active_queues.each do |q|
          break if remaining <= 0

          batch = Pgbus.client.read_batch_prioritized(q, qty: remaining)
          batch.each do |physical_queue, message|
            results << [q, message, physical_queue]
          end
          remaining -= batch.size
        end

        results
      end

      def priority_enabled?
        config.priority_levels && config.priority_levels > 1
      end

      def process_message(message, queue_name, source_queue: nil)
        result = @executor.execute(message, queue_name, source_queue: source_queue)
        @jobs_processed.increment
        @rate_counter.increment(:processed)
        if result == :failed
          @jobs_failed.increment
          @rate_counter.increment(:failed)
          @circuit_breaker.record_failure(queue_name)
        else
          @circuit_breaker.record_success(queue_name)
        end
      rescue StandardError => e
        @jobs_failed.increment
        @rate_counter.increment(:failed)
        @circuit_breaker.record_failure(queue_name)
        Pgbus.logger.error { "[Pgbus] Unhandled error processing message: #{e.message}" }
      ensure
        @in_flight.decrement
      end

      # Resolve "*" to all non-DLQ queues from pgmq.meta, stripping the prefix.
      # Called once at startup. If no wildcard, this is a no-op.
      def resolve_wildcard_queues
        return unless @queues.include?("*")

        dlq_suffix = config.dead_letter_queue_suffix
        prefix = "#{config.queue_prefix}_"

        conn = Pgbus.configuration.connects_to ? Pgbus::ApplicationRecord.connection : ActiveRecord::Base.connection
        all_queues = conn.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
        resolved = all_queues
                   .reject { |q| q.end_with?(dlq_suffix) }
                   .map { |q| q.delete_prefix(prefix) }

        if resolved.empty?
          Pgbus.logger.warn { "[Pgbus] Wildcard queue '*' resolved to no queues — falling back to default" }
          @queues = [config.default_queue]
        else
          @queues = resolved
          Pgbus.logger.info { "[Pgbus] Wildcard queue '*' resolved to: #{@queues.join(", ")}" }
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Failed to resolve wildcard queues: #{e.message} — falling back to default" }
        @queues = [config.default_queue]
      end

      def check_recycle
        return unless @lifecycle.running? && recycle_needed?

        @lifecycle.transition_to(:draining)
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
          metadata: { queues: queues, threads: threads, pid: ::Process.pid },
          on_beat: -> { @rate_counter.snapshot! }
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
