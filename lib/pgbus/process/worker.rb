# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Worker
      include SignalHandler

      attr_reader :queues, :threads, :config

      def initialize(queues:, threads: 5, config: Pgbus.configuration,
                     single_active_consumer: false, consumer_priority: 0)
        @queues = Array(queues)
        @wildcard = @queues.include?("*")
        @threads = threads
        @config = config
        @single_active_consumer = single_active_consumer
        @consumer_priority = consumer_priority
        @lifecycle = Lifecycle.new
        @last_wildcard_resolve = nil
        @jobs_processed = Concurrent::AtomicFixnum.new(0)
        @jobs_failed = Concurrent::AtomicFixnum.new(0)
        @in_flight = Concurrent::AtomicFixnum.new(0)
        @rate_counter = RateCounter.new(:processed, :failed, :dequeued)
        @started_at = Time.current
        @started_at_monotonic = monotonic_now
        @stat_buffer = config.stats_enabled ? Pgbus::StatBuffer.new : nil
        @executor = Pgbus::ActiveJob::Executor.new(stat_buffer: @stat_buffer)
        @pool = Concurrent::FixedThreadPool.new(threads)
        @circuit_breaker = Pgbus::CircuitBreaker.new(config: config)
        @queue_lock = QueueLock.new if @single_active_consumer
        @wake_signal = WakeSignal.new
      end

      def stats
        {
          jobs_processed: @jobs_processed.value,
          jobs_failed: @jobs_failed.value,
          in_flight: @in_flight.value,
          state: @lifecycle.state,
          consumer_priority: @consumer_priority,
          single_active_consumer: @single_active_consumer,
          locked_queues: @queue_lock&.held_queues || [],
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
          refresh_wildcard_queues

          break if @lifecycle.stopped?
          break if @lifecycle.draining? && @pool.queue_length.zero?

          claim_and_execute if @lifecycle.can_process?
          @stat_buffer&.flush_if_due
          @wake_signal.wait(timeout: config.polling_interval) if @lifecycle.draining? || @lifecycle.paused?
        end

        shutdown
      end

      def graceful_shutdown
        Pgbus.logger.info { "[Pgbus] Worker shutting down gracefully..." }
        Pgbus.stopping = true
        @lifecycle.transition_to(:draining)
        @wake_signal.notify!
      end

      def immediate_shutdown
        Pgbus.logger.warn { "[Pgbus] Worker shutting down immediately!" }
        Pgbus.stopping = true
        @lifecycle.transition_to!(:stopped)
        @wake_signal.notify!
        @pool.kill
      end

      WILDCARD_REFRESH_INTERVAL = 30 # seconds

      # Matches the physical queue name inside a "relation \"pgmq.q_foo\" does
      # not exist" error. Frozen module constant to avoid recompiling the
      # regex on every queue-missing error in hot fetch/read paths.
      MISSING_QUEUE_REGEX = /pgmq\.q_(\w+)/

      private

      def claim_and_execute
        poll_interval = effective_polling_interval

        idle = @pool.max_length - @pool.queue_length
        return @wake_signal.wait(timeout: poll_interval) if idle <= 0

        if config.prefetch_limit
          available = config.prefetch_limit - @in_flight.value
          return @wake_signal.wait(timeout: poll_interval) if available <= 0

          idle = [idle, available].min
        end

        tagged_messages = fetch_messages(idle)

        if tagged_messages.empty?
          @wake_signal.wait(timeout: poll_interval)
          return
        end

        @rate_counter.increment(:dequeued, tagged_messages.size)
        tagged_messages.each do |queue_name, message, source_queue|
          @in_flight.increment
          @pool.post { process_message(message, queue_name, source_queue: source_queue) }
        end
      end

      # Returns an array of [queue_name, message] pairs so we always know
      # which queue each message came from.
      def fetch_messages(qty)
        active_queues = queues.reject { |q| @circuit_breaker.paused?(q) }
        active_queues = active_queues.select { |q| @queue_lock.try_lock(q) } if @single_active_consumer
        return [] if active_queues.empty?

        if priority_enabled?
          fetch_prioritized(active_queues, qty)
        elsif active_queues.size == 1
          queue = active_queues.first
          messages = Pgbus.client.read_batch(queue, qty: qty) || []
          messages.map { |m| [queue, m] }
        else
          fetch_multi(active_queues, qty)
        end
      rescue StandardError => e
        if undefined_queue_table_error?(e)
          evict_missing_queues(e)
        else
          Pgbus.logger.error { "[Pgbus] Error fetching messages: #{e.message}" }
        end
        []
      end

      # Detect "queue table missing" via the underlying PG::UndefinedTable
      # cause when available. Falls back to a guarded message check that
      # requires BOTH "pgmq.q_" (so we know it's our queue table) and
      # "does not exist", which keeps the eviction logic working for
      # adapters/exception wrappers that don't preserve the original
      # PG::UndefinedTable as #cause (e.g. PGMQ::Errors::ConnectionError
      # raised by pgmq-ruby's auto-reconnect path). Locale-fragile, but
      # this is gated by the very specific "pgmq.q_" prefix so a false
      # positive can only come from another error mentioning that exact
      # string — which is itself a queue-table error worth handling.
      def undefined_queue_table_error?(error)
        cause = error.respond_to?(:cause) ? error.cause : nil
        return true if defined?(PG::UndefinedTable) && cause.is_a?(PG::UndefinedTable)
        return true if error.message.include?("pgmq.q_") && error.message.include?("does not exist")

        false
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

      # Use pgmq-ruby's read_multi to read from all queues in a single
      # SQL query (UNION ALL). Each returned message carries a queue_name
      # field so we can map it back to the logical queue.
      def fetch_multi(active_queues, qty)
        messages = Pgbus.client.read_multi(active_queues, qty: qty) || []
        prefix = "#{config.queue_prefix}_"

        messages.map do |m|
          logical = m.queue_name&.delete_prefix(prefix) || active_queues.first
          [logical, m]
        end
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
      def resolve_wildcard_queues
        return unless @wildcard

        dlq_suffix = Pgbus::DEAD_LETTER_SUFFIX
        prefix = "#{config.queue_prefix}_"

        conn = Pgbus.configuration.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection
        all_queues = conn.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
        resolved = all_queues
                   .reject { |q| q.end_with?(dlq_suffix) }
                   .map { |q| q.delete_prefix(prefix) }

        if resolved.empty?
          Pgbus.logger.warn { "[Pgbus] Wildcard queue '*' resolved to no queues — falling back to default" }
          @queues = [config.default_queue]
        else
          if @last_wildcard_resolve && resolved != @queues
            Pgbus.logger.info { "[Pgbus] Wildcard queues changed: #{@queues.join(", ")} → #{resolved.join(", ")}" }
          end
          @queues = resolved
          Pgbus.logger.info { "[Pgbus] Wildcard queue '*' resolved to: #{@queues.join(", ")}" } unless @last_wildcard_resolve
        end
        @last_wildcard_resolve = monotonic_now
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Failed to resolve wildcard queues: #{e.message} — falling back to default" }
        @queues = [config.default_queue] unless @last_wildcard_resolve
      end

      # Periodically re-resolve wildcard queues to pick up new queues and
      # drop deleted ones without requiring a worker restart.
      def refresh_wildcard_queues
        return unless @wildcard
        return if @last_wildcard_resolve && (monotonic_now - @last_wildcard_resolve) < WILDCARD_REFRESH_INTERVAL

        resolve_wildcard_queues
      end

      # When a "relation does not exist" error occurs, the queue was deleted.
      # Extract the queue name from the error and remove it from the active list.
      def evict_missing_queues(error)
        prefix = "#{config.queue_prefix}_"
        if (match = MISSING_QUEUE_REGEX.match(error.message))
          physical_name = match[1]
          logical_name = physical_name.delete_prefix(prefix)
          if @queues.delete(logical_name)
            Pgbus.logger.warn { "[Pgbus] Evicted deleted queue '#{logical_name}' (#{physical_name}) from worker" }
          end
        end
        Pgbus.logger.error { "[Pgbus] Queue table missing: #{error.message}" }
      end

      def check_recycle
        return unless @lifecycle.running? && recycle_needed?

        Pgbus.stopping = true
        @lifecycle.transition_to(:draining)
        @wake_signal.notify!
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
        return false unless config.max_worker_lifetime && (monotonic_now - @started_at_monotonic) > config.max_worker_lifetime

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

      def effective_polling_interval
        return config.polling_interval if @consumer_priority.zero?

        ConsumerPriority.effective_polling_interval(
          base_interval: config.polling_interval,
          my_priority: @consumer_priority,
          max_priority: ConsumerPriority.max_active_priority(queues, ::Process.pid)
        )
      rescue StandardError
        config.polling_interval
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "worker",
          metadata: {
            queues: queues, threads: threads, pid: ::Process.pid,
            consumer_priority: @consumer_priority
          },
          on_beat: -> { @rate_counter.snapshot! }
        )
        @heartbeat.start
      end

      def shutdown
        Pgbus.logger.info { "[Pgbus] Worker draining thread pool..." }
        @pool.shutdown
        @pool.wait_for_termination(30)
        @stat_buffer&.stop
        @queue_lock&.unlock_all
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Worker stopped. Processed: #{@jobs_processed.value}, Failed: #{@jobs_failed.value}" }
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end
    end
  end
end
