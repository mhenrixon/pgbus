# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Consumer
      include SignalHandler

      attr_reader :topics, :threads, :config, :execution_mode,
                  :queue_names, :wake_signal, :notify_retry_backoff
      # notify_listener is writable so tests can simulate a start_notify_listener
      # success from inside a stub (production sets it in start_notify_listener).
      # notify_retry_at is writable so a test can re-arm the backoff window
      # between successive ensure_notify_listener calls.
      attr_accessor :notify_listener, :notify_retry_at

      def shutting_down?
        @shutting_down
      end

      def jobs_processed
        @jobs_processed.value
      end

      # Seed the processed-job counter. Used by tests to drive the recycle
      # thresholds without running thousands of real jobs; production only ever
      # increments it via the AtomicFixnum during message handling.
      def jobs_processed=(count)
        @jobs_processed.value = count
      end

      # Mirrors Worker::NOTIFY_FALLBACK_POLL_SECONDS. When a live NotifyListener
      # drives wake-up latency, the empty-read wait is a safety net rather than
      # the steady-state cadence, so it rises to this ceiling: one missed NOTIFY
      # costs bounded latency, never a stuck queue.
      NOTIFY_FALLBACK_POLL_SECONDS = 15

      # NotifyListener startup can fail on a transient boot-time condition (DB
      # restarting, failover, DNS blip) or its thread can die mid-run. The loop
      # retries from ensure_notify_listener on an exponential backoff:
      # NOTIFY_RETRY_BASE doubling up to NOTIFY_RETRY_MAX. Matches Worker.
      NOTIFY_RETRY_BASE_SECONDS = 5
      NOTIFY_RETRY_MAX_SECONDS = 300

      # The notify-listener lifecycle state (`notify_listener`, `notify_retry_at`,
      # `notify_retry_backoff`), the recycle clock (`started_at_monotonic`), and
      # the resolved subscription set (`queue_names`) accept injected seeds so
      # tests can drive the run loop from a known state without poking private
      # ivars. All default to the values production initializes to; `queue_names`
      # defaults to nil, meaning "derive from the registry in setup_subscriptions".
      def initialize(topics:, threads: 3, config: Pgbus.configuration, execution_mode: :threads,
                     queue_names: nil, notify_listener: nil, notify_retry_at: 0.0,
                     notify_retry_backoff: NOTIFY_RETRY_BASE_SECONDS,
                     started_at_monotonic: nil)
        @topics = Array(topics)
        @threads = threads
        @config = config
        @execution_mode = ExecutionPools.normalize_mode(execution_mode)
        @shutting_down = false
        @recycling = false
        @jobs_processed = Concurrent::AtomicFixnum.new(0)
        @started_at_monotonic = started_at_monotonic || monotonic_now
        @wake_signal = WakeSignal.new
        @pool = ExecutionPools.build(
          mode: @execution_mode,
          capacity: threads,
          on_state_change: -> { @wake_signal.notify! }
        )
        @registry = EventBus::Registry.instance
        @queue_names = queue_names
        @notify_listener = notify_listener
        @notify_retry_at = notify_retry_at
        @notify_retry_backoff = notify_retry_backoff
      end

      def run
        setup_signals
        start_heartbeat
        setup_subscriptions
        start_notify_listener
        Pgbus.logger.info do
          "[Pgbus] Consumer started: topics=#{topics.join(",")} threads=#{threads} " \
            "notify_wakeup=#{notify_wakeup?} pid=#{::Process.pid}"
        end

        loop do
          process_signals
          check_recycle
          ensure_notify_listener

          break if @shutting_down

          consume
        end

        shutdown
      end

      def graceful_shutdown
        @shutting_down = true
        @wake_signal.notify!
      end

      def immediate_shutdown
        @shutting_down = true
        @wake_signal.notify!
        @pool.kill
      end

      private

      def setup_subscriptions
        matching = @registry.subscribers.select do |s|
          topics.any? { |t| pattern_overlaps?(t, s.pattern) }
        end
        @queue_names = matching.map(&:queue_name).uniq
      end

      def consume
        idle = @pool.available_capacity
        return @wake_signal.wait(timeout: wake_timeout) if idle <= 0

        tagged_messages = if @queue_names.size == 1
                            queue = @queue_names.first
                            (Pgbus.client.read_batch(queue, qty: idle) || []).map { |m| [queue, m] }
                          else
                            fetch_multi_consumer(idle)
                          end

        if tagged_messages.empty?
          @wake_signal.wait(timeout: wake_timeout)
          return
        end

        tagged_messages.each do |queue_name, message|
          @pool.post { handle_message(message, queue_name) }
        end
      end

      def handle_message(message, queue_name)
        if message.read_ct.to_i > config.max_retries
          Pgbus.logger.warn { "[Pgbus] Consumer moving message #{message.msg_id} to DLQ after #{message.read_ct} reads" }
          Pgbus.client.move_to_dead_letter(queue_name, message)
          return
        end

        raw = JSON.parse(message.message)
        routing_key = raw.dig("headers", "routing_key") || raw["routing_key"]

        handlers = @registry.handlers_for(routing_key || "")
        handlers.each do |subscriber|
          handler = subscriber.handler_class.new
          handler.process(message)
        end

        Pgbus.client.archive_message(queue_name, message.msg_id.to_i)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Consumer error: #{e.class}: #{e.message}" }
        # Message stays in queue; VT will expire and it becomes available again.
        # read_ct tracks delivery attempts — when it exceeds max_retries,
        # the next read will route to DLQ above.
      ensure
        # Count every message the consumer handles — success, DLQ-routed, AND
        # rescued failure — mirroring Worker#process_message, which increments
        # unconditionally. Counting only successes would let max_jobs recycling
        # never trip on a poison/all-failing queue: the exact unbounded-memory
        # scenario recycling exists to bound.
        @jobs_processed.increment
      end

      # `qty` is the total pool capacity. pgmq-ruby treats `qty:` as per-queue,
      # so we also pass `limit: qty` to cap the total across all queues —
      # otherwise we get `queue_count * qty` messages and overflow the
      # execution pool, crashing the consumer fork (issue #123).
      def fetch_multi_consumer(qty)
        messages = Pgbus.client.read_multi(@queue_names, qty: qty, limit: qty) || []
        prefix = "#{config.queue_prefix}_"

        messages.map do |m|
          logical = m.queue_name&.delete_prefix(prefix) || @queue_names.first
          [logical, m]
        end
      end

      def pattern_overlaps?(topic_filter, subscription_pattern)
        # Simple check: if either is a subset of the other
        topic_filter == subscription_pattern ||
          topic_filter.end_with?("#") ||
          subscription_pattern.start_with?(topic_filter.delete_suffix(".#"))
      end

      # Signal the loop to exit cleanly once a recycle limit is hit. The clean
      # exit gets an immediate supervisor restart (supervisor.rb:305-307), so a
      # fresh fork replaces this one before its memory grows unbounded — the
      # same fix Worker#check_recycle provides. Guarded by @recycling so the
      # per-iteration call instruments and logs exactly once.
      def check_recycle
        return if @recycling

        reason = recycle_reason
        return unless reason

        @recycling = true
        @shutting_down = true
        Pgbus::Instrumentation.instrument(
          "pgbus.consumer.recycle",
          reason: reason,
          jobs_processed: @jobs_processed.value,
          memory_mb: current_memory_mb,
          lifetime_seconds: monotonic_now - @started_at_monotonic
        )
        @wake_signal.notify!
      end

      def recycle_reason
        return :max_jobs if exceeded_max_jobs?
        return :max_memory if exceeded_max_memory?
        return :max_lifetime if exceeded_max_lifetime?

        nil
      end

      def recycle_needed?
        !recycle_reason.nil?
      end

      def exceeded_max_jobs?
        return false unless config.max_jobs_per_worker && @jobs_processed.value >= config.max_jobs_per_worker

        Pgbus.logger.info { "[Pgbus] Consumer recycling: max_jobs reached (#{@jobs_processed.value})" }
        true
      end

      def exceeded_max_memory?
        return false unless config.max_memory_mb && current_memory_mb > config.max_memory_mb

        Pgbus.logger.info do
          "[Pgbus] Consumer recycling: memory limit (#{current_memory_mb}MB > #{config.max_memory_mb}MB)"
        end
        true
      end

      def exceeded_max_lifetime?
        return false unless config.max_worker_lifetime && (monotonic_now - @started_at_monotonic) > config.max_worker_lifetime

        Pgbus.logger.info { "[Pgbus] Consumer recycling: lifetime exceeded" }
        true
      end

      # Instrumentation payload may report a value up to MEMORY_CHECK_TTL seconds old.
      def current_memory_mb
        MemoryUsage.current_mb
      end

      def notify_wakeup?
        config.respond_to?(:worker_notify_wakeup?) && config.worker_notify_wakeup?
      end

      def wake_timeout
        # A dead listener (running? false) will never wake the loop, so treat it
        # as absent and keep polling at the short interval until
        # ensure_notify_listener restarts it. Only a live listener earns the
        # long NOTIFY-mode ceiling.
        return config.polling_interval unless notify_wakeup? && @notify_listener&.running?

        [config.polling_interval, NOTIFY_FALLBACK_POLL_SECONDS].max
      end

      def start_notify_listener
        return unless notify_wakeup?

        @notify_listener = NotifyListener.new(
          physical_queues: physical_queue_names,
          on_wake: -> { @wake_signal.notify! },
          connection_options: config.worker_notify_connection_options,
          health_check_ms: (config.polling_interval * 1000).to_i.clamp(250, 5_000),
          logger: Pgbus.logger
        ).start
      rescue StandardError => e
        @notify_listener = nil
        Pgbus.logger.error do
          "[Pgbus] Consumer NotifyListener failed to start, falling back to polling: #{e.class}: #{e.message}"
        end
      end

      # Self-heal a NotifyListener that never started or whose thread died.
      # Called each loop iteration but gated by a monotonic backoff timestamp so
      # a persistent outage retries on 5s→…→300s intervals, not every tick
      # (mirrors Worker#ensure_notify_listener).
      def ensure_notify_listener
        return unless notify_wakeup?
        return if @notify_listener&.running?
        return if monotonic_now < @notify_retry_at

        stop_dead_notify_listener
        start_notify_listener

        @notify_retry_backoff = if @notify_listener&.running?
                                  NOTIFY_RETRY_BASE_SECONDS
                                else
                                  [@notify_retry_backoff * 2, NOTIFY_RETRY_MAX_SECONDS].min
                                end
        @notify_retry_at = monotonic_now + @notify_retry_backoff
      end

      # Stop a listener whose thread died so its dedicated PG connection is
      # released before start_notify_listener allocates a fresh one.
      def stop_dead_notify_listener
        return if @notify_listener.nil?

        @notify_listener.stop
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Consumer failed to stop dead NotifyListener: #{e.class}: #{e.message}" }
      ensure
        @notify_listener = nil
      end

      def physical_queue_names
        prefix = "#{config.queue_prefix}_"
        @queue_names.map { |q| "#{prefix}#{q}" }
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "consumer",
          metadata: { topics: topics, threads: threads, pid: ::Process.pid }
        )
        @heartbeat.start
      end

      def shutdown
        @notify_listener&.stop
        @pool.shutdown
        @pool.wait_for_termination(30)
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Consumer stopped. Processed: #{@jobs_processed.value}" }
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end
    end
  end
end
