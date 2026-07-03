# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Worker
      include SignalHandler

      attr_reader :queues, :threads, :config, :execution_mode

      def initialize(queues:, threads: 5, config: Pgbus.configuration,
                     single_active_consumer: false, consumer_priority: 0,
                     execution_mode: :threads, group_mode: nil)
        @queues = Array(queues)
        @initial_queues = @queues.dup.freeze
        @wildcard = @queues.include?("*")
        @threads = threads
        @config = config
        @execution_mode = ExecutionPools.normalize_mode(execution_mode)
        @group_mode = case group_mode
                      when nil then nil
                      when Symbol then group_mode
                      when String then group_mode.to_sym
                      else
                        raise ArgumentError,
                              "Invalid group_mode type: #{group_mode.class}. Must be nil, String, or Symbol"
                      end
        unless Pgbus::Configuration::VALID_GROUP_MODES.include?(@group_mode)
          raise ArgumentError,
                "Invalid group_mode: #{@group_mode.inspect}. Must be nil, :fifo, or :round_robin"
        end
        @single_active_consumer = single_active_consumer
        @consumer_priority = consumer_priority
        @lifecycle = Lifecycle.new
        @last_wildcard_resolve = nil
        @jobs_processed = Concurrent::AtomicFixnum.new(0)
        @jobs_failed = Concurrent::AtomicFixnum.new(0)
        @in_flight = Concurrent::AtomicFixnum.new(0)
        @loop_tick_at = Concurrent::AtomicReference.new(nil)
        @rate_counter = RateCounter.new(:processed, :failed, :dequeued)
        @started_at = Time.current
        @started_at_monotonic = monotonic_now
        @stat_buffer = config.stats_enabled ? Pgbus::StatBuffer.new : nil
        @executor = Pgbus::ActiveJob::Executor.new(stat_buffer: @stat_buffer)
        @wake_signal = WakeSignal.new
        @pool = ExecutionPools.build(
          mode: @execution_mode,
          capacity: threads,
          on_state_change: -> { @wake_signal.notify! }
        )
        @circuit_breaker = Pgbus::CircuitBreaker.new(config: config)
        @drain_started_at = nil
        @queue_lock = QueueLock.new if @single_active_consumer
        @notify_listener = nil
        @notify_retry_at = 0.0
        @notify_retry_backoff = NOTIFY_RETRY_BASE_SECONDS
      end

      def stats
        {
          jobs_processed: @jobs_processed.value,
          jobs_failed: @jobs_failed.value,
          in_flight: @in_flight.value,
          state: @lifecycle.state,
          execution_mode: @execution_mode,
          consumer_priority: @consumer_priority,
          single_active_consumer: @single_active_consumer,
          locked_queues: @queue_lock&.held_queues || [],
          rates: @rate_counter.rates,
          started_at: @started_at
        }.merge(@pool.metadata)
      end

      NOTIFY_FALLBACK_POLL_SECONDS = 15

      # NotifyListener startup can fail on a transient boot-time condition (DB
      # restarting, failover, DNS blip) or its thread can die mid-run. Rather
      # than downgrade to blind polling until process restart, the loop retries
      # from ensure_notify_listener on an exponential backoff: NOTIFY_RETRY_BASE
      # doubling up to NOTIFY_RETRY_MAX. Constant-tuned, matching DRAIN_TIMEOUT
      # and CircuitBreaker/Dispatcher precedent.
      NOTIFY_RETRY_BASE_SECONDS = 5
      NOTIFY_RETRY_MAX_SECONDS = 300

      # Upper bound on the drain phase. Waiting for quiesced? must be
      # bounded: a permanently-stuck job would otherwise hold the loop open
      # forever — recycling never completes, TERM shutdown hangs the whole
      # process tree, and the loop keeps stamping loop_tick so the
      # supervisor watchdog never intervenes. After this many seconds the
      # loop falls through to shutdown, whose wait_for_termination(30)
      # bounds the remaining in-flight work. Tuned via constant rather than
      # configuration, matching CircuitBreaker/Dispatcher precedent.
      DRAIN_TIMEOUT = 30

      def run
        setup_signals
        start_heartbeat
        resolve_wildcard_queues
        start_notify_listener
        @lifecycle.transition_to!(:running)
        Pgbus.logger.info do
          "[Pgbus] Worker started: queues=#{queues.join(",")} threads=#{threads} " \
            "mode=#{@execution_mode} notify_wakeup=#{notify_wakeup?} pid=#{::Process.pid}"
        end

        loop do
          stamp_loop_tick
          process_signals
          check_recycle
          refresh_wildcard_queues
          ensure_notify_listener

          break if @lifecycle.stopped?
          # quiesced? (all slots free), not idle? (any slot free) — exiting
          # with work still in flight abandons those jobs to the 30s
          # wait_for_termination timeout in shutdown. Bounded by
          # DRAIN_TIMEOUT so a stuck job can't wedge the loop forever.
          break if @lifecycle.draining? && (@pool.quiesced? || drain_deadline_exceeded?)

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
        poll_interval = wake_timeout

        idle = @pool.available_capacity
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
          detect_zombie(queue_name, message)
          @in_flight.increment
          @pool.post { process_message(message, queue_name, source_queue: source_queue) }
        end
      end

      # Returns an array of [queue_name, message] pairs so we always know
      # which queue each message came from.
      def fetch_messages(qty)
        restore_evicted_queues if queues.empty? && !@wildcard

        active_queues = queues.reject { |q| @circuit_breaker.paused?(q) }
        active_queues = active_queues.select { |q| @queue_lock.try_lock(q) } if @single_active_consumer

        if active_queues.empty?
          Pgbus.logger.debug do
            paused = queues.select { |q| @circuit_breaker.paused?(q) }
            "[Pgbus] Worker fetch: all queues filtered — queues=#{queues.join(",")} " \
              "paused=#{paused.join(",")}"
          end
          return []
        end

        if priority_enabled?
          fetch_prioritized(active_queues, qty)
        elsif @group_mode
          fetch_grouped(active_queues, qty)
        elsif active_queues.size == 1
          queue = active_queues.first
          messages = Pgbus.client.read_batch(queue, qty: qty) || []
          messages.map { |m| [queue, m] }
        else
          fetch_multi(active_queues, qty)
        end
      rescue Pgbus::ConnectionCircuitOpenError
        # The client-level connection breaker is open: the database has failed
        # enough consecutive connection attempts that reads now fail fast. Idle
        # this poll without an ErrorReporter call — the whole point of the
        # breaker is to stop every worker flooding the error tracker for the
        # duration of a database outage. The open/close transitions are logged
        # once by the client (Client#log_circuit_open / #log_circuit_close),
        # not per poll here.
        []
      rescue StandardError => e
        if undefined_queue_table_error?(e)
          evict_missing_queues(e)
        else
          ErrorReporter.report(e, { action: "fetch_messages", queues: active_queues })
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
      #
      # `qty` is the total pool capacity. pgmq-ruby treats `qty:` as per-queue,
      # so we also pass `limit: qty` to cap the total across all queues —
      # otherwise we get `queue_count * qty` messages and overflow the
      # execution pool, crashing the worker fork (issue #123).
      def fetch_multi(active_queues, qty)
        messages = Pgbus.client.read_multi(active_queues, qty: qty, limit: qty) || []
        prefix = "#{config.queue_prefix}_"

        messages.map do |m|
          logical = m.queue_name&.delete_prefix(prefix) || active_queues.first
          [logical, m]
        end
      end

      # Use grouped reads for fair or throughput-optimized multi-tenant processing.
      # Each queue is read independently with the configured group strategy.
      def fetch_grouped(active_queues, qty)
        remaining = qty
        results = []

        active_queues.each do |queue|
          break if remaining <= 0

          messages = case @group_mode
                     when :round_robin
                       Pgbus.client.read_grouped_rr(queue, qty: remaining) || []
                     else # :fifo
                       Pgbus.client.read_grouped(queue, qty: remaining) || []
                     end

          messages.each { |m| results << [queue, m] }
          remaining -= messages.size
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
        ErrorReporter.report(e, { action: "process_message", queue: queue_name })
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
        sync_notify_listener_queues
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
        restore_evicted_queues if @queues.empty? && !@wildcard
      end

      def restore_evicted_queues
        @queues = @initial_queues.dup
        Pgbus.logger.warn do
          "[Pgbus] Worker queue list was empty after eviction — " \
            "restoring initial queues: #{@queues.join(", ")}"
        end
      end

      def detect_zombie(queue_name, message)
        return unless config.zombie_detection
        return unless message.read_ct.to_i > 1

        return if FailedEventRecorder.exists?(queue_name: queue_name, msg_id: message.msg_id.to_i)

        Pgbus.logger.warn do
          "[Pgbus] Zombie message redelivered: queue=#{queue_name} msg_id=#{message.msg_id} " \
            "read_ct=#{message.read_ct} — previous read did not record a failure. " \
            "The worker may have crashed mid-execute or the executor silently dropped the job."
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Zombie detection failed: #{e.class}: #{e.message}" }
      end

      # Lazily stamps the drain start on first call — the predicate is only
      # reached while draining, so this covers every path into the drain
      # state (graceful_shutdown, recycling) without hooking each one.
      def drain_deadline_exceeded?
        @drain_started_at ||= monotonic_now
        return false unless monotonic_now - @drain_started_at > DRAIN_TIMEOUT

        Pgbus.logger.warn do
          "[Pgbus] Worker drain deadline (#{DRAIN_TIMEOUT}s) reached with #{@in_flight.value} job(s) " \
            "still in flight — proceeding to shutdown"
        end
        true
      end

      def check_recycle
        return unless @lifecycle.running?

        reason = recycle_reason
        return unless reason

        Pgbus.stopping = true
        @lifecycle.transition_to(:draining)
        Pgbus::Instrumentation.instrument(
          "pgbus.worker.recycle",
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

      # Instrumentation payload may report a value up to MEMORY_CHECK_TTL seconds old.
      def current_memory_mb
        MemoryUsage.current_mb
      end

      def notify_wakeup?
        config.respond_to?(:worker_notify_wakeup?) && config.worker_notify_wakeup?
      end

      def wake_timeout
        # A dead listener (running? false) will never wake the loop, so treat
        # it as absent and keep polling at the short interval until
        # ensure_notify_listener restarts it. Only a live listener earns the
        # long NOTIFY-mode ceiling.
        return effective_polling_interval unless notify_wakeup? && @notify_listener&.running?

        [effective_polling_interval, config.polling_interval, NOTIFY_FALLBACK_POLL_SECONDS].max
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
          "[Pgbus] NotifyListener failed to start, falling back to polling: #{e.class}: #{e.message}"
        end
      end

      # Self-heal a NotifyListener that never started or whose thread died.
      # Called each loop iteration but gated by a monotonic backoff timestamp
      # so a persistent outage retries on 5s→…→300s intervals, not every tick
      # (mirrors refresh_wildcard_queues' throttle). A restarted listener has
      # its queue subscription reconciled (wildcard workers) and the backoff
      # reset; a still-failing restart doubles the backoff up to the cap.
      def ensure_notify_listener
        return unless notify_wakeup?
        return if @notify_listener&.running?
        return if monotonic_now < @notify_retry_at

        stop_dead_notify_listener
        start_notify_listener

        if @notify_listener&.running?
          sync_notify_listener_queues
          @notify_retry_backoff = NOTIFY_RETRY_BASE_SECONDS
        else
          @notify_retry_backoff = [@notify_retry_backoff * 2, NOTIFY_RETRY_MAX_SECONDS].min
        end
        @notify_retry_at = monotonic_now + @notify_retry_backoff
      end

      # Stop a listener whose thread died so its dedicated PG connection is
      # released before start_notify_listener allocates a fresh one. A nil or
      # still-running listener is left alone (the caller already gated on it).
      def stop_dead_notify_listener
        return if @notify_listener.nil?

        @notify_listener.stop
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Failed to stop dead NotifyListener: #{e.class}: #{e.message}" }
      ensure
        @notify_listener = nil
      end

      def sync_notify_listener_queues
        return unless @notify_listener

        desired = physical_queue_names.to_set
        current = @notify_listener.listening_to.to_set { |c| channel_to_physical(c) }
        (desired - current).each { |q| @notify_listener.add_queue(q) }
        (current - desired).each { |q| @notify_listener.remove_queue(q) }
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] NotifyListener queue sync failed: #{e.class}: #{e.message}" }
      end

      def physical_queue_names
        prefix = "#{config.queue_prefix}_"
        queues.map { |q| "#{prefix}#{q}" }
      end

      def channel_to_physical(channel)
        channel.delete_prefix(NotifyListener::CHANNEL_PREFIX).delete_suffix(NotifyListener::CHANNEL_SUFFIX)
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "worker",
          metadata: {
            queues: queues, threads: threads, pid: ::Process.pid,
            execution_mode: @execution_mode, consumer_priority: @consumer_priority
          },
          on_beat: -> { on_heartbeat },
          loop_tick_supplier: -> { @loop_tick_at.get }
        )
        @heartbeat.start
      end

      # Runs once per heartbeat interval (not per job), so it's the right place
      # to snapshot the per-beat rate counters and emit connection-pool
      # observability without touching any per-job hot path. Pool utilization
      # goes out as a `pgbus.client.pool` event carrying {size:, available:,
      # pool_timeout:}. Reading the pool must never crash the beat — pool_stats
      # already rescues to {}, and this whole method is guarded so a listener or
      # unexpected error can't take down the heartbeat thread.
      def on_heartbeat
        @rate_counter.snapshot!
        emit_pool_stats
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Worker heartbeat hook error: #{e.class}: #{e.message}" }
      end

      def emit_pool_stats
        stats = Pgbus.client.pool_stats
        return if stats.empty?

        Pgbus::Instrumentation.instrument("pgbus.client.pool", stats)
      end

      def shutdown
        Pgbus.logger.info { "[Pgbus] Worker draining thread pool..." }
        @notify_listener&.stop
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

      # Stamp the loop-progress beacon with a wall-clock timestamp.
      # Wall clock is required because the supervisor watchdog reads
      # this value from a different process (cross-fork) and the
      # dashboard reads it from a different host.
      def stamp_loop_tick
        @loop_tick_at.set(Time.now.to_f)
      end
    end
  end
end
