# frozen_string_literal: true

module Pgbus
  class Client
    # In-memory, process-local circuit breaker for database-down conditions.
    #
    # Distinct from Pgbus::CircuitBreaker (which is per-queue and persists its
    # pause state in the database — useless when the database itself is down).
    # This latch is a single shared object owned by Pgbus::Client that trips on
    # raw connection failures across *any* operation, so a fleet of workers
    # stops hammering a dead database and flooding the error tracker for the
    # whole outage (issue #197).
    #
    # State machine:
    #   closed    -> open       after OPEN_THRESHOLD consecutive ConnectionErrors
    #   open      -> half_open  once the backoff window elapses (admits ONE probe)
    #   half_open -> closed     on a successful probe (resets backoff)
    #   half_open -> open       on a failed probe (backoff doubles, capped)
    #
    # Thresholds are constants, not configuration, mirroring CircuitBreaker:
    # the values rarely need tuning and exposing them never proved useful.
    #
    # Thread safety: a single Mutex serializes every state transition. The
    # guarded operation itself runs *outside* the lock (only the gate decision
    # and the outcome recording are inside), so a slow probe never blocks other
    # worker threads from failing fast.
    class ConnectionHealth
      # Consecutive ConnectionErrors that trip the breaker from closed to open.
      OPEN_THRESHOLD = 5

      # Initial backoff (seconds) on the first trip. Doubles on each re-open.
      BASE_BACKOFF = 1.0

      # Cap on the exponential backoff (seconds). After ~6 re-opens the curve
      # plateaus here so a perpetually-down database stops probing more than
      # once a minute.
      MAX_BACKOFF = 60.0

      def initialize(clock: -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) },
                     on_open: nil, on_close: nil)
        @clock = clock
        @on_open = on_open
        @on_close = on_close
        @mutex = Mutex.new
        @state = :closed
        @failure_count = 0
        @trip_count = 0
        @open_until = nil
      end

      # Gate an operation through the breaker.
      #
      # - closed / half-open-probe-admitted: yields, records the outcome.
      # - open (window not elapsed) or half-open (probe already in flight):
      #   raises Pgbus::ConnectionCircuitOpenError without yielding — no pool
      #   checkout, no ConnectionError, no ErrorReporter noise.
      #
      # The breaker never swallows an error — every exception propagates. A
      # ConnectionError in the closed state counts toward the trip threshold; a
      # failure of the single half-open probe (on ANY error) re-opens the
      # breaker; any other closed-state error is not counted.
      def run_guarded
        admitted = admit_or_raise # :run (was closed) or :probe (was open)

        begin
          result = yield
        rescue StandardError => e
          record_failure(admitted, e)
          raise
        end

        record_success(admitted)
        result
      end

      def closed?
        @mutex.synchronize { @state == :closed }
      end

      def open?
        @mutex.synchronize { @state == :open }
      end

      private

      # Decide whether this call may proceed. Runs entirely inside the mutex.
      # Returns the admission kind so the outcome recorders can attribute the
      # result to THIS call (the probe vs. an ordinary closed-state read) —
      # critical because the block runs outside the lock, so between admit and
      # record the state may have changed under another thread.
      #
      # When admitting a probe from the open state it flips @state to :half_open
      # so a second racing thread sees :half_open and is rejected — that is the
      # single-probe guarantee.
      def admit_or_raise
        @mutex.synchronize do
          case @state
          when :closed
            :run
          when :open
            raise_open unless window_elapsed?

            @state = :half_open # admit exactly this thread as the probe
            :probe
          when :half_open
            raise_open # a probe is already in flight
          end
        end
      end

      def record_success(admitted)
        callback = nil
        @mutex.synchronize do
          if admitted == :probe && @state == :half_open
            # The single probe succeeded — the database is reachable again.
            close!
            callback = @on_close
          elsif @state == :closed
            # An ordinary read completed while closed: clear the failure streak.
            # Do NOT force-close here — a straggler read admitted while closed
            # can return AFTER other threads have tripped the breaker; closing
            # would wipe a freshly-opened breaker (issue #197 straggler race).
            @failure_count = 0
          end
        end
        callback&.call
      end

      # Any exception ends the operation. A probe failure — whether a
      # ConnectionError or something else (e.g. Pgbus::ReadTimeoutError from a
      # hung socket, which the read paths raise from INSIDE the guard) — must
      # re-open the breaker. If it didn't, a non-ConnectionError probe would
      # leave the latch wedged in :half_open forever, admitting no further
      # probes and never recovering (issue #197 wedge). Ordinary closed-state
      # failures only count toward the threshold when they are ConnectionErrors.
      def record_failure(admitted, error)
        callback_backoff = nil
        @mutex.synchronize do
          if admitted == :probe && @state == :half_open
            callback_backoff = reopen
          elsif @state == :closed && connection_error?(error)
            @failure_count += 1
            callback_backoff = reopen if @failure_count >= OPEN_THRESHOLD
          end
          # Otherwise (a straggler failing after another thread already changed
          # the state) leave the current window untouched.
        end
        @on_open&.call(callback_backoff) if callback_backoff
      end

      # Transition to closed and fully reset. Must be called with @mutex held.
      def close!
        @state = :closed
        @failure_count = 0
        @trip_count = 0
        @open_until = nil
      end

      # Transition to open, arm the backoff window, and return the window size
      # so the caller can fire the on_open callback outside the lock. Must be
      # called with @mutex held.
      def reopen
        @trip_count += 1
        backoff = calculate_backoff(@trip_count)
        @state = :open
        @failure_count = 0
        @open_until = clock_now + backoff
        backoff
      end

      def window_elapsed?
        @open_until.nil? || clock_now >= @open_until
      end

      def calculate_backoff(trip_count)
        backoff = BASE_BACKOFF * (2**(trip_count - 1))
        [backoff, MAX_BACKOFF].min
      end

      def raise_open
        raise Pgbus::ConnectionCircuitOpenError,
              "database connection circuit is open (#{OPEN_THRESHOLD}+ consecutive connection failures)"
      end

      def connection_error?(error)
        defined?(PGMQ::Errors::ConnectionError) && error.is_a?(PGMQ::Errors::ConnectionError)
      end

      def clock_now
        @clock.call
      end
    end
  end
end
