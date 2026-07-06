# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Streams
    # Publisher-side throttled autoscale trigger (issue #323 follow-up).
    #
    # The streamer autoscales via a periodic check on its idle LISTEN connection,
    # but a pure-*publisher* process (a worker that fans out broadcasts via
    # Client#send_stream_message but serves no SSE) has no streamer — so its
    # streams pool would never autoscale. This closes that gap: each publish
    # opportunistically triggers a headroom check, throttled to at most once per
    # `interval` seconds across all publisher threads.
    #
    # It runs the query through the JOB pool, NOT the streams pool: pg_stat_activity
    # is global, so any connection to the same database reads the same headroom,
    # and using the job pool means the check can never starve on a saturated
    # streams pool (the very pool it's trying to grow). The decision reuses the
    # same PoolAutoscaler#evaluate the streamer uses.
    #
    # Fail-soft and off the critical path: #maybe_check swallows every error (a
    # publish must never fail because a telemetry query did) and the throttle CAS
    # means all but one thread per window return immediately.
    class PoolTrigger
      def initialize(autoscaler:, job_pool:, interval:, application_name_prefix:, clock: nil,
                     logger: Pgbus.logger)
        @autoscaler = autoscaler
        @job_pool = job_pool
        @interval = interval
        @like = "#{application_name_prefix}_%"
        @clock = clock || -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) }
        @logger = logger
        # nil = never checked. AtomicReference gives a lock-free compare-and-set
        # so exactly one concurrent publisher claims each throttle window.
        @last_check = Concurrent::AtomicReference.new(nil)
      end

      # Called from the publish path. Runs a headroom check + decision at most
      # once per interval; returns immediately (having done nothing) otherwise.
      # Never raises.
      def maybe_check
        return unless claim_window

        headroom = read_headroom
        @autoscaler.evaluate(headroom) if headroom
      rescue StandardError => e
        @logger.debug { "[Pgbus::Streams::PoolTrigger] check failed: #{e.class}: #{e.message}" }
      end

      private

      # True for exactly one caller per `interval` window; false for the rest.
      # A losing CAS (another thread advanced @last_check first) also returns
      # false, so only one thread runs the check per window.
      def claim_window
        now = @clock.call
        last = @last_check.get
        return false if last && (now - last) < @interval

        @last_check.compare_and_set(last, now)
      end

      def read_headroom
        @job_pool.with_connection do |conn|
          row = conn.exec_params(PoolAutoscaler::HEADROOM_SQL, [@like]).first
          { maxc: row["maxc"].to_i, used: row["used"].to_i, peers: row["peers"].to_i }
        end
      end
    end
  end
end
