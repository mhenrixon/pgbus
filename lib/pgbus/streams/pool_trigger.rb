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
    # OFF THE HOT PATH: the publishing thread only does a lock-free
    # compare-and-set to claim the throttle window and, if it wins, posts the
    # actual work to a background single-thread executor and returns immediately.
    # It never runs the query or the resize inline. A quiet (non-publishing)
    # process does zero work — this object is only ever built by the first publish
    # (Client#streams_pool_trigger), and only when autoscale is on and the pool is
    # dedicated, so a process that never publishes a stream never spawns the
    # executor thread. There is no always-on timer.
    #
    # AT MOST ONE CHECK AT A TIME is enforced primarily by the single-thread
    # executor (it serializes run_check calls end-to-end) plus the throttle CAS
    # (one post per interval). The @running flag is a belt-and-suspenders
    # invariant that becomes load-bearing only if a MULTI-threaded executor is
    # injected (the executor: seam) — then it is the sole guard against two
    # overlapping check bodies. It is cheap (one AtomicBoolean per check), so it
    # stays.
    #
    # The query runs through the JOB pool, NOT the streams pool: pg_stat_activity
    # is global, so any connection to the same database reads the same headroom,
    # and using the job pool means the check can never starve on a saturated
    # streams pool (the very pool it's trying to grow). The decision reuses
    # PoolAutoscaler#evaluate with allow_shrink: false — a publisher only ever
    # reacts to its own publish pressure (grow) and to DB exhaustion (emergency
    # shrink); the streamer/consumer handles normal idle shrink.
    class PoolTrigger
      def initialize(autoscaler:, job_pool:, interval:, application_name_prefix:, clock: nil,
                     executor: nil, logger: Pgbus.logger)
        @autoscaler = autoscaler
        @job_pool = job_pool
        @interval = interval
        @like = "#{application_name_prefix}_%"
        @clock = clock || -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) }
        @logger = logger
        # A single background worker with a GENUINELY bounded 1-slot queue that
        # DISCARDS overflow. NOTE: Concurrent::SingleThreadExecutor silently drops
        # max_queue: (it hardcodes an unbounded queue), so build the base
        # ThreadPoolExecutor directly to actually honor the bound. Built eagerly
        # (assigned once, safely published with the object) — the quiet-process
        # "no thread" guarantee is owned by Client#streams_pool_trigger (this
        # object only exists after a real publish with autoscale on), so there's
        # no reason to lazy-build inside. Injectable for tests.
        @executor = executor || Concurrent::ThreadPoolExecutor.new(
          min_threads: 1, max_threads: 1, max_queue: 1, fallback_policy: :discard
        )
        # nil = never checked. AtomicReference gives a lock-free compare-and-set
        # so exactly one concurrent publisher claims each throttle window.
        @last_check = Concurrent::AtomicReference.new(nil)
        # At-most-one-check guard — load-bearing only under an injected
        # multi-threaded executor (see class comment).
        @running = Concurrent::AtomicBoolean.new(false)
      end

      # Called from the publish path. Claims the throttle window with a single CAS
      # and, if it wins, defers the actual check to the background executor. Does
      # NOT run the query or resize on the calling thread. Never raises.
      def maybe_check
        return unless claim_window

        @executor.post { run_check }
      rescue StandardError => e
        # A post after shutdown raises RejectedExecutionError; swallow it so a
        # publish never fails. The check body itself is guarded inside run_check.
        @logger.debug { "[Pgbus::Streams::PoolTrigger] dispatch failed: #{e.class}: #{e.message}" }
      end

      # Stop the background executor (idempotent). For a clean Client#close.
      def shutdown
        @executor.shutdown
        @executor.wait_for_termination(5)
      end

      private

      # True for exactly one caller per `interval` window; false for the rest.
      # A losing CAS (another thread advanced @last_check first) also returns
      # false, so only one thread posts the check per window.
      def claim_window
        now = @clock.call
        last = @last_check.get
        return false if last && (now - last) < @interval

        @last_check.compare_and_set(last, now)
      end

      # Runs on the executor thread. Skips if a prior check is still in flight so
      # a slow query can't queue up work. Fully guarded — a failure here never
      # escapes the executor.
      def run_check
        return unless @running.make_true # false if already running

        begin
          headroom = read_headroom
          @autoscaler.evaluate(headroom, allow_shrink: false) if headroom
        ensure
          @running.make_false
        end
      rescue StandardError => e
        @logger.debug { "[Pgbus::Streams::PoolTrigger] check failed: #{e.class}: #{e.message}" }
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
