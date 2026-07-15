# frozen_string_literal: true

require "concurrent"

module Pgbus
  class Client
    # Owns the current streams PGMQ::Client behind a Concurrent::AtomicReference
    # and resizes it by BUILDING A NEW pool, atomically swapping the reference,
    # then bounded-draining + shutting down the old one (issue #323 spike).
    #
    # connection_pool 2.x cannot resize in place (@size is frozen at
    # construction; only shutdown/reload/available exist), so growing REQUIRES a
    # new PGMQ::Client + a reference swap.
    #
    # DRAIN CORRECTNESS — the load-bearing detail. We do NOT trust
    # connection_pool's `available == size` as "drained": pgmq-ruby's
    # #with_connection retries a lost connection (checkout → checkin → checkout
    # again), so between the two checkouts the connection sits idle in the queue
    # and `available` momentarily equals `size` while a produce/read is still
    # mid-flight. Closing the pool in that window makes the retry's checkout raise
    # PoolShuttingDownError → a genuinely LOST durable broadcast (the
    # PgBouncer/failover mid-INSERT case). Instead each reader op is bracketed by
    # an in-flight AtomicFixnum on the CAPTURED pool, and drain waits until THAT
    # hits zero. This is immune to the internal retry and independent of the
    # connection_pool version.
    #
    # DURABLE BROADCASTS are loss-safe by construction: #produce captures the
    # Pool once, runs its whole retrying checkout→INSERT→COMMIT→checkin on that
    # pool, then decrements. Both old and new pools connect to the same Postgres,
    # so any INSERT commits; and close waits for the captured pool's inflight to
    # reach zero, so a pool is never shut while a produce is in flight on it.
    class ResizablePool
      # The current pool: a PGMQ::Client plus its own in-flight checkout counter.
      Pool = Data.define(:pgmq, :inflight)

      SwapStats = Data.define(:swap_count, :last_drain_seconds, :last_conns_closed,
                              :last_from_size, :last_to_size, :last_drained)

      DRAIN_POLL = 0.01 # 10ms

      # `shared:` is accepted for symmetry with the Client's two connection paths
      # but is not stored — the shared-AR alias guard lives in #close_current via
      # the `job_pool:` identity check, and #swap is simply never called on the
      # shared path (Client#resize_streams_pool short-circuits there).
      def initialize(pgmq, shared:, drain_timeout:, logger:, clock: ::Process) # rubocop:disable Lint/UnusedMethodArgument
        @ref = Concurrent::AtomicReference.new(new_pool(pgmq))
        @drain_timeout = drain_timeout
        @logger = logger
        @clock = clock
        @swap_mutex = Mutex.new # serializes swap AND close_current; reads never take it
        @swap_count = 0
        @last = nil
      end

      # Hot path: one volatile load, no lock.
      def current = @ref.get

      # Bracketed reader ops — inflight decrements only after the WHOLE retrying
      # call returns (so drain can't close under an in-flight produce/read).
      def produce(*, **)
        pool = @ref.get
        pool.inflight.increment
        begin
          pool.pgmq.produce(*, **)
        ensure
          pool.inflight.decrement
        end
      end

      def with_connection(&)
        pool = @ref.get
        pool.inflight.increment
        begin
          pool.pgmq.with_connection(&)
        ensure
          pool.inflight.decrement
        end
      end

      def stats = @ref.get.pgmq.stats

      # Swap in a freshly-built PGMQ::Client, then drain + close the old Pool.
      def swap(new_pgmq, from_size:, to_size:)
        @swap_mutex.synchronize do
          old = @ref.get
          @ref.set(new_pool(new_pgmq)) # new checkouts now resolve to the new pool
          drained, secs, closed = drain_and_close(old)
          @swap_count += 1
          @last = SwapStats.new(swap_count: @swap_count, last_drain_seconds: secs,
                                last_conns_closed: closed, last_from_size: from_size,
                                last_to_size: to_size, last_drained: drained)
        end
      end

      # Teardown of the current pool (from Client#close), race-free vs swap.
      # Skips the close when the current pool aliases the job pool (shared-AR path).
      def close_current(job_pool:)
        @swap_mutex.synchronize do
          pool = @ref.get
          pool.pgmq.close unless pool.pgmq.equal?(job_pool)
        end
      end

      # Drop every pooled connection on the CURRENT streams client and let it
      # rebuild lazily on next checkout (pgmq-ruby >= 0.7.1) — see
      # Client#reload (issue #354). Takes the swap mutex so a reload cannot
      # race a concurrent swap/close into reloading a pool that is being
      # retired; reads stay lock-free.
      def reload
        @swap_mutex.synchronize { @ref.get.pgmq.reload }
      end

      def stats_snapshot
        @last || SwapStats.new(swap_count: 0, last_drain_seconds: 0.0, last_conns_closed: 0,
                               last_from_size: nil, last_to_size: nil, last_drained: nil)
      end

      private

      def new_pool(pgmq)
        Pool.new(pgmq: pgmq, inflight: Concurrent::AtomicFixnum.new(0))
      end

      # Bounded drain on the OLD pool's inflight counter, then shutdown. Mirrors
      # OutboundPump#stop: fixed budget, log-and-abandon, NEVER Thread#kill /
      # Timeout.timeout. Returns [drained?, wall_secs, conns_closed].
      def drain_and_close(old)
        t0 = mono
        drained = wait_until_drained(old.inflight)
        # Count connections about to be torn down BEFORE close (after close it's ~0).
        conns = conns_open(old.pgmq)
        unless drained
          @logger.warn do
            "[Pgbus::ResizablePool] streams pool did not fully drain in " \
              "#{@drain_timeout}s (inflight=#{old.inflight.value}); shutting down anyway — " \
              "in-flight connections self-close on checkin"
          end
        end
        old.pgmq.close
        [drained, mono - t0, conns]
      rescue StandardError => e
        @logger.error { "[Pgbus::ResizablePool] old pool teardown failed: #{e.class}: #{e.message}" }
        [false, mono - t0, 0]
      end

      def wait_until_drained(inflight)
        deadline = mono + @drain_timeout
        loop do
          return true if inflight.value <= 0
          return false if mono >= deadline

          sleep(DRAIN_POLL) # NOT Timeout.timeout — Pgbus/NoRubyTimeout cop
        end
      end

      # size - available = connections currently open (created & not idle-in-queue).
      def conns_open(pgmq)
        stats = pgmq.stats
        [stats[:size] - stats[:available], 0].max
      rescue StandardError
        0
      end

      # Qualify ::Process — Pgbus::Process (the worker/supervisor namespace)
      # shadows the top-level constant here.
      def mono = @clock.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end
end
