# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Self-tuning control loop for the dedicated streams DB pool (issue #323).
      #
      # One per web-server process (a sibling of Heartbeat in the Streamer
      # Instance). Every `streams_pool_autoscale_interval` seconds it:
      #   1. reads live Postgres headroom (max_connections − used) through the
      #      streams pool's own connection — a HeadroomProbe;
      #   2. EMERGENCY-SHRINKS to the baseline immediately if free connections are
      #      critically low (protect the DB, overriding busy_ratio AND cooldown);
      #   3. otherwise, when the pool is sustained-saturated AND a fair share of
      #      real headroom exists, GROWS into it; when sustained-idle, SHRINKS
      #      back toward the baseline (streams_pool_size).
      #
      # The whole point is "no connection-count config": every threshold derives
      # from live max_connections. streams_pool_max is an OPTIONAL hard cap.
      #
      # SAFETY (proven in the #323 design):
      #   * No multi-process exhaustion: four stacked grow guards (GROW_RESERVE
      #     gate, SAFETY peer inflation, STEP_MAX, per-process floor(free/2))
      #     bound the worst-case synchronized cold-boot herd; the fleet's own
      #     connections appear in pg_stat_activity next tick and grow collapses.
      #   * No grow↔emergency limit cycle: GROW_RESERVE (0.20·maxc) ≥ 4×
      #     EMERGENCY_MARGIN (0.05·maxc) leaves a ~15% dead-zone, so a grow can
      #     never push free down into the emergency band.
      #   * BUG-0 immunity: a freshly swapped connection_pool is lazy and reads
      #     busy_ratio ≈ 0; the cooldown suppresses busy_ratio logic, while the
      #     emergency check keys off DB `free` (an external fact) and so may run
      #     during cooldown.
      class PoolAutoscaler
        GROW_THRESHOLD   = 0.85 # busy_ratio to consider growing
        SHRINK_THRESHOLD = 0.30 # busy_ratio to consider shrinking
        GROW_SUSTAIN     = 3    # consecutive samples ≥ GROW_THRESHOLD
        SHRINK_SUSTAIN   = 20   # consecutive samples < SHRINK_THRESHOLD
        COOLDOWN_SECONDS = 15.0 # skip-sampling window after any swap (BUG-0 guard)
        FAIR_FRACTION    = 0.25 # claim only ¼ of the computed fair share per grow
        SAFETY           = 1.5  # inflate peer count → deflate fair share (startup undercount)
        STEP_MAX         = 4    # hard cap on connections added per single grow

        def initialize(client:, config:, probe: nil, logger: Pgbus.logger, clock: nil)
          @client = client
          @config = config
          @probe = probe || HeadroomProbe.new(client)
          @logger = logger
          @clock = clock || -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) }
          @baseline = config.streams_pool_size
          @interval = config.streams_pool_autoscale_interval
          @grow_run = 0
          @shrink_run = 0
          @cooldown_until = nil
          @running = false
          @thread = nil
          @wake = ConditionVariable.new
          @wake_mutex = Mutex.new
        end

        def start
          return self if @running

          @running = true
          @thread = Thread.new { run_loop }
          self
        end

        def stop
          return self unless @running

          @running = false
          @wake_mutex.synchronize { @wake.broadcast }
          @thread&.join(5)
          @thread = nil
          self
        end

        # One control-loop iteration. Public so the policy is unit-testable with
        # an injected clock + scripted probe (no DB). Returns the action taken.
        def tick
          now = @clock.call

          # (A) Cheap headroom read EVERY tick — DB truth, unaffected by our swap.
          headroom = @probe.read
          return :hold if headroom.nil?

          free = headroom[:maxc] - headroom[:used]

          # (B) PRIORITY 1 — EMERGENCY SHRINK. Keys off `free` (external DB fact),
          # so it is immune to BUG-0 and runs even during cooldown.
          return emergency_shrink(free, headroom[:maxc], now) if free < emergency_margin(headroom[:maxc])

          # (C) BUG-0 guard — suppress busy_ratio logic during cooldown only.
          return :cooldown if @cooldown_until && now < @cooldown_until

          @cooldown_until = nil

          # (D) Sample our own pool — no checkout (pure counter arithmetic).
          size, busy_ratio = pool_busy
          return :hold if size.nil?

          decide(size, busy_ratio, free, headroom, now)
        end

        private

        def decide(size, busy_ratio, free, headroom, now)
          if busy_ratio >= GROW_THRESHOLD
            @shrink_run = 0
            @grow_run += 1
            return maybe_grow(size, free, headroom, now) if @grow_run >= GROW_SUSTAIN
          elsif busy_ratio < SHRINK_THRESHOLD
            @grow_run = 0
            @shrink_run += 1
            return maybe_shrink(size, now) if @shrink_run >= SHRINK_SUSTAIN
          else
            reset_runs # dead-band resets both
          end
          :hold
        end

        # PRIORITY 2 — GROW into a bounded fair share of live headroom.
        def maybe_grow(size, free, headroom, now)
          reset_runs
          return :hold if free <= grow_reserve(headroom[:maxc]) # not enough headroom
          return :hold if size <= 1 # a telemetry checkout would starve publishing

          delta = grow_delta(size, free, headroom[:peers])
          return :hold if delta < 1

          target = size + delta
          cap = @config.streams_pool_max
          target = [target, cap].min if cap
          return :hold if target <= size

          act(size, target, now, :grow)
        end

        # PRIORITY 3 — normal SHRINK toward the baseline (respects cooldown).
        def maybe_shrink(size, now)
          reset_runs
          return :hold if size <= @baseline

          target = [@baseline, size - STEP_MAX].max
          act(size, target, now, :shrink)
        end

        def emergency_shrink(free, maxc, now)
          reset_runs
          return :hold if pool_current_size <= @baseline # already at floor → nothing to do

          result = @client.resize_streams_pool(@baseline)
          if swapped?(result)
            @logger.warn do
              "[Pgbus::Streamer::PoolAutoscaler] EMERGENCY streams-pool shrink to " \
                "#{@baseline} (free=#{free}/#{maxc})"
            end
            @cooldown_until = now + COOLDOWN_SECONDS
          end
          :emergency_shrink
        end

        # fair_share = free / (peers · SAFETY); claim only FAIR_FRACTION of it,
        # and never more than STEP_MAX or half the remaining headroom (so any one
        # process leaves ≥half for the unknown-count peers → geometric contraction).
        def grow_delta(_size, free, peers)
          peer_count = [peers.to_i, 1].max
          fair_share = free.to_f / (peer_count * SAFETY)
          [(FAIR_FRACTION * fair_share).floor, STEP_MAX, (free / 2)].min
        end

        def act(from_size, target_size, now, kind)
          result = @client.resize_streams_pool(target_size)
          reset_runs
          if swapped?(result)
            @logger.info { "[Pgbus::Streamer::PoolAutoscaler] streams pool #{from_size}->#{target_size} (#{kind})" }
            @cooldown_until = now + COOLDOWN_SECONDS
            kind
          else
            :hold # unchanged / shared-AR no-op: no cooldown, no action
          end
        end

        # Emergency/normal margins self-derive from live max_connections. The 4×
        # gap between them is what forbids a grow↔emergency limit cycle.
        def emergency_margin(maxc) = [5, (0.05 * maxc).ceil].max
        def grow_reserve(maxc)     = [20, (0.20 * maxc).ceil].max

        def pool_busy
          stats = @client.streams_pool_stats
          return [nil, nil] if stats.nil? || stats.empty?

          size = stats[:size]
          available = stats[:available]
          return [nil, nil] if size.nil? || available.nil? || size <= 0

          [size, (size - available).to_f / size]
        end

        def pool_current_size
          stats = @client.streams_pool_stats
          stats.is_a?(Hash) ? stats[:size].to_i : 0
        end

        def swapped?(result) = result.is_a?(Pgbus::Client::ResizablePool::SwapStats)

        def reset_runs
          @grow_run = 0
          @shrink_run = 0
        end

        def run_loop
          while @running
            begin
              tick
            rescue StandardError => e
              @logger.error { "[Pgbus::Streamer::PoolAutoscaler] tick raised: #{e.class}: #{e.message}" }
            end
            @wake_mutex.synchronize { @wake.wait(@wake_mutex, @interval) if @running }
          end
        end

        # Reads live Postgres connection headroom + peer process count through the
        # streams pool's OWN connection (the correct streams DB, after P2). Any
        # failure — including a pool-checkout timeout when the pool is saturated —
        # returns nil, and tick HOLDs (never blocks the loop, never raises). The
        # emergency path simply can't fire without a headroom reading; degrading
        # to HOLD is safe (shrink protects the DB; not-growing does no harm).
        class HeadroomProbe
          SQL = <<~SQL
            SELECT current_setting('max_connections')::int AS maxc,
                   count(*) AS used,
                   count(DISTINCT application_name)
                     FILTER (WHERE application_name LIKE $1) AS peers
            FROM pg_stat_activity
          SQL

          def initialize(client)
            @client = client
            @like = "#{client.config.streams_application_name}_%"
          end

          # Returns {maxc:, used:, peers:} or nil on any failure (fail-soft).
          def read
            @client.with_streams_connection do |conn|
              row = conn.exec_params(SQL, [@like]).first
              { maxc: row["maxc"].to_i, used: row["used"].to_i, peers: row["peers"].to_i }
            end
          rescue StandardError => e
            Pgbus.logger.debug { "[Pgbus::Streamer::PoolAutoscaler] headroom probe failed: #{e.class}: #{e.message}" }
            nil
          end
        end
      end
    end
  end
end
