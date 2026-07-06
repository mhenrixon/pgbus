# frozen_string_literal: true

module Pgbus
  module Streams
    # Self-tuning decision logic for the dedicated streams DB pool (issue #323).
    #
    # A PURE DECISION OBJECT — it owns no thread and no connection. Two callers
    # drive it, both feeding it a live-headroom reading:
    #   * the streamer's Listener, as a periodic maintenance check on its idle
    #     LISTEN connection (see Maintenance below); and
    #   * a publisher (Client#send_stream_message), throttled, on the job pool.
    # #evaluate reads the streams pool's busy ratio, decides, and resizes via the
    # #325 hot-swap primitive.
    #
    # Cadence is slow (streams_pool_autoscale_interval, default 5 min), so the
    # interval itself is the debounce: it acts on each check with no multi-sample
    # hysteresis and no cooldown. A sustained burst converges over a few checks
    # (one grow step each); a swap's transient (a freshly-swapped connection_pool
    # is lazy → reads busy ≈ 0) simply means "no shrink this check" and is gone by
    # the next check minutes later.
    #
    # Decision priority per check:
    #   1. EMERGENCY SHRINK — DB free connections critically low → resize to
    #      baseline now (protect the DB; overrides the busy signal).
    #   2. GROW — pool saturated AND a fair share of real headroom exists.
    #   3. SHRINK — pool idle → step toward baseline.
    #   4. HOLD.
    #
    # SAFETY (proven in the #323 design; independent of cadence):
    #   * No multi-process exhaustion: four stacked grow guards (GROW_RESERVE
    #     gate, SAFETY peer inflation, STEP_MAX, per-process floor(free/2)).
    #   * No grow↔emergency limit cycle: GROW_RESERVE (0.20·maxc) ≥ 4×
    #     EMERGENCY_MARGIN (0.05·maxc) → a ~15% dead-zone.
    class PoolAutoscaler
      GROW_THRESHOLD   = 0.85 # busy_ratio to grow
      SHRINK_THRESHOLD = 0.30 # busy_ratio to shrink
      FAIR_FRACTION    = 0.25 # claim only ¼ of the computed fair share per grow
      SAFETY           = 1.5  # inflate peer count → deflate fair share (undercount guard)
      STEP_MAX         = 4    # hard cap on connections added per single grow

      # SQL a caller runs on a connection to gather headroom. Public so the
      # Listener/publisher can issue it; $1 is the application_name LIKE pattern.
      HEADROOM_SQL = <<~SQL
        SELECT current_setting('max_connections')::int AS maxc,
               count(*) AS used,
               count(DISTINCT application_name)
                 FILTER (WHERE application_name LIKE $1) AS peers
        FROM pg_stat_activity
      SQL

      def initialize(client:, config:, logger: Pgbus.logger)
        @client = client
        @config = config
        @logger = logger
        @baseline = config.streams_pool_size
      end

      # One maintenance decision. `headroom` is {maxc:, used:, peers:} from a
      # caller's query, or nil if it failed (→ HOLD). Returns the action taken
      # (:emergency_shrink / :grow / :shrink / :hold) — handy for tests + logging.
      #
      # `allow_shrink:` gates the NORMAL idle shrink (priority 3). The streamer
      # passes true (it sees the sustained idle picture across replay reads). A
      # publisher passes false: it only ever reacts to its OWN publish pressure
      # (grow), and leaves idle-shrink to the streamer/consumer. Emergency shrink
      # is NEVER gated — a DB running out of connections must always be relieved,
      # whoever notices first.
      def evaluate(headroom, allow_shrink: true)
        return :hold if headroom.nil?

        free = headroom[:maxc] - headroom[:used]

        # PRIORITY 1 — EMERGENCY SHRINK (keys off DB `free`, an external fact).
        return emergency_shrink(free, headroom[:maxc]) if free < emergency_margin(headroom[:maxc])

        size, busy_ratio = pool_busy
        return :hold if size.nil?

        if busy_ratio >= GROW_THRESHOLD
          maybe_grow(size, free, headroom)
        elsif allow_shrink && busy_ratio < SHRINK_THRESHOLD
          maybe_shrink(size)
        else
          :hold
        end
      end

      private

      # PRIORITY 2 — GROW into a bounded fair share of live headroom.
      def maybe_grow(size, free, headroom)
        return :hold if free <= grow_reserve(headroom[:maxc]) # not enough headroom
        return :hold if size <= 1 # a telemetry checkout would starve publishing

        delta = grow_delta(free, headroom[:peers])
        return :hold if delta < 1

        target = size + delta
        cap = @config.streams_pool_max
        target = [target, cap].min if cap
        return :hold if target <= size

        act(size, target, :grow)
      end

      # PRIORITY 3 — SHRINK one step toward the baseline.
      def maybe_shrink(size)
        return :hold if size <= @baseline

        act(size, [@baseline, size - STEP_MAX].max, :shrink)
      end

      def emergency_shrink(free, maxc)
        return :hold if pool_current_size <= @baseline

        if swapped?(@client.resize_streams_pool(@baseline))
          @logger.warn do
            "[Pgbus::Streams::PoolAutoscaler] EMERGENCY streams-pool shrink to " \
              "#{@baseline} (free=#{free}/#{maxc})"
          end
          :emergency_shrink
        else
          # The DB is critically low on connections but the shrink was a no-op
          # (unchanged / shared-AR). This is the one case where a failed resize
          # matters most — leave a trace instead of silently reporting success.
          @logger.warn do
            "[Pgbus::Streams::PoolAutoscaler] EMERGENCY streams-pool shrink to " \
              "#{@baseline} did NOT apply (free=#{free}/#{maxc}) — resize was a no-op"
          end
          :hold
        end
      end

      # fair_share = free / (peers · SAFETY); claim FAIR_FRACTION, never more than
      # STEP_MAX or half the remaining headroom (leave ≥half for unknown peers →
      # geometric contraction → N processes can't collectively exhaust the DB).
      def grow_delta(free, peers)
        peer_count = [peers.to_i, 1].max
        fair_share = free.to_f / (peer_count * SAFETY)
        [(FAIR_FRACTION * fair_share).floor, STEP_MAX, (free / 2)].min
      end

      def act(from_size, target_size, kind)
        if swapped?(@client.resize_streams_pool(target_size))
          @logger.info { "[Pgbus::Streams::PoolAutoscaler] streams pool #{from_size}->#{target_size} (#{kind})" }
          kind
        else
          :hold # unchanged / shared-AR no-op
        end
      end

      # Margins self-derive from live max_connections; the 4× gap forbids a
      # grow↔emergency limit cycle.
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

      # Wraps an autoscaler as a Listener maintenance task: on each throttled idle
      # window the Listener calls #run(conn); this queries live headroom on THAT
      # connection (the streamer's existing LISTEN connection — no extra
      # connection) and hands it to the autoscaler. `interval` is the throttle
      # (seconds) the Listener honors between runs.
      class Maintenance
        attr_reader :interval

        def initialize(autoscaler:, interval:, application_name_prefix:)
          @autoscaler = autoscaler
          @interval = interval
          @like = "#{application_name_prefix}_%"
        end

        def run(conn)
          row = conn.exec_params(HEADROOM_SQL, [@like]).first
          # Explicit positional hash (braces) — evaluate now takes an optional
          # allow_shrink: keyword, so a bare hash would be parsed as keywords.
          # The streamer allows shrink (it sees the sustained idle picture).
          @autoscaler.evaluate(
            { maxc: row["maxc"].to_i, used: row["used"].to_i, peers: row["peers"].to_i },
            allow_shrink: true
          )
        end
      end
    end
  end
end
