# frozen_string_literal: true

module Pgbus
  module MCP
    # Computes the top-level pgbus health verdict (OK / DEGRADED / STALLED)
    # from the existing DataSource read layer. This is the single signal that
    # catches the silent-worker-wedge class of incident (#179, #174, #181):
    # a queue with visible messages and no claim progress while a subscribing
    # worker is heart-beating with idle capacity.
    #
    # Verdict semantics (issue #180 acceptance criteria):
    #   STALLED  — backlog (visible > 0) AND at least one worker is heart-beating
    #              but its claim loop has stopped advancing (status :stalled),
    #              OR backlog with live-but-idle workers and zero claim progress.
    #   DEGRADED — something is wrong but not the wedge: stale processes, a
    #              paused queue holding a backlog, growing DLQ, or MVCC horizon
    #              pinned by a long-running transaction.
    #   OK       — draining normally / nothing actionable.
    class HealthAnalyzer
      # A worker is considered to have idle capacity unless its metadata
      # explicitly reports it is saturated. We treat the presence of any live
      # worker as "has capacity" because a wedged worker reports healthy
      # heartbeats while doing no work — exactly the case we must catch.
      WORKER_KIND = "worker"

      # Cap on how many queue names a reason string lists before truncating to
      # "(+N more)" — keeps a flagged fleet of hundreds of queues from producing
      # a multi-KB log line (issue #367 minor).
      NAME_LIMIT = 10

      def initialize(data_source)
        @data_source = data_source
      end

      # Returns a machine-readable verdict hash suitable for both interactive
      # agent use and automated alerting.
      def verdict
        queues          = @data_source.queues_with_metrics
        processes       = @data_source.processes
        stream_names    = @data_source.stream_queue_names
        health, health_error = safe_queue_health

        # Partition queues once: the operational set excludes DLQs and
        # registered stream queues, and the backlog is the subset with
        # visible, claimable messages. Stream queues are excluded like DLQs
        # (issue #359): stream delivery is a non-consuming peek, so their
        # messages are permanently visible with read_ct=0 — exactly the wedge
        # signature — and counting them makes the verdict scream STALLED on
        # every streams-heavy install. Paused queues are removed from the
        # STALLED backlog (an intentional pause is not the wedge — it's
        # reported under DEGRADED), but kept in `non_dlq` for the summary.
        non_dlq = queues.reject { |q| dlq?(q) || stream_names.include?(q[:name].to_s) }
        backlog = non_dlq.select { |q| q[:queue_visible_length].to_i.positive? }
        active_backlog = backlog.reject { |q| q[:paused] }

        # Split the active backlog by whether some configured capsule or handler
        # drains the queue (issue #367 defect 2). Live workers say nothing about
        # a queue they can never claim from, so only the drained side can be the
        # silent-worker-wedge; the undrained side is its own DEGRADED hazard.
        drained, undrained = partition_by_drain(active_backlog)

        stalled  = stalled_reasons(active_backlog, drained, processes)
        degraded = degraded_reasons(queues, backlog, undrained, processes, health, health_error)

        status = if stalled.any?
                   "STALLED"
                 elsif degraded.any?
                   "DEGRADED"
                 else
                   "OK"
                 end

        {
          status: status,
          reasons: stalled + degraded,
          checked_at: Time.now.utc.iso8601,
          summary: build_summary(queues, non_dlq, processes, health)
        }
      end

      private

      def dlq?(queue)
        queue[:name].to_s.end_with?(Pgbus::DEAD_LETTER_SUFFIX)
      end

      # Partition a backlog into [drained, undrained] using the set of queues
      # some configured capsule or EventBus handler drains. A nil drained set
      # means a wildcard capsule drains everything, so nothing is undrained.
      def partition_by_drain(backlog)
        return [backlog, []] if backlog.empty?

        drained_names = @data_source.drained_queue_names
        return [backlog, []] if drained_names.nil?

        backlog.partition { |q| drained_names.include?(q[:name].to_s) }
      end

      # Returns [health_hash, error_or_nil]. We never raise — pgbus_health
      # must always produce a verdict — but we DO surface the failure so the
      # caller knows the verdict was built from partial data, and we log it
      # for operators (per the project's no-silent-errors rule).
      def safe_queue_health
        [@data_source.queue_health_stats, nil]
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus::MCP] queue_health_stats failed: #{e.class}: #{e.message}" }
        [{}, e]
      end

      # STALLED detection. Two distinct wedge signatures, with different scopes:
      #
      #   * A worker self-reporting :stalled (heart-beating but claim loop frozen)
      #     is a wedge regardless of WHICH queue holds the backlog — the worker is
      #     broken. So this branch reasons against the FULL active backlog.
      #   * Live-but-idle workers with a never-claimed backlog is only a wedge for
      #     queues those workers can actually claim from — so this branch reasons
      #     against the DRAINED backlog only (issue #367 defect 2). Worker liveness
      #     is the evidence here, and it is evidence only for drained queues.
      def stalled_reasons(active_backlog, drained_backlog, processes)
        workers = processes.select { |p| p[:kind] == WORKER_KIND }
        return [] if workers.empty?

        stalled_workers = workers.select { |w| w[:status].to_s == "stalled" }
        live_workers    = workers.select { |w| %w[healthy stalled].include?(w[:status].to_s) }

        reasons = []
        if stalled_workers.any? && active_backlog.any?
          reasons << "#{stalled_workers.size} worker(s) stalled (heart-beating but claim loop not advancing) " \
                     "while #{active_backlog.size} queue(s) have visible backlog: #{backlog_names(active_backlog)}"
        elsif live_workers.any? && drained_backlog.any? && any_unread?(drained_backlog)
          unread = drained_backlog.select { |q| unread?(q) }
          reasons << "#{unread.size} queue(s) have visible messages with read_ct=0 (never claimed) " \
                     "while #{live_workers.size} worker(s) are alive: #{backlog_names(unread)}"
        end
        reasons
      end

      # DEGRADED detection: conditions worth surfacing that are not the wedge.
      # +undrained+ is the active backlog on queues no configured capsule/handler
      # drains (issue #367 defect 2) — a real hazard (nothing will empty them),
      # but not the worker wedge, so it is DEGRADED rather than STALLED.
      def degraded_reasons(queues, backlog, undrained, processes, health, health_error)
        reasons = []

        reasons << "queue health stats unavailable: #{health_error.class}: #{health_error.message}" if health_error

        stale = processes.select { |p| p[:status].to_s == "stale" }
        reasons << "#{stale.size} process(es) stale (no recent heartbeat)" if stale.any?

        paused_backlog = backlog.select { |q| q[:paused] }
        reasons << "#{paused_backlog.size} paused queue(s) holding a backlog: #{backlog_names(paused_backlog)}" if paused_backlog.any?

        # Only queues holding visible never-claimed messages are the "nobody
        # drains this" hazard — if something is claiming the queue (visible rows
        # have been read) an absent capsule isn't why it holds a backlog.
        undrained_unread = undrained.select { |q| unread?(q) }
        if undrained_unread.any?
          reasons << "#{undrained_unread.size} queue(s) hold messages but no capsule is " \
                     "configured to drain them: #{backlog_names(undrained_unread)}"
        end

        dlq = queues.select { |q| dlq?(q) && q[:queue_length].to_i.positive? }
        reasons << "#{dlq.size} dead-letter queue(s) hold messages" if dlq.any?

        age = health[:oldest_transaction_age_sec]
        reasons << "oldest open transaction is #{age}s old (MVCC horizon pinning risk)" if age && age > 300

        reasons
      end

      # True when any queue in the backlog holds a visible, never-claimed
      # message — the wedge signal.
      def any_unread?(backlog)
        backlog.any? { |q| unread?(q) }
      end

      # True when a queue has at least one visible (claimable) message with
      # read_ct=0. This counts per visible message (issue #367 review), so one
      # retried or in-flight message (read_ct>0) can no longer veto the signal
      # for a pile of genuinely-unclaimed jobs the way max(read_ct) did.
      #
      # When metrics don't expose the count (an old hash / a stubbed test) we
      # conservatively treat the queue as unread so we never miss the wedge.
      def unread?(queue)
        return true unless queue.key?(:visible_unread_length)

        queue[:visible_unread_length].to_i.positive?
      end

      # Join backlog queue names for a reason string, truncated so a flagged
      # fleet of hundreds of queues doesn't produce a multi-KB log line
      # (issue #367 minor). Lists the first NAME_LIMIT, then "(+N more)".
      def backlog_names(queues)
        names = queues.map { |q| q[:name] }
        shown = names.first(NAME_LIMIT).join(", ")
        overflow = names.size - NAME_LIMIT
        overflow.positive? ? "#{shown} (+#{overflow} more)" : shown
      end

      def build_summary(queues, non_dlq, processes, health)
        workers = stale = stalled = 0
        processes.each do |p|
          status = p[:status].to_s
          stale += 1 if status == "stale"
          next unless p[:kind] == WORKER_KIND

          workers += 1
          stalled += 1 if status == "stalled"
        end

        {
          queues: queues.size,
          total_visible: non_dlq.sum { |q| q[:queue_visible_length].to_i },
          total_depth: non_dlq.sum { |q| q[:queue_length].to_i },
          workers: workers,
          stalled_workers: stalled,
          stale_processes: stale,
          oldest_transaction_age_sec: health[:oldest_transaction_age_sec]
        }
      end
    end
  end
end
