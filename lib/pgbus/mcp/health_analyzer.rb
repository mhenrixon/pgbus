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

      def initialize(data_source)
        @data_source = data_source
      end

      # Returns a machine-readable verdict hash suitable for both interactive
      # agent use and automated alerting.
      def verdict
        queues          = @data_source.queues_with_metrics
        processes       = @data_source.processes
        health, health_error = safe_queue_health

        # Partition queues once: non-DLQ (the operational set) and the subset
        # of those with visible, claimable backlog. Paused queues are removed
        # from the STALLED backlog (an intentional pause is not the wedge —
        # it's reported under DEGRADED), but kept in `non_dlq` for the summary.
        non_dlq = queues.reject { |q| dlq?(q) }
        backlog = non_dlq.select { |q| q[:queue_visible_length].to_i.positive? }
        active_backlog = backlog.reject { |q| q[:paused] }

        stalled  = stalled_reasons(active_backlog, processes)
        degraded = degraded_reasons(queues, backlog, processes, health, health_error)

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

      # STALLED detection: an active (non-paused) backed-up queue while a
      # worker is in the :stalled state, or backed up with live-but-idle
      # workers present.
      def stalled_reasons(backlog, processes)
        return [] if backlog.empty?

        workers = processes.select { |p| p[:kind] == WORKER_KIND }
        return [] if workers.empty?

        stalled_workers = workers.select { |w| w[:status].to_s == "stalled" }
        live_workers    = workers.select { |w| %w[healthy stalled].include?(w[:status].to_s) }

        reasons = []
        if stalled_workers.any?
          reasons << "#{stalled_workers.size} worker(s) stalled (heart-beating but claim loop not advancing) " \
                     "while #{backlog.size} queue(s) have visible backlog: #{backlog_names(backlog)}"
        elsif live_workers.any? && all_unread?(backlog)
          reasons << "#{backlog.size} queue(s) have visible messages with read_ct=0 (never claimed) " \
                     "while #{live_workers.size} worker(s) are alive: #{backlog_names(backlog)}"
        end
        reasons
      end

      # DEGRADED detection: conditions worth surfacing that are not the wedge.
      def degraded_reasons(queues, backlog, processes, health, health_error)
        reasons = []

        reasons << "queue health stats unavailable: #{health_error.class}: #{health_error.message}" if health_error

        stale = processes.select { |p| p[:status].to_s == "stale" }
        reasons << "#{stale.size} process(es) stale (no recent heartbeat)" if stale.any?

        paused_backlog = backlog.select { |q| q[:paused] }
        reasons << "#{paused_backlog.size} paused queue(s) holding a backlog: #{backlog_names(paused_backlog)}" if paused_backlog.any?

        dlq = queues.select { |q| dlq?(q) && q[:queue_length].to_i.positive? }
        reasons << "#{dlq.size} dead-letter queue(s) hold messages" if dlq.any?

        age = health[:oldest_transaction_age_sec]
        reasons << "oldest open transaction is #{age}s old (MVCC horizon pinning risk)" if age && age > 300

        reasons
      end

      # The strongest wedge signal: visible messages that have never been read.
      # When queue metrics don't expose read_ct we conservatively treat the
      # backlog as unread (the wedge default) so we don't miss the condition.
      def all_unread?(backlog)
        backlog.all? { |q| !q.key?(:max_read_ct) || q[:max_read_ct].to_i.zero? }
      end

      def backlog_names(queues)
        queues.map { |q| q[:name] }.join(", ")
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
