# frozen_string_literal: true

module Pgbus
  module Process
    class Dispatcher
      include SignalHandler

      # Maintenance runs on coarser intervals than the main loop
      CLEANUP_INTERVAL = 3600               # Run idempotency cleanup every hour
      REAP_INTERVAL = 300                   # Run stale process reaping every 5 minutes
      CONCURRENCY_INTERVAL = 300            # Run concurrency cleanup every 5 minutes
      BATCH_CLEANUP_INTERVAL = 3600         # Run batch cleanup every hour
      RECURRING_CLEANUP_INTERVAL = 3600     # Run recurring execution cleanup every hour
      ARCHIVE_COMPACTION_INTERVAL = 3600    # Run archive compaction every hour
      OUTBOX_CLEANUP_INTERVAL = 3600 # Run outbox cleanup every hour
      JOB_LOCK_CLEANUP_INTERVAL = 300 # Run job lock cleanup every 5 minutes
      STATS_CLEANUP_INTERVAL = 3600 # Run stats cleanup every hour

      attr_reader :config

      def initialize(config: Pgbus.configuration)
        @config = config
        @shutting_down = false
        @last_cleanup_at = monotonic_now
        @last_reap_at = monotonic_now
        @last_concurrency_at = monotonic_now
        @last_batch_cleanup_at = monotonic_now
        @last_recurring_cleanup_at = monotonic_now
        @last_archive_compaction_at = monotonic_now
        @last_outbox_cleanup_at = monotonic_now
        @last_job_lock_cleanup_at = monotonic_now
        @last_stats_cleanup_at = monotonic_now
      end

      def run
        setup_signals
        start_heartbeat
        Pgbus.logger.info do
          "[Pgbus] Dispatcher started: interval=#{config.dispatch_interval}s"
        end

        loop do
          break if @shutting_down

          process_signals
          break if @shutting_down

          run_maintenance
          break if @shutting_down

          interruptible_sleep(config.dispatch_interval)
        end

        shutdown
      end

      def graceful_shutdown
        @shutting_down = true
      end

      def immediate_shutdown
        @shutting_down = true
      end

      private

      def run_maintenance
        now = monotonic_now

        run_if_due(now, :@last_cleanup_at, CLEANUP_INTERVAL) { cleanup_processed_events }
        run_if_due(now, :@last_reap_at, REAP_INTERVAL) { reap_stale_processes }
        run_if_due(now, :@last_concurrency_at, CONCURRENCY_INTERVAL) { cleanup_concurrency }
        run_if_due(now, :@last_batch_cleanup_at, BATCH_CLEANUP_INTERVAL) { cleanup_batches }
        run_if_due(now, :@last_recurring_cleanup_at, RECURRING_CLEANUP_INTERVAL) { cleanup_recurring_executions }
        run_if_due(now, :@last_archive_compaction_at, archive_compaction_interval) { compact_archives }
        run_if_due(now, :@last_stream_archive_compaction_at, archive_compaction_interval) { prune_stream_archives }
        run_if_due(now, :@last_outbox_cleanup_at, OUTBOX_CLEANUP_INTERVAL) { cleanup_outbox }
        run_if_due(now, :@last_job_lock_cleanup_at, JOB_LOCK_CLEANUP_INTERVAL) { cleanup_job_locks }
        run_if_due(now, :@last_stats_cleanup_at, STATS_CLEANUP_INTERVAL) { cleanup_stats }
      end

      # Only update the timestamp when the block succeeds.
      # On failure, the next tick retries instead of waiting the full interval.
      def run_if_due(now, ivar, interval)
        return unless now - instance_variable_get(ivar) >= interval

        yield
        instance_variable_set(ivar, now)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Dispatcher maintenance error: #{e.message}" }
      end

      def cleanup_processed_events
        ttl = config.idempotency_ttl
        return unless ttl&.positive?

        deleted = ProcessedEvent.expired(Time.current - ttl).delete_all
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} expired processed events" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Idempotency cleanup failed: #{e.message}" }
      end

      def reap_stale_processes
        threshold = Heartbeat::ALIVE_THRESHOLD
        deleted = ProcessEntry.stale(Time.current - threshold).delete_all
        Pgbus.logger.info { "[Pgbus] Reaped #{deleted} stale processes" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Stale process reaping failed: #{e.message}" }
      end

      def cleanup_concurrency
        expired_keys = Concurrency::Semaphore.expire_stale
        expired_keys.each do |row|
          release_blocked_for_key(row["key"])
        end

        orphaned = Concurrency::BlockedExecution.expire_stale
        Pgbus.logger.debug { "[Pgbus] Expired #{orphaned} orphaned blocked executions" } if orphaned.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Concurrency cleanup failed: #{e.message}" }
      end

      def release_blocked_for_key(key)
        promoted = Concurrency::BlockedExecution.promote_next(key, client: Pgbus.client)
        Pgbus.logger.debug { "[Pgbus] Released blocked execution for key: #{key}" } if promoted
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Failed to release blocked execution for #{key}: #{e.message}" }
      end

      def cleanup_batches
        deleted = Batch.cleanup(older_than: Time.current - (7 * 24 * 3600)) # 7 days
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} finished batches" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Batch cleanup failed: #{e.message}" }
      end

      def cleanup_stats
        return unless config.stats_enabled

        retention = config.stats_retention
        return unless retention&.positive?

        deleted = JobStat.cleanup!(older_than: Time.current - retention)
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} old job stats" } if deleted.positive?
      end

      def cleanup_job_locks
        # Clean up orphaned uniqueness keys whose msg_id no longer exists
        # in any PGMQ queue. This handles the rare case where a message is
        # lost (e.g., queue table truncated) but the uniqueness key remains.
        reaped = reap_orphaned_uniqueness_keys
        Pgbus.logger.info { "[Pgbus] Reaped #{reaped} orphaned uniqueness keys" } if reaped.positive?
      end

      def reap_orphaned_uniqueness_keys
        keys = UniquenessKey.all.to_a
        return 0 if keys.empty?

        threshold = Time.current - (config.visibility_timeout * 2)

        orphaned = keys.select do |key|
          # msg_id == 0 means pre-produce placeholder or :while_executing lock.
          # These are live locks — never reap them based on msg_id alone.
          # Only reap if old enough that the job is certainly gone.
          next false if key.msg_id.zero? && (!key.created_at || key.created_at >= threshold)
          next true if key.msg_id.zero? && key.created_at && key.created_at < threshold

          # For real msg_ids, only reap if stale (old enough that VT has
          # long expired). The message itself may still be in the queue
          # awaiting retry — age is the only safe signal without scanning
          # every queue table.
          key.created_at && key.created_at < threshold
        end

        return 0 if orphaned.empty?

        UniquenessKey.where(lock_key: orphaned.map(&:lock_key)).delete_all
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Uniqueness key cleanup failed: #{e.message}" }
        0
      end

      def cleanup_outbox
        return unless config.outbox_enabled

        retention = config.outbox_retention
        return unless retention&.positive?

        deleted = OutboxEntry.published_before(Time.current - retention).delete_all
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} published outbox entries" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Outbox cleanup failed: #{e.message}" }
      end

      def archive_compaction_interval
        config.archive_compaction_interval || ARCHIVE_COMPACTION_INTERVAL
      end

      def compact_archives
        retention = config.archive_retention
        return unless retention&.positive?

        cutoff = Time.current - retention
        batch_size = config.archive_compaction_batch_size || 1000
        prefix = config.queue_prefix

        conn = config.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection
        queue_names = conn.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")

        queue_names.each do |full_name|
          next unless full_name.start_with?("#{prefix}_")

          stripped = full_name.delete_prefix("#{prefix}_")
          deleted = Pgbus.client.purge_archive(stripped, older_than: cutoff, batch_size: batch_size)
          Pgbus.logger.debug { "[Pgbus] Compacted #{deleted} archive entries from #{full_name}" } if deleted.positive?
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Archive compaction failed for #{full_name}: #{e.message}" }
        end
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Archive compaction failed: #{e.message}" }
      end

      # Prunes per-stream archive tables. Unlike compact_archives (which
      # uses the global archive_retention, typically 7 days), streams need
      # per-stream retention because a chat-history stream and a
      # presence-ping stream have wildly different storage requirements.
      # Lookup order for a given queue: exact string match, then regex
      # match, then streams_default_retention (5 minutes by default).
      def prune_stream_archives
        prefix = config.streams_queue_prefix
        return if prefix.nil? || prefix.empty?

        batch_size = config.archive_compaction_batch_size || 1000
        conn = config.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection
        queue_names = conn.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")

        queue_names.each do |full_name|
          next unless full_name.start_with?("#{prefix}_")

          retention = retention_for_stream_queue(full_name)
          next unless retention.positive?

          cutoff = Time.current - retention
          stripped = full_name.delete_prefix("#{config.queue_prefix}_")
          deleted = Pgbus.client.purge_archive(stripped, older_than: cutoff, batch_size: batch_size)
          if deleted.positive?
            Pgbus.logger.debug do
              "[Pgbus] Compacted #{deleted} stream archive entries from #{full_name}"
            end
          end
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Stream archive compaction failed for #{full_name}: #{e.message}" }
        end
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Stream archive compaction failed: #{e.message}" }
      end

      def retention_for_stream_queue(full_name)
        retention_map = config.streams_retention || {}
        # Exact-match first — cheapest and most common path
        exact = retention_map[full_name]
        return exact.to_f if exact

        # Regex-match second for pattern-based overrides (e.g. /^chat_/)
        retention_map.each do |key, value|
          return value.to_f if key.is_a?(Regexp) && key.match?(full_name)
        end

        config.streams_default_retention.to_f
      end

      def cleanup_recurring_executions
        retention = config.recurring_execution_retention
        return unless retention&.positive?

        deleted = RecurringExecution.older_than(Time.current - retention).delete_all
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} old recurring executions" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Recurring execution cleanup failed: #{e.message}" }
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(kind: "dispatcher", metadata: { pid: ::Process.pid })
        @heartbeat.start
      end

      def shutdown
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Dispatcher stopped" }
      end
    end
  end
end
