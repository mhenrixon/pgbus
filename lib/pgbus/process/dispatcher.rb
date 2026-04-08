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

      # Page size for archive compaction. Each cycle deletes up to this
      # many archived rows per queue. Tuned via constant rather than
      # configuration because the value rarely needs adjusting and a
      # too-small value just delays cleanup, never breaks anything.
      ARCHIVE_COMPACTION_BATCH_SIZE = 1000

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
        run_if_due(now, :@last_archive_compaction_at, ARCHIVE_COMPACTION_INTERVAL) { compact_archives }
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
        # Clean up truly orphaned uniqueness keys: rows whose referenced
        # message no longer exists in the PGMQ queue. This handles crashes
        # or queue truncation. It must NEVER delete a lock while the message
        # is still in the queue, even if the lock is "old" — recurring jobs
        # that fail and retry can hold locks for hours.
        reaped = reap_orphaned_uniqueness_keys
        Pgbus.logger.info { "[Pgbus] Reaped #{reaped} orphaned uniqueness keys" } if reaped.positive?
      end

      def reap_orphaned_uniqueness_keys
        keys = UniquenessKey.all.to_a
        return 0 if keys.empty?

        # Only consider locks that are old enough that we wouldn't be racing
        # an in-flight enqueue. visibility_timeout * 2 is the floor — anything
        # younger could be a freshly-acquired lock whose send_message hasn't
        # committed yet.
        threshold = Time.current - (config.visibility_timeout * 2)

        orphaned = keys.select do |key|
          next false unless key.created_at && key.created_at < threshold
          next false unless key.queue_name

          message_gone?(key)
        end

        return 0 if orphaned.empty?

        UniquenessKey.where(lock_key: orphaned.map(&:lock_key)).delete_all
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Uniqueness key cleanup failed: #{e.message}" }
        0
      end

      # Returns true if the message referenced by this lock is definitely gone
      # from the queue. Returns false otherwise (message present, or unknown).
      #
      # Routes through Pgbus::Client#message_exists? so all PGMQ access stays
      # behind the client interface. The client returns nil when it can't
      # determine the answer (queue table missing, etc.); we treat that as
      # "still here" — the reaper must NEVER delete a lock when in doubt.
      def message_gone?(key)
        msg_id = key.msg_id.to_i
        result = if msg_id.positive?
                   Pgbus.client.message_exists?(key.queue_name, msg_id: msg_id)
                 else
                   Pgbus.client.message_exists?(key.queue_name, uniqueness_key: key.lock_key)
                 end

        result == false
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Reap check failed for #{key.lock_key}: #{e.message}" }
        false
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

      def compact_archives
        retention = config.archive_retention
        return unless retention&.positive?

        cutoff = Time.current - retention
        batch_size = ARCHIVE_COMPACTION_BATCH_SIZE
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
