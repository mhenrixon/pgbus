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

      attr_reader :config

      def initialize(config: Pgbus.configuration)
        @config = config
        @shutting_down = false
        @last_cleanup_at = Time.now
        @last_reap_at = Time.now
        @last_concurrency_at = Time.now
        @last_batch_cleanup_at = Time.now
        @last_recurring_cleanup_at = Time.now
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
        now = Time.now

        if now - @last_cleanup_at >= CLEANUP_INTERVAL
          cleanup_processed_events
          @last_cleanup_at = now
        end

        if now - @last_reap_at >= REAP_INTERVAL
          reap_stale_processes
          @last_reap_at = now
        end

        if now - @last_concurrency_at >= CONCURRENCY_INTERVAL
          cleanup_concurrency
          @last_concurrency_at = now
        end

        if now - @last_batch_cleanup_at >= BATCH_CLEANUP_INTERVAL
          cleanup_batches
          @last_batch_cleanup_at = now
        end

        if now - @last_recurring_cleanup_at >= RECURRING_CLEANUP_INTERVAL
          cleanup_recurring_executions
          @last_recurring_cleanup_at = now
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Dispatcher maintenance error: #{e.message}" }
      end

      def cleanup_processed_events
        ttl = config.idempotency_ttl
        return unless ttl&.positive?

        deleted = ProcessedEvent.expired(Time.now.utc - ttl).delete_all
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} expired processed events" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Idempotency cleanup failed: #{e.message}" }
      end

      def reap_stale_processes
        threshold = Heartbeat::ALIVE_THRESHOLD
        deleted = ProcessEntry.stale(Time.now.utc - threshold).delete_all
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
        deleted = Batch.cleanup(older_than: Time.now.utc - (7 * 24 * 3600)) # 7 days
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} finished batches" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Batch cleanup failed: #{e.message}" }
      end

      def cleanup_recurring_executions
        retention = config.recurring_execution_retention
        return unless retention&.positive?

        deleted = RecurringExecution.older_than(Time.now.utc - retention).delete_all
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} old recurring executions" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Recurring execution cleanup failed: #{e.message}" }
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
