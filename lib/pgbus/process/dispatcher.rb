# frozen_string_literal: true

module Pgbus
  module Process
    class Dispatcher
      include SignalHandler

      # Maintenance runs on coarser intervals than the main loop
      CLEANUP_INTERVAL = 3600       # Run idempotency cleanup every hour
      REAP_INTERVAL = 300           # Run stale process reaping every 5 minutes

      attr_reader :config

      def initialize(config: Pgbus.configuration)
        @config = config
        @shutting_down = false
        @last_cleanup_at = Time.now
        @last_reap_at = Time.now
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

          sleep(config.dispatch_interval)
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
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Dispatcher maintenance error: #{e.message}" }
      end

      def cleanup_processed_events
        return unless defined?(ActiveRecord::Base)

        ttl = config.idempotency_ttl
        return unless ttl&.positive?

        deleted = ActiveRecord::Base.connection.delete(
          "DELETE FROM pgbus_processed_events WHERE processed_at < $1",
          "Pgbus Idempotency Cleanup",
          [Time.now.utc - ttl]
        )
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} expired processed events" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Idempotency cleanup failed: #{e.message}" }
      end

      def reap_stale_processes
        return unless defined?(ActiveRecord::Base)

        threshold = Heartbeat::ALIVE_THRESHOLD
        deleted = ActiveRecord::Base.connection.delete(
          "DELETE FROM pgbus_processes WHERE last_heartbeat_at < $1",
          "Pgbus Stale Process Reap",
          [Time.now.utc - threshold]
        )
        Pgbus.logger.info { "[Pgbus] Reaped #{deleted} stale processes" } if deleted.positive?
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Stale process reaping failed: #{e.message}" }
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
