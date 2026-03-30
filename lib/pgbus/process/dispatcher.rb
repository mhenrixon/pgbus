# frozen_string_literal: true

module Pgbus
  module Process
    class Dispatcher
      include SignalHandler

      attr_reader :config

      def initialize(config: Pgbus.configuration)
        @config = config
        @shutting_down = false
      end

      def run
        setup_signals
        start_heartbeat
        Pgbus.logger.info do
          "[Pgbus] Dispatcher started: interval=#{config.dispatch_interval}s batch=#{config.dispatch_batch_size}"
        end

        loop do
          break if @shutting_down

          process_signals
          dispatched = dispatch_scheduled
          sleep(dispatched < config.dispatch_batch_size ? config.dispatch_interval : 0)
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

      def dispatch_scheduled
        return 0 unless defined?(ActiveRecord::Base)

        result = fetch_due_events
        result.each do |row|
          enqueue_event(row)
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus] Failed to dispatch event: #{e.message}" }
          track_failed_dispatch(row, e)
        end

        count = result.count
        Pgbus.logger.debug { "[Pgbus] Dispatched #{count} scheduled events" } if count.positive?
        count
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Dispatcher error: #{e.message}" }
        0
      end

      def fetch_due_events
        ActiveRecord::Base.connection.execute(<<~SQL)
          DELETE FROM pgbus_scheduled_events
          WHERE id IN (
            SELECT id FROM pgbus_scheduled_events
            WHERE scheduled_at <= NOW()
            ORDER BY scheduled_at ASC
            LIMIT #{config.dispatch_batch_size}
            FOR UPDATE SKIP LOCKED
          )
          RETURNING queue_name, payload, headers
        SQL
      end

      def enqueue_event(row)
        Pgbus.client.send_message(
          row["queue_name"],
          JSON.parse(row["payload"]),
          headers: row["headers"] ? JSON.parse(row["headers"]) : nil
        )
      end

      def track_failed_dispatch(row, error)
        conn = ActiveRecord::Base.connection
        conn.execute(<<~SQL)
          INSERT INTO pgbus_failed_events (queue_name, payload, headers, error_message, error_class, failed_at)
          VALUES (
            #{conn.quote(row["queue_name"])},
            #{conn.quote(row["payload"])},
            #{conn.quote(row["headers"])},
            #{conn.quote(error.message)},
            #{conn.quote(error.class.name)},
            NOW()
          )
        SQL
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Failed to track dispatch failure: #{e.message}" }
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
