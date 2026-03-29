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
        Pgbus.logger.info { "[Pgbus] Dispatcher started: interval=#{config.dispatch_interval}s batch=#{config.dispatch_batch_size}" }

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

        result = ActiveRecord::Base.connection.execute(<<~SQL)
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

        result.each do |row|
          Pgbus.client.send_message(
            row["queue_name"],
            JSON.parse(row["payload"]),
            headers: row["headers"] ? JSON.parse(row["headers"]) : nil
          )
        end

        count = result.count
        Pgbus.logger.debug { "[Pgbus] Dispatched #{count} scheduled events" } if count > 0
        count
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Dispatcher error: #{e.message}" }
        0
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
