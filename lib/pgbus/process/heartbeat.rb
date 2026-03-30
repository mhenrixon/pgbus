# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Heartbeat
      INTERVAL = 60 # seconds
      ALIVE_THRESHOLD = 300 # 5 minutes

      attr_reader :process_record

      def initialize(kind:, metadata: {})
        @kind = kind
        @metadata = metadata
        @timer = nil
      end

      def start
        register_process
        @timer = Concurrent::TimerTask.new(execution_interval: INTERVAL) { beat }
        @timer.execute
      end

      def stop
        @timer&.shutdown
        deregister_process
      end

      def beat
        return unless @process_id && defined?(ActiveRecord::Base)

        ActiveRecord::Base.connection.execute(
          "UPDATE pgbus_processes SET last_heartbeat_at = NOW() WHERE id = $1",
          "Pgbus Heartbeat",
          [@process_id]
        )
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Heartbeat failed: #{e.message}" }
      end

      private

      def register_process
        return unless defined?(ActiveRecord::Base)

        result = ActiveRecord::Base.connection.exec_insert(
          "INSERT INTO pgbus_processes (kind, hostname, pid, metadata, last_heartbeat_at, created_at, updated_at) " \
          "VALUES ($1, $2, $3, $4, NOW(), NOW(), NOW()) RETURNING id",
          "Pgbus Register Process",
          [@kind, Socket.gethostname, ::Process.pid, JSON.generate(@metadata)]
        )
        @process_id = result.first["id"]
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Process registration failed: #{e.message}" }
      end

      def deregister_process
        return unless @process_id && defined?(ActiveRecord::Base)

        ActiveRecord::Base.connection.execute(
          "DELETE FROM pgbus_processes WHERE id = $1",
          "Pgbus Deregister Process",
          [@process_id]
        )
      rescue StandardError
        # Best effort — process is exiting anyway
      end
    end
  end
end
