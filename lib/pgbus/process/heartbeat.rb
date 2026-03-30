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
        return unless @process_id

        ProcessRecord.where(id: @process_id).update_all(last_heartbeat_at: Time.current)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Heartbeat failed: #{e.message}" }
      end

      private

      def register_process
        record = ProcessRecord.create!(
          kind: @kind,
          hostname: Socket.gethostname,
          pid: ::Process.pid,
          metadata: JSON.generate(@metadata),
          last_heartbeat_at: Time.current
        )
        @process_id = record.id
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Process registration failed: #{e.message}" }
      end

      def deregister_process
        return unless @process_id

        ProcessRecord.where(id: @process_id).delete_all
      rescue StandardError
        # Best effort — process is exiting anyway
      end
    end
  end
end
