# frozen_string_literal: true

require "concurrent"
require "socket"

module Pgbus
  module Process
    class Heartbeat
      INTERVAL = 60 # seconds
      ALIVE_THRESHOLD = 300 # 5 minutes

      attr_reader :process_entry

      def initialize(kind:, metadata: {}, on_beat: nil)
        @kind = kind
        @metadata = metadata
        @on_beat = on_beat
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

        @on_beat&.call
        ProcessEntry.where(id: @process_id).update_all(last_heartbeat_at: Time.current)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Heartbeat failed: #{e.message}" }
      end

      private

      def register_process
        record = ProcessEntry.create!(
          kind: @kind,
          hostname: Socket.gethostname,
          pid: ::Process.pid,
          metadata: @metadata,
          last_heartbeat_at: Time.current
        )
        @process_id = record.id
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Process registration failed: #{e.message}" }
      end

      def deregister_process
        return unless @process_id

        ProcessEntry.where(id: @process_id).delete_all
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Process deregistration failed: #{e.message}" }
      end
    end
  end
end
