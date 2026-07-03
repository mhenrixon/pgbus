# frozen_string_literal: true

require "concurrent"
require "socket"

module Pgbus
  module Process
    class Heartbeat
      INTERVAL = 60 # seconds
      ALIVE_THRESHOLD = 300 # 5 minutes

      attr_reader :process_entry

      def initialize(kind:, metadata: {}, on_beat: nil, loop_tick_supplier: nil, metadata_supplier: nil)
        @kind = kind
        @metadata = metadata
        @on_beat = on_beat
        @loop_tick_supplier = loop_tick_supplier
        @metadata_supplier = metadata_supplier
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
        updates = { last_heartbeat_at: Time.current }
        metadata = beat_metadata
        updates[:metadata] = metadata unless metadata.nil?
        ProcessEntry.where(id: @process_id).update_all(updates)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Heartbeat failed: #{e.message}" }
      end

      private

      # Build the metadata hash to persist on this beat. Returns nil when no
      # supplier is configured so heartbeats without dynamic metadata
      # (supervisor/dispatcher/scheduler) leave the column untouched, exactly
      # as before. The loop_tick and metadata suppliers are called here — after
      # @on_beat has run — so any snapshot!/refresh in on_beat is reflected.
      def beat_metadata
        return nil unless @loop_tick_supplier || @metadata_supplier

        metadata = @metadata.dup
        metadata["loop_tick_at"] = @loop_tick_supplier.call&.to_f if @loop_tick_supplier
        metadata.merge!(@metadata_supplier.call) if @metadata_supplier
        metadata
      end

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
