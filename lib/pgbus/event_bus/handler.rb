# frozen_string_literal: true

module Pgbus
  module EventBus
    class Handler
      class << self
        def idempotent!
          @idempotent = true
        end

        def idempotent?
          @idempotent == true
        end
      end

      def process(message)
        raw = JSON.parse(message.message)
        event = build_event(raw)

        if self.class.idempotent?
          return :skipped if already_processed?(event.event_id)

          mark_processed!(event.event_id)
        end

        handle(event)
        :handled
      end

      def handle(event)
        raise NotImplementedError, "#{self.class.name} must implement #handle(event)"
      end

      private

      def build_event(raw)
        payload = raw["payload"]
        if payload.is_a?(Hash) && payload["_global_id"]
          payload = GlobalID::Locator.locate(payload["_global_id"])
        end

        Event.new(
          event_id: raw["event_id"],
          payload: payload,
          published_at: raw["published_at"] ? Time.parse(raw["published_at"]) : nil
        )
      end

      def already_processed?(event_id)
        return false unless defined?(ActiveRecord::Base)

        ActiveRecord::Base.connection.select_value(
          "SELECT 1 FROM pgbus_processed_events WHERE event_id = $1 AND handler_class = $2",
          "Pgbus Idempotency Check",
          [event_id, self.class.name]
        )
      end

      def mark_processed!(event_id)
        return unless defined?(ActiveRecord::Base)

        ActiveRecord::Base.connection.exec_insert(
          "INSERT INTO pgbus_processed_events (event_id, handler_class, processed_at) " \
          "VALUES ($1, $2, $3) ON CONFLICT (event_id, handler_class) DO NOTHING",
          "Pgbus Idempotency Mark",
          [event_id, self.class.name, Time.now.utc]
        )
      end
    end
  end
end
