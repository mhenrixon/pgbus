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
        instrument("pgbus.event_processed", event_id: event.event_id, handler: self.class.name)
        :handled
      end

      def handle(event)
        raise NotImplementedError, "#{self.class.name} must implement #handle(event)"
      end

      private

      def build_event(raw)
        payload = raw["payload"]
        payload = GlobalID::Locator.locate(payload["_global_id"]) if payload.is_a?(Hash) && payload["_global_id"]

        Event.new(
          event_id: raw["event_id"],
          payload: payload,
          published_at: raw["published_at"] ? Time.parse(raw["published_at"]) : nil
        )
      end

      def instrument(event_name, payload = {})
        return unless defined?(ActiveSupport::Notifications)

        ActiveSupport::Notifications.instrument(event_name, payload)
      end

      def already_processed?(event_id)
        ProcessedEvent.exists?(event_id: event_id, handler_class: self.class.name)
      end

      def mark_processed!(event_id)
        ProcessedEvent.upsert(
          { event_id: event_id, handler_class: self.class.name, processed_at: Time.now.utc },
          unique_by: %i[event_id handler_class]
        )
      end
    end
  end
end
