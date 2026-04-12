# frozen_string_literal: true

require "time"

module Pgbus
  module EventBus
    module Publisher
      module_function

      def publish(routing_key, payload, headers: nil, delay: 0)
        event_data = build_event_data(payload)

        if defined?(Pgbus::Testing) && !Pgbus::Testing.disabled?
          event = Pgbus::Event.new(
            event_id: event_data["event_id"],
            payload: event_data["payload"],
            published_at: event_data["published_at"] ? Time.parse(event_data["published_at"]) : nil,
            routing_key: routing_key,
            headers: headers
          )

          Pgbus::Testing.store.push_event(event)

          if Pgbus::Testing.inline? && delay.to_i <= 0
            Pgbus::EventBus::Registry.instance.handlers_for(routing_key).each do |subscriber|
              subscriber.handler_class.new.handle(event)
            end
          end

          return event_data
        end

        Pgbus.client.publish_to_topic(routing_key, event_data, headers: headers, delay: delay)
      end

      def publish_later(routing_key, payload, delay:, headers: nil)
        publish(routing_key, payload, headers: headers, delay: delay)
      end

      def build_event_data(payload)
        event_id = SecureRandom.uuid

        serialized_payload = if payload.respond_to?(:to_global_id)
                               { "_global_id" => payload.to_global_id.to_s }
                             elsif payload.is_a?(Hash)
                               payload
                             else
                               { "value" => payload }
                             end

        {
          "event_id" => event_id,
          "payload" => serialized_payload,
          "published_at" => Time.now.utc.iso8601(6)
        }
      end
    end
  end
end
