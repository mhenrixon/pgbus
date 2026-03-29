# frozen_string_literal: true

module Pgbus
  module EventBus
    module Publisher
      module_function

      def publish(routing_key, payload, headers: nil, delay: 0)
        event_data = build_event_data(payload)

        if Pgbus.client.pgmq.respond_to?(:produce_topic)
          Pgbus.client.publish_to_topic(routing_key, event_data, headers: headers, delay: delay)
        else
          # Fallback: send directly to queues matching the routing key
          Pgbus.client.send_message(routing_key, event_data, headers: headers, delay: delay)
        end
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
