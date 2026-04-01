# frozen_string_literal: true

module Pgbus
  module Outbox
    module_function

    def publish(queue_name, payload, headers: nil, priority: nil, delay: 0)
      OutboxEntry.create!(
        queue_name: queue_name,
        payload: payload,
        headers: headers,
        priority: priority || Pgbus.configuration.default_priority,
        delay: delay
      )
    end

    def publish_event(routing_key, payload, headers: nil)
      event_data = EventBus::Publisher.build_event_data(payload)
      OutboxEntry.create!(
        routing_key: routing_key,
        payload: event_data,
        headers: headers
      )
    end

    def flush!
      Poller.new.poll_and_publish
    end
  end
end
