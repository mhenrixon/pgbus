# frozen_string_literal: true

module Pgbus
  module Outbox
    module_function

    def publish(queue_name, payload, headers: nil, priority: nil, delay: 0)
      Instrumentation.instrument("pgbus.outbox.publish", queue: queue_name, kind: :job) do
        OutboxEntry.create!(
          queue_name: queue_name,
          payload: payload,
          headers: headers,
          priority: priority || Pgbus.configuration.default_priority,
          delay: delay
        )
      end
    end

    def publish_event(routing_key, payload, headers: nil)
      Instrumentation.instrument("pgbus.outbox.publish", routing_key: routing_key, kind: :event) do
        event_data = EventBus::Publisher.build_event_data(payload)
        OutboxEntry.create!(
          routing_key: routing_key,
          payload: event_data,
          headers: headers
        )
      end
    end

    def flush!
      Poller.new.poll_and_publish
    end
  end
end
