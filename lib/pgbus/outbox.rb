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
        # routing_key: in the envelope so the consumer can dispatch the relayed
        # event to handlers — pgmq.send_topic matches queues at relay time but
        # stamps nothing on the message, and Consumer#handle_message reads the
        # routing key from the envelope (fixed alongside issue #431; previously
        # relayed outbox events matched zero handlers and were archived unrun).
        event_data = EventBus::Publisher.build_event_data(payload, routing_key: routing_key)
        event_data = EventBus::Publisher.tag_fair_share(event_data, payload, routing_key: routing_key, headers: headers)
        event_data = EventBus::Publisher.tag_current(event_data)
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
