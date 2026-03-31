# frozen_string_literal: true

module Pgbus
  # Lightweight instrumentation via ActiveSupport::Notifications.
  #
  # All events are prefixed with "pgbus." and carry timing information
  # automatically when used with the block form of AS::Notifications.instrument.
  #
  # Events emitted:
  #   pgbus.client.send_message   — single message enqueue
  #   pgbus.client.send_batch     — batch enqueue
  #   pgbus.client.read_batch     — batch dequeue
  #   pgbus.client.read_message   — single message dequeue
  #   pgbus.executor.execute      — full job execution (deserialize + perform + archive)
  #   pgbus.serializer.serialize  — job/event serialization
  #   pgbus.serializer.deserialize — job/event deserialization
  #
  module Instrumentation
    module_function

    def instrument(event, payload = {}, &block)
      if defined?(ActiveSupport::Notifications)
        ActiveSupport::Notifications.instrument(event, payload, &block)
      elsif block
        yield payload
      end
    end
  end
end
