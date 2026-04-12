# frozen_string_literal: true

module Pgbus
  module Testing
    # Framework-agnostic assertion helpers. Included by both RSpec and Minitest
    # integrations. Can also be included directly in any test class.
    #
    #   include Pgbus::Testing::Assertions
    #
    #   assert_pgbus_published(count: 1, routing_key: "orders.created") do
    #     Order.create!(...)
    #   end
    module Assertions
      def pgbus_published_events(routing_key: nil)
        Pgbus::Testing.store.events(routing_key: routing_key)
      end

      def assert_pgbus_published(count:, routing_key: nil)
        before = pgbus_published_events(routing_key: routing_key).size
        yield
        after = pgbus_published_events(routing_key: routing_key).size
        actual = after - before

        return if actual == count

        suffix = routing_key ? " matching #{routing_key.inspect}" : ""
        raise_assertion("Expected #{count} event(s) published#{suffix}, got #{actual}")
      end

      def assert_no_pgbus_published(routing_key: nil)
        before = pgbus_published_events(routing_key: routing_key).size
        yield
        after = pgbus_published_events(routing_key: routing_key).size
        actual = after - before

        return if actual.zero?

        suffix = routing_key ? " matching #{routing_key.inspect}" : ""
        raise_assertion("Expected no events published#{suffix}, got #{actual}")
      end

      # Execute the block, capturing events, then dispatch all captured events
      # to their registered handlers.
      def perform_published_events
        yield
        Pgbus::Testing.store.drain!
      end

      private

      def raise_assertion(message)
        # Use Minitest::Assertion if available, otherwise a generic RuntimeError
        raise Minitest::Assertion, message if defined?(Minitest::Assertion)

        raise message
      end
    end
  end
end
