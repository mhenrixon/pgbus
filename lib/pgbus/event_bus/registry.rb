# frozen_string_literal: true

require "singleton"

module Pgbus
  module EventBus
    class Registry
      include Singleton

      attr_reader :subscribers

      def initialize
        @subscribers = []
        @mutex = Mutex.new
      end

      def subscribe(pattern, handler_class, queue_name: nil)
        subscriber = Subscriber.new(
          pattern: pattern,
          handler_class: handler_class,
          queue_name: queue_name
        )

        @mutex.synchronize do
          @subscribers << subscriber
        end

        subscriber
      end

      def setup_all!
        @subscribers.each(&:setup!)
      end

      def handlers_for(routing_key)
        @subscribers.select { |s| matches?(s.pattern, routing_key) }
      end

      # Physical PGMQ queue names for every registered event subscriber, so a
      # wildcard (`queues: ['*']`) worker can exclude them — an event queue
      # carries event payloads, not ActiveJob jobs, and a job worker that adopts
      # one fails to deserialize and DLQ-moves the event (issue #333). Returns a
      # Set of prefixed names (`#{queue_prefix}_<subscriber>`), matching the
      # pgmq.meta rows the wildcard resolver diffs against.
      def event_queue_names
        @subscribers.to_set { |s| Pgbus.configuration.queue_name(s.queue_name) }
      end

      def clear!
        @mutex.synchronize { @subscribers.clear }
      end

      private

      def matches?(pattern, routing_key)
        regex = pattern
                .gsub(".", "\\.")
                .gsub("*", "[^.]+")
                .gsub("#", ".*")
        routing_key.match?(/\A#{regex}\z/)
      end
    end
  end
end
