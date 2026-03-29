# frozen_string_literal: true

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
