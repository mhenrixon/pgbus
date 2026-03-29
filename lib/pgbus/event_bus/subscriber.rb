# frozen_string_literal: true

module Pgbus
  module EventBus
    class Subscriber
      attr_reader :pattern, :handler_class, :queue_name

      def initialize(pattern:, handler_class:, queue_name: nil)
        @pattern = pattern
        @handler_class = handler_class
        @queue_name = queue_name || derive_queue_name
      end

      def setup!
        Pgbus.client.ensure_queue(queue_name)
        Pgbus.client.bind_topic(pattern, queue_name)
      end

      private

      def derive_queue_name
        handler_class.name
                     .gsub("::", "_")
                     .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
                     .gsub(/([a-z\d])([A-Z])/, '\1_\2')
                     .downcase
      end
    end
  end
end
