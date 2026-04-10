# frozen_string_literal: true

require "concurrent"

module Pgbus
  module ExecutionPools
    class ThreadPool
      attr_reader :capacity

      def initialize(capacity:, on_state_change: nil)
        @capacity = capacity
        @on_state_change = on_state_change
        @available_capacity = Concurrent::AtomicFixnum.new(capacity)
        @pool = Concurrent::FixedThreadPool.new(capacity)
      end

      def post(&block)
        @available_capacity.decrement
        begin
          @pool.post do
            block.call
          ensure
            @available_capacity.increment
            @on_state_change&.call
          end
        rescue StandardError
          @available_capacity.increment
          raise
        end
      end

      def available_capacity
        @available_capacity.value
      end

      def idle?
        available_capacity.positive?
      end

      def shutdown
        @pool.shutdown
      end

      def wait_for_termination(timeout)
        @pool.wait_for_termination(timeout)
      end

      def kill
        @pool.kill
      end

      def metadata
        {
          mode: :threads,
          capacity: @capacity,
          busy: @capacity - available_capacity
        }
      end
    end
  end
end
