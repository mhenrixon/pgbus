# frozen_string_literal: true

module Pgbus
  module ExecutionPools
    class << self
      def build(mode:, capacity:, on_state_change: nil)
        case normalize_mode(mode)
        when :threads
          ThreadPool.new(capacity: capacity, on_state_change: on_state_change)
        when :async
          AsyncPool.new(capacity: capacity, on_state_change: on_state_change)
        end
      end

      def normalize_mode(mode)
        case mode.to_s
        when "", "threads"
          :threads
        when "async", "fiber"
          :async
        else
          raise ArgumentError, "Unknown execution_mode: #{mode.inspect}. Expected one of: :threads, :async, :fiber"
        end
      end
    end
  end
end
