# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    # Thread-safe wake signal inspired by LavinMQ's BoolChannel pattern.
    # Replaces polling-based coordination with instant state-change signaling.
    #
    # Usage:
    #   signal = WakeSignal.new
    #   # In worker thread:
    #   signal.wait(timeout: 5)  # blocks until signaled or timeout
    #   # In another thread (e.g. LISTEN/NOTIFY callback):
    #   signal.notify!           # wakes all waiting threads
    class WakeSignal
      def initialize
        @event = Concurrent::Event.new
      end

      # Block until signaled or timeout expires.
      # Returns true if signaled, false if timed out.
      def wait(timeout: nil)
        result = @event.wait(timeout)
        @event.reset
        result
      end

      # Wake all waiting threads immediately.
      def notify!
        @event.set
      end

      # Check if a notification is pending without blocking.
      def pending?
        @event.set?
      end

      # Clear the pending notification.
      def reset!
        @event.reset
      end
    end
  end
end
