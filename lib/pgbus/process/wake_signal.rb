# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    # Wake signal inspired by LavinMQ's BoolChannel pattern.
    # Replaces polling-based coordination with instant state-change signaling.
    #
    # IMPORTANT: Single-waiter only. The +wait+ method calls @event.reset
    # immediately after waking, which means concurrent waiters may miss
    # notifications. Callers must ensure only one thread calls +wait+ at
    # a time. In pgbus this is guaranteed because each Worker has exactly
    # one main loop thread that calls +wait+, while +notify!+ can be
    # called from any thread (signal handlers, lifecycle transitions).
    #
    # Usage:
    #   signal = WakeSignal.new
    #   # In worker thread (single waiter):
    #   signal.wait(timeout: 5)  # blocks until signaled or timeout
    #   # In another thread (e.g. signal handler, lifecycle transition):
    #   signal.notify!           # wakes the waiting thread
    class WakeSignal
      def initialize
        @event = Concurrent::Event.new
      end

      # Block until +notify!+ is called or timeout expires.
      # Returns true if signaled, false if timed out.
      # Resets the event after waking — only safe with a single waiter.
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
