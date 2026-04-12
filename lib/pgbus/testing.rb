# frozen_string_literal: true

require "pgbus"

module Pgbus
  # Test helpers for Pgbus EventBus. Opt-in via explicit require — never
  # autoloaded by Zeitwerk so this code never leaks into production.
  #
  #   require "pgbus/testing"          # core only
  #   require "pgbus/testing/rspec"    # RSpec matchers + auto-config
  #   require "pgbus/testing/minitest" # Minitest assertions
  #
  # Three modes:
  #   :fake     — capture published events in an in-memory store (default for tests)
  #   :inline   — capture AND immediately dispatch to matching handlers
  #   :disabled — pass through to the real publisher (production behavior)
  module Testing
    MODES = %i[fake inline disabled].freeze
    MODE_KEY = :__pgbus_test_mode

    class << self
      def mode!(mode, &block)
        raise ArgumentError, "Unknown mode: #{mode}. Valid modes: #{MODES.join(", ")}" unless MODES.include?(mode)

        unless block
          Thread.main[MODE_KEY] = mode
          return
        end

        old = Thread.current[MODE_KEY]
        Thread.current[MODE_KEY] = mode
        yield
      ensure
        Thread.current[MODE_KEY] = old if block
      end

      def mode
        Thread.current[MODE_KEY] || Thread.main[MODE_KEY] || :disabled
      end

      def fake!(&) = mode!(:fake, &)
      def inline!(&) = mode!(:inline, &)
      def disabled!(&) = mode!(:disabled, &)

      def fake?     = mode == :fake
      def inline?   = mode == :inline
      def disabled? = mode == :disabled

      def store
        @store ||= EventStore.new
      end
    end

    # Thread-safe in-memory store for events captured in fake/inline mode.
    class EventStore
      def initialize
        @mutex = Mutex.new
        @events = []
      end

      def push_event(event)
        @mutex.synchronize { @events << event }
      end

      def events(routing_key: nil)
        @mutex.synchronize do
          result = @events.dup
          result = result.select { |e| e.routing_key == routing_key } if routing_key
          result
        end
      end

      def size
        @mutex.synchronize { @events.size }
      end

      def clear!
        @mutex.synchronize { @events.clear }
      end

      # Dispatch all stored events to their matching handlers, then clear.
      def drain!
        events_to_drain = @mutex.synchronize { @events.dup.tap { @events.clear } }

        events_to_drain.each do |event|
          Pgbus::EventBus::Registry.instance.handlers_for(event.routing_key).each do |subscriber|
            subscriber.handler_class.new.handle(event)
          end
        end
      end
    end
  end
end

require_relative "testing/assertions"
