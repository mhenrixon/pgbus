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
      # Events are removed one at a time after successful dispatch so that
      # a handler exception leaves unprocessed events in the store.
      def drain!
        loop do
          event = @mutex.synchronize { @events.first }
          break unless event

          Pgbus::EventBus::Registry.instance.handlers_for(event.routing_key).each do |subscriber|
            subscriber.handler_class.new.handle(event)
          end

          @mutex.synchronize { @events.shift }
        end
      end
    end

    # Eagerly initialize the store so concurrent access never races on creation.
    @store = EventStore.new

    class << self
      def mode!(mode, &block)
        raise ArgumentError, "Unknown mode: #{mode}. Valid modes: #{MODES.join(", ")}" unless MODES.include?(mode)

        sync_streams_test_mode!(mode)

        unless block
          Thread.main[MODE_KEY] = mode
          return
        end

        old = Thread.current[MODE_KEY]
        Thread.current[MODE_KEY] = mode
        yield
      ensure
        if block
          Thread.current[MODE_KEY] = old
          sync_streams_test_mode!(old || :disabled)
        end
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

      attr_reader :store

      private

      # When entering fake/inline mode, enable streams_test_mode to prevent
      # rack.hijack from spawning background threads that acquire DB
      # connections outside the test transaction (see issue #133). When
      # returning to :disabled mode, restore the previous setting.
      def sync_streams_test_mode!(mode)
        return unless defined?(Pgbus.configuration)

        if mode == :disabled
          Pgbus.configuration.streams_test_mode = false
          Pgbus::Web::Streamer.reset! if defined?(Pgbus::Web::Streamer)
        else
          Pgbus.configuration.streams_test_mode = true
        end
      end
    end
  end
end

require_relative "testing/assertions"
