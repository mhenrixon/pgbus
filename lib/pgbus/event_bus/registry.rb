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

      # Set up every registered subscriber (creates its queue + binds its topic
      # via Pgbus.client, which opens a PGMQ connection).
      #
      # safe: false (default) — set up unconditionally; a connection error
      #   propagates. Use when you know the database is up.
      # safe: true — boot/rake-safe: skip entirely in a schema/db: rake context
      #   (opening a PGMQ connection there would block DROP DATABASE and isn't
      #   wanted during schema load / asset precompile), and swallow a connection
      #   error with a warning instead of crashing boot when the DB isn't ready.
      #   This is the path host apps should call from an initializer so they stop
      #   hand-wrapping setup_all! in a multi-class rescue (issue #334).
      def setup_all!(safe: false)
        return if safe && schema_task_context?

        # Snapshot under the mutex — subscribe/clear! mutate @subscribers under
        # @mutex, and setup! does DB I/O we must NOT hold the lock across, so
        # iterate a copy taken atomically.
        subscribers = @mutex.synchronize { @subscribers.dup }

        subscribers.each do |subscriber|
          subscriber.setup!
        rescue PGMQ::Errors::ConnectionError, PG::ConnectionBad => e
          # Only a genuine CONNECTION failure ("database isn't up yet") is
          # tolerable under safe:; a PG::Error subclass like a syntax/permission/
          # missing-table error is a real setup bug and must still surface.
          raise unless safe

          Pgbus.logger.warn do
            "[Pgbus] EventBus subscriber setup skipped (#{e.class}: #{e.message}) — " \
              "the database isn't reachable yet; subscribers set up on the next attempt."
          end
        end
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

      # Rake task-name prefixes during which pgbus must NOT open a PGMQ
      # connection: creating/dropping/migrating/loading the schema (a live
      # connection blocks DROP DATABASE) and asset precompile (no DB expected).
      SCHEMA_TASK_PREFIXES = %w[db: db_test: assets: webpacker: yarn:].freeze
      private_constant :SCHEMA_TASK_PREFIXES

      # True when running inside a rake schema/asset task, where setup_all!(safe:)
      # should skip rather than open a connection. Detected from the invoked rake
      # task names; false outside a Rake run.
      def schema_task_context?
        return false unless defined?(::Rake) && ::Rake.respond_to?(:application)

        tasks = ::Rake.application.top_level_tasks
        tasks.any? { |t| SCHEMA_TASK_PREFIXES.any? { |prefix| t.to_s.start_with?(prefix) } }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] schema_task_context? detection failed, assuming non-schema: #{e.class}: #{e.message}" }
        false
      end

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
