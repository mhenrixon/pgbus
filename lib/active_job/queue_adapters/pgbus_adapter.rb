# frozen_string_literal: true

require "pgbus" unless defined?(Pgbus)

module ActiveJob
  module QueueAdapters
    # Adapter for Rails ActiveJob integration with Pgbus.
    #
    # This class lives in the ActiveJob::QueueAdapters namespace so that
    # Rails can find it via const_get("PgbusAdapter") — the standard
    # lookup mechanism used across all Rails versions (7.1+).
    #
    # Usage:
    #   config.active_job.queue_adapter = :pgbus
    class PgbusAdapter
      delegate :enqueue, :enqueue_at, :enqueue_all, to: :adapter

      def enqueue_after_transaction_commit?
        true
      end

      # Called by ActiveJob::Continuation (Rails 8.1+) at each checkpoint.
      # When true, continuable jobs save their cursor and re-enqueue
      # themselves so the worker can shut down gracefully.
      def stopping?
        Pgbus.stopping
      end

      private

      def adapter
        @adapter ||= Pgbus::ActiveJob::Adapter.new
      end
    end
  end
end
