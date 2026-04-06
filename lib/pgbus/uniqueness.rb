# frozen_string_literal: true

require "active_support/concern"

module Pgbus
  # Job uniqueness guarantees: prevent duplicate jobs from running concurrently.
  #
  # Unlike concurrency limits (which allow N concurrent jobs for the same key),
  # uniqueness ensures AT MOST ONE job with a given key exists in the system
  # at any time — from enqueue through completion.
  #
  # Lock lifecycle (advisory lock + thin lookup table):
  #   1. Enqueue: pg_advisory_xact_lock serializes concurrent attempts,
  #      then INSERT INTO pgbus_uniqueness_keys ON CONFLICT DO NOTHING.
  #      The lock row lives as long as the job is in the queue or executing.
  #   2. Execution: PGMQ's visibility timeout is the execution lock —
  #      no separate claim_for_execution step needed.
  #   3. Completion/DLQ: DELETE FROM pgbus_uniqueness_keys WHERE lock_key = ?.
  #   4. Crash recovery: if a worker dies, VT expires, the message becomes
  #      readable again. The uniqueness key row stays (correctly — the job
  #      hasn't finished). The next worker picks it up and executes.
  #
  # Strategies:
  #   :until_executed  — Lock acquired at enqueue, held through execution, released on
  #                      completion or DLQ. Prevents duplicate enqueue AND duplicate execution.
  #
  #   :while_executing — Lock acquired at execution start, released on completion.
  #                      Allows duplicate enqueue (multiple copies in queue) but only one
  #                      executes at a time.
  #
  # Usage:
  #   class ImportOrderJob < ApplicationJob
  #     ensures_uniqueness strategy: :until_executed,
  #                        key: ->(order_id) { "import-order-#{order_id}" },
  #                        on_conflict: :reject
  #
  #     def perform(order_id)
  #       # Only one instance of this job per order_id can exist at a time
  #     end
  #   end
  module Uniqueness
    extend ActiveSupport::Concern

    METADATA_KEY = "pgbus_uniqueness_key"
    STRATEGY_KEY = "pgbus_uniqueness_strategy"
    TTL_KEY = "pgbus_uniqueness_lock_ttl"

    # TTL is kept for metadata compatibility but no longer drives lock expiry.
    # The lock exists until the job completes or is dead-lettered.
    DEFAULT_LOCK_TTL = 24 * 60 * 60

    VALID_STRATEGIES = %i[until_executed while_executing].freeze
    VALID_CONFLICTS = %i[reject discard log].freeze

    class_methods do
      def ensures_uniqueness(strategy: :until_executed, key: nil, lock_ttl: DEFAULT_LOCK_TTL, on_conflict: :reject)
        raise ArgumentError, "strategy must be one of: #{VALID_STRATEGIES.join(", ")}" unless VALID_STRATEGIES.include?(strategy)
        raise ArgumentError, "on_conflict must be one of: #{VALID_CONFLICTS.join(", ")}" unless VALID_CONFLICTS.include?(on_conflict)

        valid_ttl = lock_ttl.is_a?(Numeric) || (defined?(ActiveSupport::Duration) && lock_ttl.is_a?(ActiveSupport::Duration))
        raise ArgumentError, "lock_ttl must be a positive number or Duration" unless valid_ttl && lock_ttl.positive?
        raise ArgumentError, "key must be callable (Proc or lambda)" if key && !key.respond_to?(:call)

        @pgbus_uniqueness = {
          strategy: strategy,
          key: key || ->(*) { name },
          lock_ttl: lock_ttl,
          on_conflict: on_conflict
        }.freeze
      end

      def pgbus_uniqueness
        @pgbus_uniqueness
      end
    end

    class << self
      def resolve_key(active_job)
        config = uniqueness_config(active_job)
        return nil unless config

        args = active_job.arguments
        last = args.last
        key = if last.is_a?(Hash) && last.each_key.all?(Symbol)
                config[:key].call(*args[...-1], **last)
              else
                config[:key].call(*args)
              end

        # Automatically serialize GlobalID-compatible objects (e.g. ActiveRecord models)
        # so users can pass model instances directly without manual .to_global_id.to_s
        key = key.to_global_id.to_s if key.respond_to?(:to_global_id)
        key
      end

      def inject_metadata(active_job, payload_hash)
        config = uniqueness_config(active_job)
        return payload_hash unless config

        key = resolve_key(active_job)
        return payload_hash unless key

        payload_hash.merge(
          METADATA_KEY => key,
          STRATEGY_KEY => config[:strategy].to_s,
          TTL_KEY => config[:lock_ttl]
        )
      end

      def extract_key(payload)
        payload&.dig(METADATA_KEY)
      end

      def extract_strategy(payload)
        payload&.dig(STRATEGY_KEY)&.to_sym
      end

      def uniqueness_config(active_job)
        return nil unless active_job.class.respond_to?(:pgbus_uniqueness)

        active_job.class.pgbus_uniqueness
      end

      # Acquire the uniqueness lock at enqueue time (:until_executed only).
      # Uses pg_advisory_xact_lock to serialize concurrent attempts.
      # Returns :acquired, :locked, or :no_lock.
      def acquire_enqueue_lock(key, active_job, queue_name: nil, msg_id: nil)
        config = uniqueness_config(active_job)
        return :no_lock unless config
        return :no_lock unless config[:strategy] == :until_executed

        acquired = if msg_id && queue_name
                     UniquenessKey.acquire!(key, queue_name: queue_name, msg_id: msg_id)
                   else
                     # Pre-produce check: use advisory lock + ON CONFLICT
                     UniquenessKey.acquire!(key, queue_name: queue_name || "pending", msg_id: msg_id || 0)
                   end
        acquired ? :acquired : :locked
      end

      # Acquire the uniqueness lock at execution time (:while_executing only).
      # Returns true if acquired, false if another instance is running.
      def acquire_execution_lock(key, payload)
        strategy = extract_strategy(payload)
        return true unless strategy == :while_executing

        queue_name = payload["queue_name"] || "unknown"
        UniquenessKey.acquire!(key, queue_name: queue_name, msg_id: 0)
      end

      # Release the uniqueness lock after execution completes.
      def release_lock(key)
        return unless key

        UniquenessKey.release!(key)
      end
    end
  end
end
