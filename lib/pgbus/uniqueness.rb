# frozen_string_literal: true

require "active_support/concern"

module Pgbus
  # Job uniqueness guarantees: prevent duplicate jobs from running concurrently.
  #
  # Unlike concurrency limits (which allow N concurrent jobs for the same key),
  # uniqueness ensures AT MOST ONE job with a given key exists in the system
  # at any time — from enqueue through completion.
  #
  # Strategies:
  #   :until_executed  — Lock acquired at enqueue, held through execution, released on
  #                      completion or DLQ. Prevents duplicate enqueue AND duplicate execution.
  #                      This is the safest option for critical jobs.
  #
  #   :while_executing — Lock acquired at execution start, released on completion.
  #                      Allows duplicate enqueue (multiple copies in queue) but only one
  #                      executes at a time. Others are re-queued with a delay.
  #
  # Usage:
  #   class ImportOrderJob < ApplicationJob
  #     include Pgbus::Uniqueness
  #
  #     ensures_uniqueness strategy: :until_executed,
  #                        key: ->(order_id) { "import-order-#{order_id}" },
  #                        lock_ttl: 30.minutes,
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

    VALID_STRATEGIES = %i[until_executed while_executing].freeze
    VALID_CONFLICTS = %i[reject discard log].freeze

    class_methods do
      def ensures_uniqueness(strategy: :until_executed, key: nil, lock_ttl: 30 * 60, on_conflict: :reject)
        raise ArgumentError, "strategy must be one of: #{VALID_STRATEGIES.join(", ")}" unless VALID_STRATEGIES.include?(strategy)
        raise ArgumentError, "on_conflict must be one of: #{VALID_CONFLICTS.join(", ")}" unless VALID_CONFLICTS.include?(on_conflict)
        raise ArgumentError, "lock_ttl must be a positive number" unless lock_ttl.is_a?(Numeric) && lock_ttl.positive?
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
        if last.is_a?(Hash) && last.each_key.all?(Symbol)
          config[:key].call(*args[...-1], **last)
        else
          config[:key].call(*args)
        end
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

      # Try to acquire the uniqueness lock at enqueue time.
      # Returns :acquired or :locked.
      def acquire_enqueue_lock(key, active_job)
        config = uniqueness_config(active_job)
        return :acquired unless config
        return :acquired unless config[:strategy] == :until_executed

        acquired = JobLock.acquire!(
          key,
          job_class: active_job.class.name,
          job_id: active_job.job_id,
          ttl: config[:lock_ttl]
        )
        acquired ? :acquired : :locked
      end

      # Try to acquire the uniqueness lock at execution time.
      # Returns true if acquired, false if another instance is running.
      def acquire_execution_lock(key, payload)
        strategy = extract_strategy(payload)
        return true unless strategy == :while_executing

        job_class = payload["job_class"]
        job_id = payload["job_id"]
        ttl = payload[TTL_KEY] || (30 * 60)

        JobLock.acquire!(key, job_class: job_class, job_id: job_id, ttl: ttl)
      end

      # Release the uniqueness lock after execution completes.
      def release_lock(key)
        return unless key

        JobLock.release!(key)
      end
    end
  end
end
