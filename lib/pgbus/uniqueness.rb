# frozen_string_literal: true

require "active_support/concern"

module Pgbus
  # Job uniqueness guarantees: prevent duplicate jobs from running concurrently.
  #
  # Unlike concurrency limits (which allow N concurrent jobs for the same key),
  # uniqueness ensures AT MOST ONE job with a given key exists in the system
  # at any time — from enqueue through completion.
  #
  # Lock lifecycle:
  #   1. Enqueue: lock acquired (state: queued), no owner yet
  #   2. Execution start: lock transitions to (state: executing, owner_pid: PID)
  #   3. Completion/DLQ: lock released (row deleted)
  #   4. Crash recovery: reaper detects orphaned locks by cross-referencing
  #      owner_pid against pgbus_processes heartbeats
  #   5. Last resort: expires_at TTL (default 24h) handles the case where
  #      even the reaper can't run (entire supervisor dead)
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

    # 24 hours — last-resort fallback only. The reaper handles normal crash recovery.
    DEFAULT_LOCK_TTL = 24 * 60 * 60

    VALID_STRATEGIES = %i[until_executed while_executing].freeze
    VALID_CONFLICTS = %i[reject discard log].freeze

    class_methods do
      def ensures_uniqueness(strategy: :until_executed, key: nil, lock_ttl: DEFAULT_LOCK_TTL, on_conflict: :reject)
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

      # Acquire the uniqueness lock at enqueue time (:until_executed only).
      # Lock state: queued, no owner_pid yet.
      # Returns :acquired or :locked.
      def acquire_enqueue_lock(key, active_job)
        config = uniqueness_config(active_job)
        return :acquired unless config
        return :acquired unless config[:strategy] == :until_executed

        acquired = JobLock.acquire!(
          key,
          job_class: active_job.class.name,
          job_id: active_job.job_id,
          state: "queued",
          ttl: config[:lock_ttl]
        )
        acquired ? :acquired : :locked
      end

      # Transition a queued lock to executing state when the worker picks it up.
      # Called for :until_executed jobs at execution start.
      def claim_for_execution!(key, ttl:)
        JobLock.claim_for_execution!(key, owner_pid: ::Process.pid, ttl: ttl)
      end

      # Acquire the uniqueness lock at execution time (:while_executing only).
      # Lock state: executing with owner_pid.
      # Returns true if acquired, false if another instance is running.
      def acquire_execution_lock(key, payload)
        strategy = extract_strategy(payload)
        return true unless strategy == :while_executing

        ttl = payload[TTL_KEY] || DEFAULT_LOCK_TTL

        JobLock.acquire!(
          key,
          job_class: payload["job_class"],
          job_id: payload["job_id"],
          state: "executing",
          owner_pid: ::Process.pid,
          ttl: ttl
        )
      end

      # Release the uniqueness lock after execution completes.
      def release_lock(key)
        return unless key

        JobLock.release!(key)
      end
    end
  end
end
