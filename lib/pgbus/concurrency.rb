# frozen_string_literal: true

require "active_support/concern"

module Pgbus
  module Concurrency
    extend ActiveSupport::Concern

    METADATA_KEY = "pgbus_concurrency_key"

    class_methods do
      # Limit concurrent execution of jobs with the same key.
      #
      #   limits_concurrency to: 1, key: ->(order) { "ProcessOrder-#{order.id}" }
      #   limits_concurrency to: 3, key: ->(user_id) { "ImportUser-#{user_id}" }, on_conflict: :discard
      #
      # Options:
      #   to:          Maximum concurrent jobs for the same key (required)
      #   key:         Proc receiving job arguments, returns a string key. Default: job class name.
      #   duration:    Safety expiry for semaphore (default: 15 minutes)
      #   on_conflict: What to do when limit is reached — :block, :discard, or :raise (default: :block)
      def limits_concurrency(to:, key: nil, duration: 15 * 60, on_conflict: :block) # rubocop:disable Naming/MethodParameterName
        raise ArgumentError, "to: must be a positive integer" unless to.is_a?(Integer) && to.positive?
        raise ArgumentError, "on_conflict must be :block, :discard, or :raise" unless %i[block discard raise].include?(on_conflict)
        raise ArgumentError, "duration must be a positive number" unless duration.is_a?(Numeric) && duration.positive?
        raise ArgumentError, "key must be callable (Proc or lambda)" if key && !key.respond_to?(:call)

        @pgbus_concurrency = {
          limit: to,
          key: key || ->(*) { name },
          duration: duration,
          on_conflict: on_conflict
        }.freeze
      end

      def pgbus_concurrency
        @pgbus_concurrency
      end
    end

    class << self
      # Resolve the concurrency key for a given job instance.
      # Returns nil if the job class has no concurrency config.
      def resolve_key(active_job)
        return nil unless active_job.class.respond_to?(:pgbus_concurrency)

        config = active_job.class.pgbus_concurrency
        return nil unless config

        args = active_job.arguments
        last = args.last
        if last.is_a?(Hash) && last.each_key.all?(Symbol)
          config[:key].call(*args[...-1], **last)
        else
          config[:key].call(*args)
        end
      end

      # Inject the resolved concurrency key into the job's serialized payload.
      def inject_metadata(active_job, payload_hash)
        key = resolve_key(active_job)
        return payload_hash unless key

        payload_hash.merge(METADATA_KEY => key)
      end

      # Extract the concurrency key from a deserialized payload.
      def extract_key(payload)
        payload[METADATA_KEY]
      end
    end
  end
end
