# frozen_string_literal: true

module Pgbus
  # Dispatches queue operations based on queue type (standard vs priority).
  # Replaces conditional `priority_enabled?` checks scattered through Client
  # with a single strategy object selected at initialization.
  #
  # Inspired by LavinMQ's QueueFactory which dispatches queue creation by
  # type: standard, durable, priority, stream, delayed.
  module QueueFactory
    def self.for(config)
      if config.priority_levels && config.priority_levels > 1
        PriorityStrategy.new(config)
      else
        StandardStrategy.new(config)
      end
    end

    # Standard single-queue strategy: one PGMQ queue per logical name.
    class StandardStrategy
      def initialize(config)
        @config = config
      end

      def physical_queue_names(name)
        [@config.queue_name(name)]
      end

      def target_queue(name, _priority)
        @config.queue_name(name)
      end

      def priority?
        false
      end
    end

    # Priority sub-queue strategy: N PGMQ queues per logical name (_p0.._pN).
    class PriorityStrategy
      def initialize(config)
        @config = config
      end

      def physical_queue_names(name)
        @config.priority_queue_names(name)
      end

      def target_queue(name, priority)
        level = if priority
                  priority.clamp(0, @config.priority_levels - 1)
                else
                  @config.default_priority.clamp(0, @config.priority_levels - 1)
                end
        @config.priority_queue_name(name, level)
      end

      def priority?
        true
      end
    end
  end
end
