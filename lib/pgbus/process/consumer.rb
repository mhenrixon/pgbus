# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Consumer
      include SignalHandler

      attr_reader :topics, :threads, :config, :execution_mode

      def initialize(topics:, threads: 3, config: Pgbus.configuration, execution_mode: :threads)
        @topics = Array(topics)
        @threads = threads
        @config = config
        @execution_mode = ExecutionPools.normalize_mode(execution_mode)
        @shutting_down = false
        @pool = ExecutionPools.build(mode: @execution_mode, capacity: threads)
        @registry = EventBus::Registry.instance
      end

      def run
        setup_signals
        start_heartbeat
        setup_subscriptions
        Pgbus.logger.info { "[Pgbus] Consumer started: topics=#{topics.join(",")} threads=#{threads}" }

        loop do
          break if @shutting_down

          process_signals
          consume
        end

        shutdown
      end

      def graceful_shutdown
        @shutting_down = true
      end

      def immediate_shutdown
        @shutting_down = true
        @pool.kill
      end

      private

      def setup_subscriptions
        matching = @registry.subscribers.select do |s|
          topics.any? { |t| pattern_overlaps?(t, s.pattern) }
        end
        @queue_names = matching.map(&:queue_name).uniq
      end

      def consume
        idle = @pool.available_capacity
        return interruptible_sleep(config.polling_interval) if idle <= 0

        tagged_messages = if @queue_names.size == 1
                            queue = @queue_names.first
                            (Pgbus.client.read_batch(queue, qty: idle) || []).map { |m| [queue, m] }
                          else
                            fetch_multi_consumer(idle)
                          end

        if tagged_messages.empty?
          interruptible_sleep(config.polling_interval)
          return
        end

        tagged_messages.each do |queue_name, message|
          @pool.post { handle_message(message, queue_name) }
        end
      end

      def handle_message(message, queue_name)
        if message.read_ct.to_i > config.max_retries
          Pgbus.logger.warn { "[Pgbus] Consumer moving message #{message.msg_id} to DLQ after #{message.read_ct} reads" }
          Pgbus.client.move_to_dead_letter(queue_name, message)
          return
        end

        raw = JSON.parse(message.message)
        routing_key = raw.dig("headers", "routing_key") || raw["routing_key"]

        handlers = @registry.handlers_for(routing_key || "")
        handlers.each do |subscriber|
          handler = subscriber.handler_class.new
          handler.process(message)
        end

        Pgbus.client.archive_message(queue_name, message.msg_id.to_i)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Consumer error: #{e.class}: #{e.message}" }
        # Message stays in queue; VT will expire and it becomes available again.
        # read_ct tracks delivery attempts — when it exceeds max_retries,
        # the next read will route to DLQ above.
      end

      def fetch_multi_consumer(qty)
        messages = Pgbus.client.read_multi(@queue_names, qty: qty) || []
        prefix = "#{config.queue_prefix}_"

        messages.map do |m|
          logical = m.queue_name&.delete_prefix(prefix) || @queue_names.first
          [logical, m]
        end
      end

      def pattern_overlaps?(topic_filter, subscription_pattern)
        # Simple check: if either is a subset of the other
        topic_filter == subscription_pattern ||
          topic_filter.end_with?("#") ||
          subscription_pattern.start_with?(topic_filter.delete_suffix(".#"))
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "consumer",
          metadata: { topics: topics, threads: threads, pid: ::Process.pid }
        )
        @heartbeat.start
      end

      def shutdown
        @pool.shutdown
        @pool.wait_for_termination(30)
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Consumer stopped" }
      end
    end
  end
end
