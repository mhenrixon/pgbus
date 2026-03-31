# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Consumer
      include SignalHandler

      attr_reader :topics, :threads, :config

      def initialize(topics:, threads: 3, config: Pgbus.configuration)
        @topics = Array(topics)
        @threads = threads
        @config = config
        @shutting_down = false
        @pool = Concurrent::FixedThreadPool.new(threads)
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
        idle = @pool.max_length - @pool.queue_length
        return interruptible_sleep(config.polling_interval) if idle <= 0

        messages = @queue_names.flat_map do |queue_name|
          Pgbus.client.read_batch(queue_name, qty: idle) || []
        end.first(idle)

        if messages.empty?
          interruptible_sleep(config.polling_interval)
          return
        end

        messages.each do |message|
          @pool.post { handle_message(message) }
        end
      end

      def handle_message(message)
        raw = JSON.parse(message.message)
        routing_key = raw.dig("headers", "routing_key") || raw["routing_key"]

        handlers = @registry.handlers_for(routing_key || "")
        handlers.each do |subscriber|
          handler = subscriber.handler_class.new
          handler.process(message)
        end

        queue_name = message.respond_to?(:queue_name) ? message.queue_name : @queue_names.first
        Pgbus.client.archive_message(queue_name, message.msg_id.to_i)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Consumer error: #{e.class}: #{e.message}" }
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
