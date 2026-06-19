# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Consumer
      include SignalHandler

      # Fallback poll ceiling when NOTIFY-gated wakeups are active (see
      # Worker::NOTIFY_FALLBACK_POLL_SECONDS).
      NOTIFY_FALLBACK_POLL_SECONDS = 15

      attr_reader :topics, :threads, :config, :execution_mode

      def initialize(topics:, threads: 3, config: Pgbus.configuration, execution_mode: :threads)
        @topics = Array(topics)
        @threads = threads
        @config = config
        @execution_mode = ExecutionPools.normalize_mode(execution_mode)
        @shutting_down = false
        @pool = ExecutionPools.build(mode: @execution_mode, capacity: threads)
        @registry = EventBus::Registry.instance
        @notify_listener = nil
      end

      def run
        setup_signals
        start_heartbeat
        setup_subscriptions
        start_notify_listener
        Pgbus.logger.info do
          "[Pgbus] Consumer started: topics=#{topics.join(",")} threads=#{threads} " \
            "notify_wakeup=#{notify_wakeup?}"
        end

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
        return interruptible_sleep(consume_wake_timeout) if idle <= 0

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

      # `qty` is the total pool capacity. pgmq-ruby treats `qty:` as per-queue,
      # so we also pass `limit: qty` to cap the total across all queues —
      # otherwise we get `queue_count * qty` messages and overflow the
      # execution pool, crashing the consumer fork (issue #123).
      def fetch_multi_consumer(qty)
        messages = Pgbus.client.read_multi(@queue_names, qty: qty, limit: qty) || []
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

      def notify_wakeup?
        config.respond_to?(:worker_notify_wakeup) && config.worker_notify_wakeup
      end

      # See Worker#wake_timeout. With the listener active, the consumer's poll
      # sleep becomes a safety-net ceiling (a missed NOTIFY costs bounded extra
      # latency); the listener wakes it via wake! on a real insert.
      def consume_wake_timeout
        return config.polling_interval unless notify_wakeup? && @notify_listener

        [config.polling_interval, NOTIFY_FALLBACK_POLL_SECONDS].max
      end

      def start_notify_listener
        return unless notify_wakeup?
        return if Array(@queue_names).empty?

        @notify_listener = NotifyListener.new(
          physical_queues: @queue_names,
          on_wake: -> { wake! },
          connection_options: config.worker_notify_connection_options,
          health_check_ms: (config.polling_interval * 1000).to_i.clamp(250, 5_000),
          logger: Pgbus.logger
        ).start
      rescue StandardError => e
        @notify_listener = nil
        Pgbus.logger.error { "[Pgbus] Consumer NotifyListener failed to start, falling back to polling: #{e.class}: #{e.message}" }
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "consumer",
          metadata: { topics: topics, threads: threads, pid: ::Process.pid }
        )
        @heartbeat.start
      end

      def shutdown
        @notify_listener&.stop
        @pool.shutdown
        @pool.wait_for_termination(30)
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Consumer stopped" }
      end
    end
  end
end
