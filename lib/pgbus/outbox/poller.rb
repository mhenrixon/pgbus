# frozen_string_literal: true

module Pgbus
  module Outbox
    class Poller
      include Process::SignalHandler

      attr_reader :config

      def initialize(config: Pgbus.configuration)
        @config = config
        @shutting_down = false
      end

      def run
        setup_signals
        start_heartbeat
        Pgbus.logger.info { "[Pgbus] Outbox poller started: interval=#{config.outbox_poll_interval}s" }

        loop do
          break if @shutting_down

          process_signals
          break if @shutting_down

          poll_and_publish
          break if @shutting_down

          interruptible_sleep(config.outbox_poll_interval)
        end

        shutdown
      end

      def graceful_shutdown
        @shutting_down = true
      end

      def immediate_shutdown
        @shutting_down = true
      end

      def poll_and_publish
        published = 0

        loop do
          entries = OutboxEntry.unpublished
                               .order(:id)
                               .limit(config.outbox_batch_size)
                               .lock("FOR UPDATE SKIP LOCKED")
                               .to_a
          break if entries.empty?

          entries.each do |entry|
            publish_entry(entry)
            published += 1
          end

          break if entries.size < config.outbox_batch_size
        end

        Pgbus.logger.debug { "[Pgbus] Outbox published #{published} entries" } if published.positive?
        published
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Outbox poll error: #{e.message}" }
        0
      end

      private

      def publish_entry(entry)
        if entry.routing_key.present?
          Pgbus.client.publish_to_topic(
            entry.routing_key,
            entry.payload,
            headers: entry.headers
          )
        else
          Pgbus.client.send_message(
            entry.queue_name,
            entry.payload,
            headers: entry.headers,
            delay: entry.delay || 0,
            priority: entry.priority
          )
        end

        entry.update!(published_at: Time.current)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Failed to publish outbox entry #{entry.id}: #{e.message}" }
      end

      def start_heartbeat
        @heartbeat = Process::Heartbeat.new(
          kind: "outbox_poller",
          metadata: { pid: ::Process.pid }
        )
        @heartbeat.start
      end

      def shutdown
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Outbox poller stopped" }
      end
    end
  end
end
