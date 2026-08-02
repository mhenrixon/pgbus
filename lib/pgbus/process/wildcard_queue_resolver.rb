# frozen_string_literal: true

module Pgbus
  module Process
    # Resolves a wildcard ("*") queue list to the concrete logical queue names
    # a job worker may adopt: every pgmq.meta queue minus dead-letter queues,
    # stream queues (issue #309/#366 — a job worker adopting one would claim
    # durable broadcasts and DLQ-move them out of replay history), and
    # event-subscriber queues (issue #333 — event payloads, not ActiveJob
    # jobs), with the configured prefix stripped.
    #
    # Shared by Worker#resolve_wildcard_queues (per-fork adoption) and the
    # supervisor-owned NotifyHub (issue #381 — the LISTEN union for wildcard
    # capsules), so both sides of the wake path agree on what "*" means.
    module WildcardQueueResolver
      module_function

      def resolve(config: Pgbus.configuration)
        prefix = "#{config.queue_prefix}_"

        # Reset first so a stream created since the last resolve is excluded.
        Pgbus::StreamQueue.reset_cache!
        stream_names = Pgbus::StreamQueue.known_names
        event_names = Pgbus::EventBus::Registry.instance.event_queue_names

        conn = config.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection
        conn.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
            .reject { |q| q.end_with?(Pgbus::DEAD_LETTER_SUFFIX) }
            .reject { |q| stream_names.include?(q) }
            .reject { |q| event_names.include?(q) }
            .map { |q| q.delete_prefix(prefix) }
      end
    end
  end
end
