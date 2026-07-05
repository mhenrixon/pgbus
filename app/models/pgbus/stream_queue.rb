# frozen_string_literal: true

module Pgbus
  # Registry of physical PGMQ queue names that back streams (as opposed to
  # job queues). Stream and job queues share the same `#{queue_prefix}_`
  # namespace (see Configuration#queue_name), so there is no reliable way to
  # tell them apart by name alone. `ensure_stream_queue` records each stream
  # queue here on first broadcast/subscribe; consumers that iterate
  # `pgmq.meta` (the dispatcher's stream-archive prune and orphan sweep,
  # `compact_archives`, and the worker's wildcard resolution) use this
  # registry to classify a queue as a stream.
  #
  # Degrades safely: on an install that has not run the migration, the table
  # is absent — `record!` no-ops and `all_names` returns an empty set, so the
  # pre-registry behavior (streams treated as job queues by maintenance) is
  # preserved rather than raising.
  class StreamQueue < BusRecord
    self.table_name = "pgbus_stream_queues"

    class << self
      # Upserts the physical queue name. Idempotent and cheap to call on
      # every broadcast; the caller (`ensure_stream_queue`) also memoizes
      # per-process, so the DB write happens once per stream per process.
      # Errors are swallowed — a registry hiccup must never abort a broadcast.
      def record!(queue_name)
        return unless table_exists?

        upsert({ queue_name: queue_name }, unique_by: :queue_name)
        # Keep the in-process cache consistent with the write so a subsequent
        # stream? check reflects this registration without a re-query.
        (@all_names ||= Set.new).add(queue_name)
        nil
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Failed to record stream queue #{queue_name}: #{e.message}" }
      end

      # Set of all registered physical stream queue names. Memoized so a
      # dispatcher maintenance pass or a wildcard re-resolve does one query,
      # not one per queue. Callers that need freshness (a long-lived process
      # picking up newly-created streams) call `reset_cache!` at the top of
      # their loop.
      def all_names
        @all_names ||= load_names
      end

      def stream?(queue_name)
        all_names.include?(queue_name)
      end

      # Drops the memoized set so the next `all_names`/`stream?` re-queries.
      # Called at the start of each dispatcher maintenance pass and each
      # wildcard resolution so freshly-created streams are picked up without
      # a process restart.
      def reset_cache!
        @all_names = nil
      end

      # Memoized like StreamStat: a successful probe sticks; a transient
      # error returns false without caching, so the next call retries.
      def table_exists?
        return @table_exists if defined?(@table_exists) && @table_exists

        @table_exists = connection.table_exists?(table_name)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Failed to check stream queue table: #{e.message}" }
        false
      end

      private

      def load_names
        return Set.new unless table_exists?

        Set.new(all.pluck(:queue_name))
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Failed to load stream queues: #{e.message}" }
        Set.new
      end
    end
  end
end
