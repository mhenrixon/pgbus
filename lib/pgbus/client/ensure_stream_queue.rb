# frozen_string_literal: true

module Pgbus
  class Client
    # Idempotent stream-queue setup. Creates the PGMQ queue (delegating to
    # `ensure_queue` which already handles schema bootstrap and dedup),
    # overrides the NOTIFY throttle to 0 so every broadcast fires its
    # own NOTIFY, and adds an `msg_id` index on the archive table that
    # PGMQ does not ship with.
    #
    # PGMQ's archive tables (`pgmq.a_<name>`) only carry an `archived_at`
    # index by default. `Client#read_after`'s replay query filters by
    # `WHERE msg_id > $1`, which becomes a sequential scan once the archive
    # grows past a few thousand rows. We add the index here, scoped to
    # stream queues only, so users with chat-history-style retention don't
    # hit a performance cliff.
    #
    # Called from `Pgbus.stream(name).broadcast(...)` on first publish per
    # stream and from the streamer on first subscription per stream.
    module EnsureStreamQueue
      def ensure_stream_queue(stream_name)
        full_name = config.queue_name(stream_name)

        with_stale_connection_retry do
          ensure_queue(stream_name)

          # PGMQ's default NOTIFY throttle is 250ms — meant to coalesce
          # high-frequency worker queue inserts. Streams are latency-
          # sensitive and need every broadcast to fire a NOTIFY, even
          # when several are batched within a single millisecond.
          # Override the throttle to 0 specifically for stream queues.
          # Use the idempotent path to avoid deadlocks when multiple
          # processes race to set up the same stream queue.
          synchronized { enable_notify_if_needed(full_name, 0) }
        end

        # CREATE INDEX IF NOT EXISTS is idempotent in Postgres but still
        # requires a roundtrip and a brief ACCESS SHARE lock on the archive
        # table. Broadcast-per-after_commit loops can hit this 1000x/sec on
        # the same stream, so memoize per-process after the first success.
        return if @stream_indexes_created[stream_name]

        sanitized = QueueNameValidator.sanitize!(full_name)
        sql = <<~SQL
          CREATE INDEX IF NOT EXISTS a_#{sanitized}_msg_id_idx
          ON pgmq.a_#{sanitized} (msg_id)
        SQL

        synchronized do
          with_raw_connection do |conn|
            conn.exec(sql)
          end
        end

        @stream_indexes_created[stream_name] = true
      end
    end
  end
end
