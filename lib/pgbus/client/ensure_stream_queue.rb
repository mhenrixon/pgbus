# frozen_string_literal: true

module Pgbus
  class Client
    # Idempotent stream-queue setup. Creates the PGMQ queue (delegating to
    # `ensure_queue` which already handles schema bootstrap and dedup) and
    # then adds an `msg_id` index on the archive table that PGMQ does not
    # ship with.
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
        ensure_queue(stream_name)
        full_name = config.queue_name(stream_name)

        # PGMQ's default NOTIFY throttle is 250ms — meant to coalesce
        # high-frequency worker queue inserts. Streams are latency-
        # sensitive and need every broadcast to fire a NOTIFY, even
        # when several are batched within a single millisecond.
        # Override the throttle to 0 specifically for stream queues.
        synchronized { @pgmq.enable_notify_insert(full_name, throttle_interval_ms: 0) } if config.listen_notify

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
      end
    end
  end
end
