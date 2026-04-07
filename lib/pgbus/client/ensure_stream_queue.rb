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

        sanitized = QueueNameValidator.sanitize!(config.queue_name(stream_name))
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
