# frozen_string_literal: true

module Pgbus
  class Client
    # Non-consuming peek across PGMQ live (`q_`) and archive (`a_`) tables. Used
    # exclusively by `Pgbus::Web::Streamer` for SSE replay — workers continue to
    # use `read_batch` (claim semantics). The two read paths are disjoint.
    #
    # The cursor is the highest msg_id the client has already seen. Replay returns
    # everything strictly greater, ordered by msg_id ASC, capped by `limit`.
    module ReadAfter
      Envelope = Data.define(:msg_id, :enqueued_at, :payload, :source)

      DEFAULT_LIMIT = 500

      def read_after(stream_name, after_id:, limit: DEFAULT_LIMIT)
        sanitized = sanitized_queue(stream_name)
        sql = build_read_after_sql(sanitized)

        rows = synchronized do
          with_raw_connection do |conn|
            conn.exec_params(sql, [after_id.to_i, limit.to_i]).to_a
          end
        end

        rows.map { |row| build_envelope(row) }
      end

      def stream_current_msg_id(stream_name)
        sanitized = sanitized_queue(stream_name)
        sql = "SELECT COALESCE(MAX(msg_id), 0) AS max FROM pgmq.q_#{sanitized}"
        synchronized do
          with_raw_connection do |conn|
            conn.exec(sql).first.fetch("max").to_i
          end
        end
      end

      def stream_oldest_msg_id(stream_name)
        sanitized = sanitized_queue(stream_name)
        sql = <<~SQL
          SELECT LEAST(
            (SELECT MIN(msg_id) FROM pgmq.q_#{sanitized}),
            (SELECT MIN(msg_id) FROM pgmq.a_#{sanitized})
          ) AS least
        SQL
        synchronized do
          with_raw_connection do |conn|
            value = conn.exec(sql).first.fetch("least")
            value&.to_i
          end
        end
      end

      private

      # Builds the union of live and archive tables. The outer ORDER BY + LIMIT
      # ensures we never return more than `limit` rows total even if both
      # subqueries hit it. The 'live'/'archive' constants are how the streamer
      # tells whether a row was peeked from the queue or replayed from history;
      # the streamer doesn't currently distinguish them, but we keep the column
      # so debugging is straightforward when archive replay misbehaves.
      def build_read_after_sql(sanitized)
        <<~SQL
          (
            SELECT msg_id, enqueued_at, message, 'live'::text AS source
            FROM pgmq.q_#{sanitized}
            WHERE msg_id > $1
            ORDER BY msg_id ASC
            LIMIT $2
          )
          UNION ALL
          (
            SELECT msg_id, enqueued_at, message, 'archive'::text AS source
            FROM pgmq.a_#{sanitized}
            WHERE msg_id > $1
            ORDER BY msg_id ASC
            LIMIT $2
          )
          ORDER BY msg_id ASC
          LIMIT $2
        SQL
      end

      def sanitized_queue(stream_name)
        full = config.queue_name(stream_name)
        QueueNameValidator.sanitize!(full)
      end

      def build_envelope(row)
        Envelope.new(
          msg_id: row.fetch("msg_id").to_i,
          enqueued_at: row.fetch("enqueued_at"),
          payload: row.fetch("message"),
          source: row.fetch("source")
        )
      end
    end
  end
end
