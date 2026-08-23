# frozen_string_literal: true

module Pgbus
  class Client
    # Fair share reads (issue #426): a weighted, work-conserving interleave
    # across fair-share keys within one queue. Replaces `read_batch` on the
    # worker when `config.fair_share` is set.
    #
    # Scheduling rule per read of `qty`: for every key that has visible
    # messages, rank that key's visible messages 1..qty oldest-visible first
    # (`vt, msg_id`); a message's virtual time is `rank / weight`; take the
    # `qty` messages with the lowest `(virtual_time, msg_id)`. Weight 3 vs 1
    # yields a 3:1 split under contention; a lone key fills the whole batch.
    # This is batch-level weighted fair queuing — proportional within each
    # batch, memoryless across batches (no deficit carry-over).
    #
    # Cost model: key enumeration is a loose index scan over
    # (key, vt, msg_id) — one probe per key with visible work; keys whose
    # messages are all invisible (in flight / delayed / in retry backoff) are
    # skipped at the index level because `vt` is in the index. Per-key
    # candidates are a bounded `LIMIT qty` index range. Roughly
    # O(K · log n + K · qty), K = keys with visible work, independent of
    # backlog depth. Within a key the order is `(vt, msg_id)` — FIFO for
    # immediate enqueues; retried/delayed messages sort by when they became
    # visible — a deliberate deviation from pgmq.read's pure msg_id order so
    # the per-key lookup never sorts a tenant's whole visible backlog.
    #
    # Candidates are selected before locking (same shape as
    # pgmq.read_grouped_rr), so under concurrent readers a batch can come back
    # short; the worker loop re-reads immediately while it has capacity.
    module FairRead
      # The read expressions are written against the alias `m`; the index
      # expression is the same shape minus the alias so the planner matches it.
      FAIR_INDEX_KEY_EXPR = "COALESCE(message->>'#{FairShare::METADATA_KEY}', '')".freeze
      FAIR_KEY_EXPR = "COALESCE(m.message->>'#{FairShare::METADATA_KEY}', '')".freeze
      FAIR_WEIGHT_EXPR = "COALESCE((m.message->>'#{FairShare::WEIGHT_KEY}')::numeric, 1)".freeze
      FAIR_INDEX_SUFFIX = "_fair_idx"

      def read_batch_fair(queue_name, qty:, vt: nil)
        full_name = fair_queue_name(queue_name)
        guarded_read { fair_read_step(full_name, qty, vt || config.visibility_timeout) }
      end

      # Idempotent, memoized per process. Uses CREATE INDEX CONCURRENTLY so
      # enabling fair share on an existing, populated queue never blocks
      # enqueues. A queue table that does not exist yet is left alone (its
      # creation path builds the index non-concurrently); any other failure is
      # logged with the remediation and NOT memoized so the next ensure retries.
      def ensure_fair_index(queue_name)
        full_name = fair_queue_name(queue_name)
        return if fair_indexes_ensured[full_name]

        with_stale_connection_retry do
          synchronized { exec_ddl(fair_index_sql(full_name, concurrently: true)) }
        end
        fair_indexes_ensured[full_name] = true
      rescue StandardError => e
        if duplicate_relation_error?(e)
          fair_indexes_ensured[full_name] = true
        elsif undefined_table_error?(e)
          Pgbus.logger.debug { "[Pgbus] Fair index deferred — queue table #{full_name} not created yet" }
        else
          Pgbus.logger.error do
            "[Pgbus] Could not create fair index #{fair_index_name(full_name)} on pgmq.q_#{full_name}: " \
              "#{e.class}: #{e.message}. A failed CONCURRENTLY build leaves an INVALID index behind — " \
              "run `DROP INDEX IF EXISTS pgmq.#{fair_index_name(full_name)}` and restart the worker to retry."
          end
        end
      end

      private

      # One instrumented, retried, serialized, timeout-bounded fair read of a
      # physical (already prefixed + sanitized) queue table. Not breaker-guarded
      # itself so callers that loop over sub-queues can wrap the loop once.
      def fair_read_step(full_name, qty, vt_seconds)
        Instrumentation.instrument("pgbus.client.read_batch_fair", queue: full_name, qty: qty) do
          with_stale_connection_retry do
            synchronized { with_read_timeout { exec_fair_read(full_name, qty, vt_seconds) } }
          end
        end
      end

      def fair_queue_name(queue_name)
        QueueNameValidator.sanitize!(config.queue_name(queue_name))
      end

      def fair_indexes_ensured
        @fair_indexes_ensured ||= Concurrent::Map.new
      end

      # Runs inside create_queue_physically (caller owns the mutex) on a
      # freshly created, empty table — plain CREATE INDEX is instant there.
      def create_fair_index_if_needed(full_name)
        return unless FairShare.enabled?(config)

        exec_ddl(fair_index_sql(full_name, concurrently: false))
        fair_indexes_ensured[full_name] = true
      rescue StandardError => e
        raise unless duplicate_relation_error?(e)

        fair_indexes_ensured[full_name] = true
      end

      def exec_ddl(sql)
        @pgmq.with_connection { |conn| conn.exec(sql) }
      end

      def exec_fair_read(full_name, qty, vt_seconds)
        rows = @pgmq.with_connection do |conn|
          conn.exec_params(fair_read_sql(full_name), [qty.to_i, vt_seconds.to_i]).to_a
        end
        rows.map { |row| PGMQ::Message.new(row) }
      end

      def fair_index_name(full_name)
        "q_#{full_name}#{FAIR_INDEX_SUFFIX}"
      end

      def fair_index_sql(full_name, concurrently:)
        "CREATE INDEX #{"CONCURRENTLY " if concurrently}IF NOT EXISTS #{fair_index_name(full_name)} " \
          "ON pgmq.q_#{full_name} ((#{FAIR_INDEX_KEY_EXPR}), vt, msg_id)"
      end

      def fair_read_sql(full_name)
        table = "pgmq.q_#{full_name}"
        <<~SQL
          WITH RECURSIVE fair_keys AS (
            (SELECT #{FAIR_KEY_EXPR} AS k
               FROM #{table} m
              WHERE m.vt <= now()
              ORDER BY 1 LIMIT 1)
            UNION ALL
            SELECT (SELECT #{FAIR_KEY_EXPR}
                      FROM #{table} m
                     WHERE #{FAIR_KEY_EXPR} > fk.k
                       AND m.vt <= now()
                     ORDER BY 1 LIMIT 1)
              FROM fair_keys fk
             WHERE fk.k IS NOT NULL
          ),
          candidates AS (
            SELECT c.msg_id, c.rn, c.w
              FROM fair_keys fk
              CROSS JOIN LATERAL (
                SELECT m.msg_id,
                       ROW_NUMBER() OVER (ORDER BY m.vt, m.msg_id) AS rn,
                       #{FAIR_WEIGHT_EXPR} AS w
                  FROM #{table} m
                 WHERE #{FAIR_KEY_EXPR} = fk.k
                   AND m.vt <= now()
                 ORDER BY m.vt, m.msg_id
                 LIMIT $1
              ) c
             WHERE fk.k IS NOT NULL
          ),
          picked AS (
            SELECT msg_id, ROW_NUMBER() OVER (ORDER BY rn / w, msg_id) AS selection_order
              FROM candidates
             ORDER BY rn / w, msg_id
             LIMIT $1
          ),
          locked AS (
            SELECT m.msg_id, p.selection_order
              FROM #{table} m
              JOIN picked p ON p.msg_id = m.msg_id
             WHERE m.vt <= now()
             FOR UPDATE OF m SKIP LOCKED
          ),
          updated AS (
            UPDATE #{table} m
               SET vt = clock_timestamp() + make_interval(secs => $2),
                   read_ct = read_ct + 1,
                   last_read_at = clock_timestamp()
              FROM locked l
             WHERE m.msg_id = l.msg_id
            RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers,
                      l.selection_order
          )
          SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
            FROM updated
           ORDER BY selection_order
        SQL
      end
    end
  end
end
