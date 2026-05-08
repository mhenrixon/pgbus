# frozen_string_literal: true

module Pgbus
  class Client
    # Fire-and-forget PG NOTIFY for ephemeral stream broadcasts. No PGMQ
    # queue is created — the payload travels via the Postgres NOTIFY channel
    # only, matching the channel naming convention that PGMQ's trigger uses:
    #   pgmq.q_<full_queue_name>.INSERT
    #
    # Subscribers already LISTEN on this channel via the Streamer's Listener.
    # When a subscriber is connected, the StreamEventDispatcher receives the
    # NOTIFY and fans out the payload. When no subscriber is connected,
    # the NOTIFY is silently discarded by Postgres — no queue, no storage,
    # no orphan tables.
    #
    # The payload is JSON-serialized into the NOTIFY's optional payload
    # parameter (max 8000 bytes in Postgres). Broadcasts exceeding this
    # limit will raise a PG::ProgramLimitExceeded error — callers needing
    # large payloads should use durable mode (which inserts into PGMQ).
    module NotifyStream
      def notify_stream(stream_name, payload)
        full_name = config.queue_name(stream_name)
        sanitized = QueueNameValidator.sanitize!(full_name)
        channel = "pgmq.q_#{sanitized}.INSERT"
        json = payload.is_a?(String) ? payload : JSON.generate(payload)

        Instrumentation.instrument("pgbus.stream.notify", stream: stream_name, bytes: json.bytesize) do
          synchronized do
            with_raw_connection do |conn|
              conn.exec_params("SELECT pg_notify($1, $2)", [channel, json])
            end
          end
        end
      end
    end
  end
end
