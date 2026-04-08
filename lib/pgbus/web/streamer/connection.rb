# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Wraps a single hijacked SSE client socket with its own cursor state,
      # per-io mutex, and liveness flag. Owns no threads — the Dispatcher and
      # Heartbeat threads call #enqueue / #write_comment on Connection instances
      # directly, and the per-io mutex in IoWriter serialises concurrent writes.
      #
      # Cursor semantics: `last_msg_id_sent` is strictly monotonic. `enqueue`
      # filters envelopes with `msg_id > last_msg_id_sent` and advances the
      # cursor only for envelopes that actually wrote successfully. This is
      # the client-side leg of the replay-race fix (§6.5 of the design doc).
      class Connection
        attr_reader :id, :stream_name, :io, :mutex, :last_msg_id_sent, :context

        def initialize(id:, stream_name:, io:, since_id:, writer:, write_deadline_ms:, context: nil)
          @id = id
          @stream_name = stream_name
          @io = io
          @last_msg_id_sent = since_id.to_i
          @writer = writer
          @write_deadline_ms = write_deadline_ms
          @mutex = Mutex.new
          @dead = false
          @closed = false
          @created_at = monotonic
          @last_write_at = @created_at
          # Context is whatever the StreamApp's authorize hook returned
          # (a truthy non-boolean value). Typically a user model or a
          # session hash. The Dispatcher passes it to the Filters
          # registry when evaluating visible_to predicates. Defaults to
          # nil for tests that don't need audience filtering.
          @context = context
        end

        def enqueue(envelopes)
          written = []
          envelopes.each do |envelope|
            next if envelope.msg_id <= @last_msg_id_sent

            bytes = Pgbus::Streams::Envelope.message(
              id: envelope.msg_id,
              event: "turbo-stream",
              data: envelope.payload
            )

            result = @writer.write(self, bytes, deadline_ms: @write_deadline_ms)
            if result == :ok
              @last_msg_id_sent = envelope.msg_id
              @last_write_at = monotonic
              written << envelope
            else
              mark_dead!
              break
            end
          end
          written
        end

        def write_comment(text)
          bytes = Pgbus::Streams::Envelope.comment(text)
          result = @writer.write(self, bytes, deadline_ms: @write_deadline_ms)
          if result == :ok
            @last_write_at = monotonic
          else
            mark_dead!
          end
          result
        end

        def idle_for
          monotonic - @last_write_at
        end

        def dead?
          @dead
        end

        def mark_dead!
          @dead = true
        end

        # Idempotent socket close for use by Instance#shutdown! and the
        # heartbeat idle reaper. Wraps the respond_to? / closed? dance
        # so callers don't need to know about StringIO-in-tests vs real
        # Socket-in-prod or about the mark_dead! ordering.
        #
        # Takes the same mutex as IoWriter.write so it can't fire
        # mid-write — otherwise the write loop could hit a half-closed
        # socket and corrupt the `last_msg_id_sent` cursor by marking
        # the connection dead between successful writes. The rescue
        # narrows to IO-related exceptions; unrelated errors (bugs in
        # the fake IO used by tests, nil-dereferences, etc.) should
        # still propagate so the test suite catches them.
        def close
          @mutex.synchronize do
            return if @closed

            @closed = true
            mark_dead!
            return unless @io.respond_to?(:close)

            @io.close unless @io.respond_to?(:closed?) && @io.closed?
          end
        rescue IOError, SystemCallError => e
          Pgbus.logger&.debug { "[Pgbus::Streamer::Connection] close failed: #{e.class}: #{e.message}" }
        end

        private

        def monotonic
          # Qualify ::Process because Pgbus::Process already exists as a
          # namespace for worker/supervisor/consumer and would otherwise
          # shadow the top-level constant here.
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        end
      end
    end
  end
end
