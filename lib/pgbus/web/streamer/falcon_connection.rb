# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Connection adapter for Falcon's native streaming body path.
      # Wraps a Protocol::HTTP::Body::Writable instead of a raw IO socket.
      #
      # Satisfies the same duck-type interface as Connection so the
      # Dispatcher, Heartbeat, and Instance can use either type
      # interchangeably. Key difference: no IoWriter — writes go
      # directly to body.write which is backed by Thread::Queue and
      # is fiber-safe under Falcon's scheduler.
      class FalconConnection
        attr_reader :id, :stream_name, :io, :mutex, :last_msg_id_sent, :context

        def initialize(id:, stream_name:, body:, since_id:, write_deadline_ms:, context: nil)
          @id = id
          @stream_name = stream_name
          @body = body
          @io = body
          @last_msg_id_sent = since_id.to_i
          @write_deadline_ms = write_deadline_ms
          @mutex = Mutex.new
          @dead = false
          @closed = false
          @created_at = monotonic
          @last_write_at = @created_at
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

            result = write_to_body(bytes)
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
          result = write_to_body(bytes)
          if result == :ok
            @last_write_at = monotonic
          else
            mark_dead!
          end
          result
        end

        def write_sentinel(bytes)
          write_to_body(bytes)
        end

        def close
          @mutex.synchronize do
            return if @closed

            @closed = true
            mark_dead!
            @body.close_write
          end
        rescue StandardError => e
          Pgbus.logger&.debug { "[Pgbus::Streamer::FalconConnection] close failed: #{e.class}: #{e.message}" }
        end

        def idle_for
          monotonic - @last_write_at
        end

        def dead?
          @dead || @body.closed?
        end

        def mark_dead!
          @dead = true
        end

        private

        def write_to_body(bytes)
          @mutex.synchronize do
            return :closed if @dead || @body.closed?

            @body.write(bytes)
            :ok
          end
        rescue Protocol::HTTP::Body::Writable::Closed
          :closed
        end

        def monotonic
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        end
      end
    end
  end
end
