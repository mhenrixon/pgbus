# frozen_string_literal: true

require "json"

module Pgbus
  module Streams
    # Encodes Server-Sent Events frames per https://html.spec.whatwg.org/multipage/server-sent-events.html.
    #
    # Pgbus uses three frame types:
    #   - `message(id:, event:, data:)` — a real broadcast (carries an `id:` so the client
    #     can resume via `Last-Event-ID` on reconnect)
    #   - `comment(text)` — a heartbeat or sentinel that the SSE parser ignores
    #   - `retry_directive(ms)` — tells `EventSource` how long to wait before reconnecting
    #
    # All frames end with `\n\n` (the SSE event terminator). `data:` lines must not
    # contain newlines — the SSE spec uses `\n` as the field terminator, so a multi-line
    # payload would arrive as multiple events. We strip `\r` and `\n` from data and
    # comment text rather than splitting into multiple `data:` lines, because Turbo
    # Stream HTML is already flat and the simpler encoding is easier to debug.
    module Envelope
      NEWLINES = /[\r\n]+/

      RESPONSE_HEADERS = "HTTP/1.1 200 OK\r\n" \
                         "content-type: text/event-stream\r\n" \
                         "cache-control: no-cache, no-transform\r\n" \
                         "x-accel-buffering: no\r\n" \
                         "connection: keep-alive\r\n" \
                         "\r\n"

      def self.message(id:, event:, data:)
        raise ArgumentError, "id is required" if id.nil?
        raise ArgumentError, "event is required" if event.nil? || event.to_s.empty?

        "id: #{id}\nevent: #{event}\ndata: #{strip_newlines(data.to_s)}\n\n"
      end

      def self.comment(text)
        ": #{strip_newlines(text.to_s)}\n\n"
      end

      # Emits a `pgbus:connected` frame carrying the server-minted
      # connection id as JSON. Sent once, right after the open handshake,
      # so the page can read its own connection id and send it back as
      # `X-Pgbus-Connection` on action requests (actor-echo suppression,
      # issue #165). Deliberately omits an `id:` line: this is connection
      # metadata, not a broadcast, and giving it a cursor id would corrupt
      # the client's Last-Event-ID replay position on reconnect.
      def self.connected(id:)
        raise ArgumentError, "id is required" if id.nil? || id.to_s.empty?

        "event: pgbus:connected\ndata: #{JSON.generate({ connectionId: id.to_s })}\n\n"
      end

      def self.retry_directive(milliseconds)
        unless milliseconds.is_a?(Integer) && !milliseconds.negative?
          raise ArgumentError, "retry must be a non-negative integer (got #{milliseconds.inspect})"
        end

        "retry: #{milliseconds}\n\n"
      end

      def self.http_response_headers
        RESPONSE_HEADERS
      end

      def self.strip_newlines(str)
        str.gsub(NEWLINES, "")
      end

      private_class_method :strip_newlines
    end
  end
end
