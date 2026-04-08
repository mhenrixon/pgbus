# frozen_string_literal: true

require "rack/request"

module Pgbus
  module Web
    # Rack app mounted at /pgbus/streams. Not a Rails controller — this
    # bypasses the entire Rails middleware stack for the streaming path,
    # which avoids several classes of bug (ActionDispatch::Cookies leaking
    # into long-lived requests, ActiveRecord::QueryCache holding the AR
    # connection open, flash modifications after hijack, etc.). It also
    # removes the temptation to use ActionController::Live, which has a
    # well-documented tendency to tie up Puma threads (see puma#1009,
    # puma#938, puma#569).
    #
    # Call flow (happy path, Puma 6.1+ hijack):
    #   1. Parse the signed name from PATH_INFO
    #   2. Verify it via Pgbus::Streams::SignedName (raises → 404)
    #   3. Run the authorize hook (raises → 403)
    #   4. Parse the cursor from ?since= or Last-Event-ID
    #   5. Check streams_max_connections per worker (over → 503)
    #   6. Check rack.hijack? (missing → 501)
    #   7. Hijack, write the HTTP response line + SSE headers + opening
    #      comment directly to the socket
    #   8. Build a Connection, hand to Streamer.current.register(...)
    #   9. Return [-1, {}, []] (Puma's async protocol)
    #
    # On errors, returns a normal [status, headers, body] response so
    # Rack / the reverse proxy can log and retry. The whole thing is
    # designed so a reviewer can read call/2 top-to-bottom and see the
    # full request lifecycle.
    class StreamApp
      PATH_PREFIX = "/pgbus/streams"
      private_constant :PATH_PREFIX

      def initialize(streamer: nil, config: nil, logger: nil, authorize: nil)
        @streamer_override = streamer
        @config_override   = config
        @logger_override   = logger
        @authorize         = authorize || ->(_env, _stream_name) { true }
      end

      def call(env)
        request = Rack::Request.new(env)
        return not_found("only GET is supported") unless request.get?

        signed_name = extract_signed_name(env)
        return not_found("missing signed stream name") if signed_name.nil?

        stream_name = verify!(signed_name)
        return not_found("invalid signed stream name") if stream_name.nil?

        authorize_result = @authorize.call(env, stream_name)
        return forbidden unless authorize_result

        # If authorize returned a non-boolean value (e.g. a User model),
        # treat it as the connection context for audience filtering.
        # A bare `true` means "authorized but no context" — filters that
        # depend on a context will fail-closed on these connections.
        context = authorize_result unless authorize_result == true

        cursor = parse_cursor(env, request)
        return bad_request("invalid cursor: #{cursor}") if cursor.is_a?(String)

        return unsupported_server unless env["rack.hijack?"]

        return over_capacity if streamer.registry.size >= config.streams_max_connections

        hijack_and_register(env, stream_name: stream_name, since_id: cursor, context: context)
      rescue StandardError => e
        logger.error { "[Pgbus::StreamApp] #{e.class}: #{e.message}" }
        server_error
      end

      private

      def extract_signed_name(env)
        path = env["PATH_INFO"].to_s
        # PATH_INFO when mounted is relative to the mount point, so it's
        # just "/<signed_name>". When not mounted (e.g. tests calling call
        # directly) it's "/pgbus/streams/<signed_name>". Handle both.
        path = path.sub(PATH_PREFIX, "")
        path = path[1..] if path.start_with?("/") # strip leading slash
        return nil if path.nil? || path.empty?

        path
      end

      def verify!(token)
        Pgbus::Streams::SignedName.verify!(token)
      rescue Pgbus::Streams::SignedName::InvalidSignedName,
             Pgbus::Streams::SignedName::MissingSecret
        nil
      end

      def parse_cursor(env, request)
        Pgbus::Streams::Cursor.parse(
          query_since: request.params["since"],
          last_event_id: env["HTTP_LAST_EVENT_ID"]
        )
      rescue Pgbus::Streams::Cursor::InvalidCursor => e
        e.message
      end

      def hijack_and_register(env, stream_name:, since_id:, context: nil)
        # Rack's full-hijack API: calling env["rack.hijack"] returns
        # the IO on some servers (Puma) and populates env["rack.hijack_io"]
        # as a side effect on all servers. Use the side-effect variable
        # if set (more portable), else fall back to the return value.
        returned_io = env["rack.hijack"].call
        io = env["rack.hijack_io"] || returned_io

        write_headers(io, stream_name: stream_name, since_id: since_id)

        connection = Pgbus::Web::Streamer::Connection.new(
          id: SecureRandom.hex(8),
          stream_name: stream_name,
          io: io,
          since_id: since_id,
          writer: Pgbus::Web::Streamer::IoWriter,
          write_deadline_ms: config.streams_write_deadline_ms,
          context: context
        )
        streamer.register(connection)

        [-1, {}, []]
      end

      def write_headers(io, stream_name:, since_id:)
        io.write(Pgbus::Streams::Envelope.http_response_headers)
        io.write(Pgbus::Streams::Envelope.retry_directive(2_000))
        io.write(Pgbus::Streams::Envelope.comment("pgbus stream open since_id=#{since_id} stream=#{stream_name}"))
      end

      def streamer
        @streamer_override || Pgbus::Web::Streamer.current
      end

      def config
        @config_override || Pgbus.configuration
      end

      def logger
        @logger_override || Pgbus.logger
      end

      # Error responses — plain text, no body parsing, no Rails. The client
      # is EventSource which doesn't render error bodies, but we include
      # short text for operator debugging.

      def not_found(reason)
        [404, { "content-type" => "text/plain" }, ["pgbus: #{reason}"]]
      end

      def forbidden
        [403, { "content-type" => "text/plain" }, ["pgbus: forbidden"]]
      end

      def bad_request(reason)
        [400, { "content-type" => "text/plain" }, ["pgbus: #{reason}"]]
      end

      def over_capacity
        [503, { "content-type" => "text/plain", "retry-after" => "2" }, ["pgbus: streamer over capacity"]]
      end

      def unsupported_server
        [501,
         { "content-type" => "text/plain" },
         ["pgbus streams require Puma 6.1+ or Falcon — current Rack server does not provide rack.hijack"]]
      end

      def server_error
        [500, { "content-type" => "text/plain" }, ["pgbus: internal error"]]
      end
    end
  end
end
