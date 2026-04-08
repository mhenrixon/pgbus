# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # The single-threaded consumer of the shared dispatch_queue. Drains
      # three kinds of messages:
      #
      #   - Listener::WakeMessage(queue_name:) — a NOTIFY fired; read_after
      #     the minimum cursor and fan out to every connection on the stream
      #     (both registered and in-flight connects).
      #
      #   - ConnectMessage(connection:) — a new SSE client connected. Runs
      #     the 5-step race-free replay sequence from design doc §6.5:
      #       1. ensure_listening on the stream (so future WakeMessages
      #          deliver to the in-flight buffer)
      #       2. register an in-flight buffer keyed by connection
      #       3. read_after(connection.since_id) + enqueue to connection
      #       4. drain the in-flight buffer into the connection (dedup is
      #          handled by Connection#enqueue's cursor check)
      #       5. move the connection from in-flight to the main Registry
      #
      #   - DisconnectMessage(connection:) — unregister and, if the stream
      #     now has zero subscribers, eventually unlisten (lazy GC,
      #     implemented in the Streamer sweep rather than here).
      #
      # All state ownership lives on this one thread: the registry is
      # thread-safe (Phase 2.1) but the in-flight buffers are local to
      # the Dispatcher and accessed only from this thread, so no locks.
      class Dispatcher
        WakeMessage       = Listener::WakeMessage
        ConnectMessage    = Data.define(:connection)
        DisconnectMessage = Data.define(:connection)

        DEFAULT_READ_LIMIT = 500

        def initialize(client:, registry:, listener:, dispatch_queue:, logger: Pgbus.logger, read_limit: DEFAULT_READ_LIMIT)
          @client = client
          @registry = registry
          @listener = listener
          @queue = dispatch_queue
          @logger = logger
          @read_limit = read_limit
          # stream_name → Array<[connection, Array<Envelope>]>
          @in_flight = Hash.new { |h, k| h[k] = [] }
          # PGMQ full table name (pgbus_<prefix>_<name>) → logical stream
          # name. Populated on connect so handle_wake can translate
          # Listener::WakeMessage#queue_name (a full table name, because
          # that's what PG NOTIFY channels carry) into the logical name
          # used by Registry and the in-flight buffer.
          @full_to_logical = {}
          # @running is a soft hint, not the authoritative stop signal.
          # The :__stop__ sentinel pushed onto @queue is what actually
          # terminates run_loop — even if a torn read of @running ever
          # happened (it cannot under MRI's GVL for a single-word
          # boolean assignment), the sentinel break would still fire.
          @running = false
          @thread = nil
        end

        def start
          return if @running

          @running = true
          @thread = Thread.new { run_loop }
          self
        end

        def stop
          return unless @running

          @running = false
          @queue << :__stop__
          if @thread && @thread.join(5).nil?
            # join returned nil → 5s timeout. The thread is still running
            # (probably blocked inside an unresponsive client write or a
            # slow Postgres query). We log and clear the reference rather
            # than calling Thread#kill, which leaves IO state corrupt.
            # The orphaned thread will exit on its own once the blocking
            # call returns and it sees @running == false on the next loop.
            @logger.warn { "[Pgbus::Streamer::Dispatcher] thread did not terminate within 5s" }
          end
          @thread = nil
          self
        end

        private

        def run_loop
          while @running
            msg = @queue.pop
            break if msg == :__stop__

            handle(msg)
          end
        rescue StandardError => e
          @logger.error { "[Pgbus::Streamer::Dispatcher] crashed: #{e.class}: #{e.message}" }
          raise
        end

        def handle(msg)
          case msg
          when WakeMessage       then handle_wake(msg)
          when ConnectMessage    then handle_connect(msg)
          when DisconnectMessage then handle_disconnect(msg)
          else
            @logger.warn { "[Pgbus::Streamer::Dispatcher] unknown message: #{msg.class}" }
          end
        rescue StandardError => e
          # Intentionally swallows per-message failures so one bad
          # broadcast can't kill the dispatcher thread and orphan every
          # connected client. The top-level run_loop rescue (below)
          # does re-raise — a crash *between* messages is a real bug
          # and the supervisor should see it.
          @logger.error { "[Pgbus::Streamer::Dispatcher] handling #{msg.class} raised #{e.class}: #{e.message}" }
        end

        def handle_wake(msg)
          # msg.queue_name is the PGMQ full table name (pgbus_int_pbns_xxx),
          # but connections are registered under the logical name (pbns_xxx).
          # Translate before looking up.
          stream = @full_to_logical[msg.queue_name] || msg.queue_name
          registered = @registry.connections_for(stream)
          in_flight_pairs = @in_flight[stream]
          return if registered.empty? && in_flight_pairs.empty?

          min_seen = minimum_cursor(registered, in_flight_pairs)
          raw_envelopes = @client.read_after(stream, after_id: min_seen, limit: @read_limit)
          return if raw_envelopes.empty?

          envelopes = raw_envelopes.map { |e| unwrap_stream_envelope(e) }

          registered.each { |conn| safe_enqueue(conn, envelopes) }
          in_flight_pairs.each { |(_conn, buffer)| buffer.concat(envelopes) }

          prune_dead(registered)
        end

        def handle_connect(msg)
          connection = msg.connection
          stream = connection.stream_name

          # Step 1: subscribe first. Any WakeMessage that arrives after
          # this line will see our in-flight buffer and fan out into it.
          # The Listener is told the prefixed PGMQ queue name (not the
          # logical stream name) because the NOTIFY channel includes the
          # prefix: pgmq.q_<prefixed>.INSERT. Registry and the in-flight
          # buffer use the logical name. The Dispatcher is the single
          # translator between the two naming worlds.
          full_name = notify_queue_name_for(stream)
          @full_to_logical[full_name] = stream
          @listener.ensure_listening(full_name)

          # Step 2: install the in-flight buffer BEFORE any read.
          buffer = []
          @in_flight[stream] << [connection, buffer]

          # Step 3: read the archive for anything published before this
          # connect landed, and write to the connection.
          raw_initial = @client.read_after(
            stream,
            after_id: connection.last_msg_id_sent,
            limit: @read_limit
          )
          initial = raw_initial.map { |e| unwrap_stream_envelope(e) }
          safe_enqueue(connection, initial)

          # Step 4: drain the in-flight buffer (anything published between
          # step 2 and now). Connection#enqueue dedupes by cursor, so
          # overlap with step 3 is safe.
          safe_enqueue(connection, buffer)

          # Step 5: promote to the main registry. From this point the
          # regular WakeMessage path handles the connection.
          remove_in_flight(stream, connection)
          @registry.register(connection) unless connection.dead?
        rescue StandardError => e
          remove_in_flight(stream, connection)
          connection.mark_dead!
          @logger.error { "[Pgbus::Streamer::Dispatcher] connect failed for #{connection.id}: #{e.class}: #{e.message}" }
        end

        def handle_disconnect(msg)
          connection = msg.connection
          stream = connection.stream_name
          @registry.unregister(connection)

          # If this was the last subscriber to the stream, release all
          # per-stream state so long-running processes don't leak memory
          # proportional to unique stream count (important for apps that
          # use GlobalID-keyed streams like `order_42`). Three places to
          # clean up:
          #   1. @full_to_logical (the translation map — this file)
          #   2. @in_flight[stream] (cleared by remove_in_flight already)
          #   3. Listener's @listening_to set + the PG LISTEN itself
          return unless @registry.empty?(stream) && @in_flight[stream].empty?

          full_name = @full_to_logical.key(stream)
          return unless full_name

          @full_to_logical.delete(full_name)
          @listener.remove_listening(full_name)
        end

        def minimum_cursor(registered, in_flight_pairs)
          cursors = registered.map(&:last_msg_id_sent)
          in_flight_pairs.each { |(conn, _buf)| cursors << conn.last_msg_id_sent }
          cursors.min || 0
        end

        def safe_enqueue(connection, envelopes_or_buffer)
          return if connection.dead?
          return if envelopes_or_buffer.empty?

          connection.enqueue(envelopes_or_buffer)
        end

        def prune_dead(connections)
          connections.each do |conn|
            @queue << DisconnectMessage.new(connection: conn) if conn.dead?
          end
        end

        def remove_in_flight(stream, connection)
          pairs = @in_flight[stream]
          pairs.reject! { |(conn, _buf)| conn.equal?(connection) }
          @in_flight.delete(stream) if pairs.empty?
        end

        # Translates a logical stream name (e.g. "chat") into the prefixed
        # PGMQ queue name (e.g. "pgbus_int_chat") that appears in the
        # NOTIFY channel `pgmq.q_<prefixed>.INSERT`. Mirrors the prefix
        # Pgbus::Client#send_message already applied when the broadcast
        # was published, so the Listener's LISTEN matches the NOTIFY.
        def notify_queue_name_for(stream_name)
          @client.config.queue_name(stream_name)
        end

        # Pgbus::Streams::Stream#broadcast wraps HTML payloads as
        # {"html": "..."} so PGMQ's JSONB column accepts them. Here we
        # unwrap the html field and return a new envelope whose payload
        # is the raw HTML, ready for the SSE `data:` line. If the
        # payload is not a valid JSON object with an html key (e.g. a
        # legacy broadcast that predates this subsystem), we fall back
        # to passing it through untouched — a permissive approach that
        # plays nicely with ad-hoc `Pgbus.client.send_message` calls
        # pointed at stream queues by mistake.
        def unwrap_stream_envelope(envelope)
          parsed = JSON.parse(envelope.payload.to_s)
          html = parsed.is_a?(Hash) ? parsed["html"] : nil
          return envelope unless html.is_a?(String)

          Pgbus::Client::ReadAfter::Envelope.new(
            msg_id: envelope.msg_id,
            enqueued_at: envelope.enqueued_at,
            payload: html,
            source: envelope.source
          )
        rescue JSON::ParserError
          envelope
        end
      end
    end
  end
end
