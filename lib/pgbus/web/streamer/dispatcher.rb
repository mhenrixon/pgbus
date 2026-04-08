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

        # An unwrapped stream broadcast. Similar shape to
        # Pgbus::Client::ReadAfter::Envelope (msg_id + payload) so
        # Connection#enqueue can consume either type via duck typing,
        # but adds the `visible_to` label carried through from
        # Pgbus::Streams::Stream#broadcast. The Dispatcher uses
        # visible_to to decide per-connection delivery; Connection
        # never sees the field.
        StreamEnvelope = Data.define(:msg_id, :enqueued_at, :payload, :source, :visible_to)

        DEFAULT_READ_LIMIT = 500

        def initialize(client:, registry:, listener:, dispatch_queue:,
                       logger: Pgbus.logger, read_limit: DEFAULT_READ_LIMIT,
                       filters: nil)
          @client = client
          @registry = registry
          @listener = listener
          @queue = dispatch_queue
          @logger = logger
          @read_limit = read_limit
          # Filters default to the process-wide registry so production
          # code picks up whatever was registered at boot. Tests inject
          # a fresh Filters instance to avoid cross-test pollution.
          @filters = filters || Pgbus::Streams.filters
          # stream_name → Array<[connection, Array<Envelope>]>
          @in_flight = Hash.new { |h, k| h[k] = [] }
          # PGMQ full table name (pgbus_<prefix>_<name>) → logical stream
          # name. Populated on connect so handle_wake can translate
          # Listener::WakeMessage#queue_name (a full table name, because
          # that's what PG NOTIFY channels carry) into the logical name
          # used by Registry and the in-flight buffer.
          @full_to_logical = {}
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
          @thread&.join(5)
          @thread = nil
          self
        end

        private

        def run_loop
          while @running
            msg = @queue.pop
            break if msg == :__stop__

            # Wake coalescing: if a WakeMessage arrives, opportunistically
            # drain consecutive same-stream wakes from the queue. Without
            # this, N broadcasts in rapid succession produce N
            # WakeMessages, each running its own read_after roundtrip
            # even though one read_after with the lowest cursor would
            # have pulled all N messages. The drain is bounded by the
            # queue's current contents — once we hit a non-Wake or a
            # different stream, we stop and let the regular path handle
            # the rest.
            if msg.is_a?(WakeMessage)
              wakes, trailing = drain_wakes_for(msg)
              wakes.each { |w| handle(w) }
              handle(trailing) if trailing
            else
              handle(msg)
            end
          end
        rescue StandardError => e
          @logger.error { "[Pgbus::Streamer::Dispatcher] crashed: #{e.class}: #{e.message}" }
          raise
        end

        # Coalesces consecutive WakeMessages from the queue into one
        # per unique stream. Returns [coalesced_wakes, trailing_msg]
        # where trailing_msg is the first non-WakeMessage we hit (or
        # nil if the queue is empty after the wakes). The caller
        # processes the wakes first, then the trailing message — same
        # order as the original queue, but with redundant wakes folded.
        def drain_wakes_for(first)
          seen = Set.new([first.queue_name])
          coalesced = [first]
          loop do
            begin
              peek = @queue.pop(true)
            rescue ThreadError
              return [coalesced, nil] # queue drained
            end

            return [coalesced, peek] unless peek.is_a?(WakeMessage)

            next if seen.include?(peek.queue_name)

            seen.add(peek.queue_name)
            coalesced << peek
          end
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

          # Each connection gets a per-connection filtered subset. We
          # can't pre-filter once because different connections have
          # different authorize contexts.
          registered.each do |conn|
            safe_enqueue(conn, visible_envelopes_for(envelopes, conn))
          end
          in_flight_pairs.each do |(conn, buffer)|
            buffer.concat(visible_envelopes_for(envelopes, conn))
          end

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
          safe_enqueue(connection, visible_envelopes_for(initial, connection))

          # Step 4: drain the in-flight buffer (anything published between
          # step 2 and now). Connection#enqueue dedupes by cursor, so
          # overlap with step 3 is safe. The buffer entries were already
          # filtered when enqueued by handle_wake, so no re-filter here.
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

          visible_to = parsed["visible_to"]
          visible_to = visible_to.to_sym if visible_to.is_a?(String)

          StreamEnvelope.new(
            msg_id: envelope.msg_id,
            enqueued_at: envelope.enqueued_at,
            payload: html,
            source: envelope.source,
            visible_to: visible_to
          )
        rescue JSON::ParserError
          envelope
        end

        # Filters a list of envelopes against a specific connection's
        # context. Envelopes without a visible_to label pass through
        # unchanged; envelopes with a label are evaluated via the
        # Filters registry. Envelopes that predate the StreamEnvelope
        # refactor (plain ReadAfter::Envelope with no visible_to) also
        # pass through.
        def visible_envelopes_for(envelopes, connection)
          envelopes.select do |envelope|
            label = envelope.respond_to?(:visible_to) ? envelope.visible_to : nil
            @filters.visible?(label, connection.context)
          end
        end
      end
    end
  end
end
