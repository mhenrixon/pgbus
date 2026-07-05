# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Composes the Streamer's three background threads (Listener, Dispatcher,
      # Heartbeat) with the shared Registry and dispatch_queue. One Instance
      # per Puma worker. Owns the lifecycle of all three threads and the
      # dedicated PG::Connection for LISTEN.
      #
      # Lifecycle:
      #   Instance.new(...)  — allocates wiring, does NOT start threads
      #   #start             — spawns listener/dispatcher/heartbeat in order
      #   #register(conn)    — enqueues a ConnectMessage into the dispatch queue
      #   #shutdown!         — sends shutdown sentinel to every connection and
      #                        stops all threads in reverse order
      #
      # Dependency injection: every collaborator is constructor-injected so
      # tests can swap in fakes without touching Pgbus.configuration. In
      # production the module-level Streamer.current(...) builds all of the
      # defaults from the configuration.
      class Instance
        attr_reader :registry, :listener, :dispatcher, :heartbeat, :dispatch_queue, :stream_counter, :pump

        def initialize(
          client: Pgbus.client,
          config: Pgbus.configuration,
          pg_connection: nil,
          logger: Pgbus.logger,
          registry: nil,
          dispatch_queue: nil,
          connection_factory: nil
        )
          @client = client
          @config = config
          @logger = logger
          @registry = registry || Registry.new
          @dispatch_queue = dispatch_queue || Queue.new

          @stream_counter = StreamCounter.new
          @pg_connection = pg_connection || build_pg_connection
          @listener = Listener.new(
            pg_connection: @pg_connection,
            dispatch_queue: @dispatch_queue,
            health_check_ms: @config.streams_listen_health_check_ms,
            # Opt-in dispatch-queue backpressure (issue #315 item 3). 0 =
            # unbounded (default). The queue itself stays an unbounded
            # Queue.new so the request-thread Connect push and the dispatcher's
            # own prune_dead self-post never block.
            dispatch_queue_limit: @config.streams_dispatch_queue_limit,
            logger: @logger,
            # On reconnect the Listener rebuilds its OWN connection via this
            # factory (fresh connect re-resolves DNS, converges on the promoted
            # primary after a failover) instead of resetting a possibly-dead
            # socket. Always provided — even when an initial pg_connection: is
            # injected, the reconnect path builds a fresh raw connection. A test
            # can inject its own factory to avoid touching real configuration.
            connection_factory: connection_factory || -> { build_raw_pg_connection }
          )
          # Off-thread durable fanout writer (issue #321). Built only when
          # streams_writer_threads > 0; nil means fanout writes stay inline on
          # the dispatcher thread (the default, pre-#321 behavior). The pump
          # reports write acks on @ack_queue (drained by the dispatcher) and
          # posts a DisconnectMessage via on_dead when a write fails, so the
          # dispatcher owns all cursor + registry cleanup on its own thread.
          @ack_queue = Queue.new
          @pump = build_pump
          @dispatcher = StreamEventDispatcher.new(
            client: @client,
            registry: @registry,
            listener: @listener,
            dispatch_queue: @dispatch_queue,
            logger: @logger,
            config: @config,
            stream_counter: @stream_counter,
            pump: @pump,
            ack_queue: @ack_queue
          )
          @heartbeat = Heartbeat.new(
            registry: @registry,
            dispatch_queue: @dispatch_queue,
            interval: @config.streams_heartbeat_interval,
            idle_timeout: @config.streams_idle_timeout,
            logger: @logger
          )
          @started = false
          @shutdown_mutex = Mutex.new
        end

        def start
          return if @started

          @started = true
          # Pump first: it must be ready to accept writes before the dispatcher
          # can post any (issue #321). No-op when offload is off (@pump nil).
          @pump&.start
          @listener.start
          @dispatcher.start
          @heartbeat.start
          self
        end

        # Enqueue a new SSE client. The dispatcher picks this up on its next
        # iteration and runs the 5-step race-free replay sequence. The StreamApp
        # calls this right after hijacking the socket.
        #
        # Guarded against the worker-shutdown race: if the request thread
        # arrives here after `shutdown!` has flipped @started, we mark the
        # connection dead and bail out instead of enqueueing a
        # ConnectMessage. Otherwise the message would land on a dispatch
        # queue that no one is draining, leaving the socket outside the
        # registry and outside close_all_connections — the client would
        # never see the pgbus:shutdown sentinel.
        def register(connection)
          return if connection.dead?

          @shutdown_mutex.synchronize do
            unless @started
              connection.mark_dead!
              return
            end

            @dispatch_queue << StreamEventDispatcher::ConnectMessage.new(connection: connection)
          end
        end

        # Graceful shutdown for Puma worker restart. Order matters:
        #   1. Heartbeat first (stop writing comments to connections we're
        #      about to close)
        #   2. Listener next (stop accepting new NOTIFYs)
        #   3. Dispatcher next (drain the queue; it's now finite because
        #      nothing else writes into it)
        #   4. Writer pump next (issue #321): once the dispatcher — the pump's
        #      only producer — is stopped, each partition is finite, so the
        #      pump drains every accepted-but-unflushed durable frame and joins
        #      its workers before we touch the sockets. No-op when offload is
        #      off. Must come BEFORE close_all_connections so buffered frames
        #      flush before their sockets close.
        #   5. Send pgbus:shutdown sentinel to every connection and close
        #      their sockets. We do this AFTER stopping the dispatcher AND the
        #      pump so nothing else is writing to these IOs concurrently.
        #
        # Bounded by the configured write deadline per connection; a dead
        # client drops instantly, a slow one stalls for at most write_deadline_ms.
        def shutdown!
          @shutdown_mutex.synchronize do
            return unless @started

            @started = false
            safely { @heartbeat.stop }
            safely { @listener.stop }
            safely { @dispatcher.stop }
            safely { @pump&.stop }
            close_all_connections
          end
        end

        private

        def safely
          yield
        rescue StandardError => e
          @logger.warn { "[Pgbus::Streamer::Instance] component stop raised: #{e.class}: #{e.message}" }
        end

        # Off-thread durable fanout writer (issue #321). nil (the default) keeps
        # fanout writes inline on the dispatcher thread. When on_dead fires
        # (a write failed), the pump posts a DisconnectMessage onto the shared
        # dispatch queue — the SAME explicit protocol prune_dead and the
        # heartbeat use — so the dispatcher thread owns the lockless cursor +
        # registry cleanup and the pump never touches dispatcher state.
        def build_pump
          threads = @config.streams_writer_threads
          return nil unless threads.positive?

          OutboundPump.new(
            threads: threads,
            ack_queue: @ack_queue,
            on_dead: ->(conn) { @dispatch_queue << StreamEventDispatcher::DisconnectMessage.new(connection: conn) },
            buffer_limit: @config.streams_writer_buffer_limit,
            logger: @logger
          )
        end

        def close_all_connections
          sentinel_bytes = Pgbus::Streams::Envelope.message(
            id: 0,
            event: "pgbus:shutdown",
            data: '{"reason":"worker_restart"}'
          )

          @registry.each_connection do |connection|
            safely { connection.write_sentinel(sentinel_bytes) }
            safely { connection.close }
          end
        end

        # Build the INITIAL LISTEN connection: raw connect, reject a replica
        # fatally (a misconfiguration at boot), then run the one-shot delivery
        # self-probe. The reconnect path uses build_raw_pg_connection directly
        # (validation retries there, probe is skipped) — see the factory wired
        # into Listener.new above.
        def build_pg_connection
          probe(validate_primary(build_raw_pg_connection))
        end

        # Raw PG.connect with no validation or probe. This is what the Listener's
        # connection_factory calls on every reconnect attempt: the reconnect loop
        # runs its own PrimaryValidator (retrying on a replica) and skips the
        # probe to stay cheap, mirroring Pgbus::Process::NotifyListener.
        def build_raw_pg_connection
          require "pg" unless defined?(::PG::Connection)
          opts = @config.streams_connection_options
          case opts
          when String then ::PG.connect(opts)
          when Hash   then ::PG.connect(**opts)
          when Proc
            # The Proc branch in connection_options typically returns
            # ActiveRecord::Base.connection.raw_connection — a pooled
            # AR connection. That's fatal for the streamer: LISTEN and
            # wait_for_notify bind the session to the connection object,
            # so if AR's pool hands the same raw_connection to another
            # thread mid-wait, we get concurrent libpq calls → segfault
            # or result corruption. Bail out with a clear error instead
            # of silently segfaulting in production.
            raise Pgbus::ConfigurationError,
                  "Streamer cannot use a Proc-based connection_options. " \
                  "Set Pgbus.configuration.database_url or connection_params " \
                  "so the streamer owns its own dedicated PG::Connection for LISTEN/NOTIFY."
          else
            raise Pgbus::ConfigurationError,
                  "Cannot build streamer PG connection from #{opts.class}"
          end
        end

        # Reject a connection that landed on a read-only replica before the
        # streamer wires it into a Listener. After a failover, stale DNS can
        # point this fresh PG.connect at the demoted master; NOTIFY fires only
        # on the primary, so the streamer would sit deaf. Unlike the reconnect
        # loop (which retries), this runs once at Instance construction (Puma
        # worker boot), so a replica here is a fatal misconfiguration: re-raise
        # as a ConfigurationError naming the direct-connection overrides.
        def validate_primary(pg_connection)
          Pgbus::Process::PrimaryValidator.validate_primary!(pg_connection)
        rescue Pgbus::Process::ReplicaConnectionError => e
          close_quietly(pg_connection)
          raise Pgbus::ConfigurationError,
                "Streamer LISTEN connection landed on a read-only replica " \
                "(#{e.message}). NOTIFY fires only on the primary, so the " \
                "streamer would never wake. Point the streamer at a DIRECT " \
                "primary via streams_database_url (or streams_host / " \
                "streams_port)."
        end

        def close_quietly(pg_connection)
          pg_connection.close if pg_connection.respond_to?(:close)
        rescue StandardError
          nil
        end

        # One-shot LISTEN/NOTIFY delivery self-probe on the freshly built
        # connection. A transaction-mode pooler or replica that silently breaks
        # NOTIFY is surfaced with an actionable error (naming the streams_*
        # overrides); the streamer still starts and degrades to slow SSE
        # updates rather than crashing. Returns the connection unchanged.
        def probe(pg_connection)
          Pgbus::Process::NotifyProbe.probe_notify_delivery!(pg_connection, logger: @logger)
          pg_connection
        end
      end
    end
  end
end
