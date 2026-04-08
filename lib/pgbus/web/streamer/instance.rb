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
        attr_reader :registry, :listener, :dispatcher, :heartbeat, :dispatch_queue

        def initialize(
          client: Pgbus.client,
          config: Pgbus.configuration,
          pg_connection: nil,
          logger: Pgbus.logger,
          registry: nil,
          dispatch_queue: nil
        )
          @client = client
          @config = config
          @logger = logger
          @registry = registry || Registry.new
          @dispatch_queue = dispatch_queue || Queue.new

          @pg_connection = pg_connection || build_pg_connection
          @listener = Listener.new(
            pg_connection: @pg_connection,
            dispatch_queue: @dispatch_queue,
            health_check_ms: @config.streams_listen_health_check_ms,
            logger: @logger
          )
          @dispatcher = Dispatcher.new(
            client: @client,
            registry: @registry,
            listener: @listener,
            dispatch_queue: @dispatch_queue,
            logger: @logger
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

            @dispatch_queue << Dispatcher::ConnectMessage.new(connection: connection)
          end
        end

        # Graceful shutdown for Puma worker restart. Order matters:
        #   1. Heartbeat first (stop writing comments to connections we're
        #      about to close)
        #   2. Listener next (stop accepting new NOTIFYs)
        #   3. Dispatcher next (drain the queue; it's now finite because
        #      nothing else writes into it)
        #   4. Send pgbus:shutdown sentinel to every connection and close
        #      their sockets. We do this AFTER stopping the dispatcher so
        #      no one else is writing to these IOs concurrently.
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
            close_all_connections
          end
        end

        private

        def safely
          yield
        rescue StandardError => e
          @logger.warn { "[Pgbus::Streamer::Instance] component stop raised: #{e.class}: #{e.message}" }
        end

        def close_all_connections
          sentinel_bytes = Pgbus::Streams::Envelope.message(
            id: 0,
            event: "pgbus:shutdown",
            data: '{"reason":"worker_restart"}'
          )
          deadline_ms = @config.streams_write_deadline_ms

          @registry.each_connection do |connection|
            # IoWriter holds the connection's mutex, so this write is
            # serialised against any write the dispatcher/heartbeat
            # might still be performing if their stop hadn't fully
            # returned yet.
            safely { IoWriter.write(connection, sentinel_bytes, deadline_ms: deadline_ms) }
            safely do
              connection.io.close if connection.io.respond_to?(:close) && !connection.io.closed?
            end
          end
        end

        def build_pg_connection
          require "pg" unless defined?(::PG::Connection)
          opts = @config.connection_options
          case opts
          when String then ::PG.connect(opts)
          when Hash   then ::PG.connect(**opts)
          when Proc   then opts.call
          else
            raise ConfigurationError,
                  "Cannot build streamer PG connection from #{opts.class}"
          end
        end
      end
    end
  end
end
