# frozen_string_literal: true

module Pgbus
  module Process
    # Owns a single dedicated PG::Connection that LISTENs on the INSERT NOTIFY
    # channel of every queue a Worker/Consumer reads, and fires a WakeSignal the
    # moment any of them receives a row. This converts the worker/consumer loop
    # from "blind-read every polling_interval" into "sleep until a real insert,
    # poll only as a fallback" — eliminating the empty-read storm that dominates
    # DB load on idle queues.
    #
    # pgmq-ruby's `wait_for_notify(queue, timeout:)` is single-queue and wraps
    # the wait in `with_connection`, which only watches one channel and holds the
    # pooled connection for the whole wait. Neither fits a worker that reads N
    # queues on a small shared pool. So we own ONE raw PG::Connection and
    # hand-roll per-channel LISTEN on it.
    #
    # A persistent LISTEN connection silently dies under a transaction-pool
    # PgBouncer (LISTEN does not survive COMMIT boundaries). Point this
    # connection at a DIRECT port via `config.worker_notify_*` overrides.
    # The health-check-on-timeout catches a connection killed out from under us
    # and re-LISTENs everything.
    #
    # NOTIFY channel naming (pgmq trigger): PG_NOTIFY('pgmq.' || table || '.' ||
    # TG_OP). For queue `pgbus_default` the table is `q_pgbus_default`, so the
    # channel is `pgmq.q_pgbus_default.INSERT`.
    #
    # Thread safety: @running, @conn, and @listening_to are guarded by
    # @state_mutex. The listener thread owns @conn during wait_for_notify (a
    # blocking IO call where the mutex MUST NOT be held), so wait_once reads
    # the connection out of the mutex first and operates on a local. Reconnect
    # publishes the new connection + channel set under the mutex.
    class NotifyListener
      CHANNEL_PREFIX = "pgmq.q_"
      CHANNEL_SUFFIX = ".INSERT"

      RECONNECT_BACKOFF_SECONDS = 0.5

      def initialize(physical_queues:, on_wake:, connection_options:,
                     health_check_ms: 1000, logger: Pgbus.logger)
        @physical_queues = Array(physical_queues)
        @on_wake = on_wake
        @connection_options = connection_options
        @health_check_ms = health_check_ms
        @logger = logger
        @state_mutex = Mutex.new
        @listening_to = Set.new
        @commands = Queue.new
        @running = false
        @thread = nil
        @conn = nil
      end

      def listening_to
        @state_mutex.synchronize { @listening_to.dup }
      end

      def start
        @state_mutex.synchronize do
          return self if @running

          @running = true
        end
        @physical_queues.each { |q| @commands << [:listen, q] }
        @thread = Thread.new { run_loop }
        self
      end

      def stop
        conn_to_close = nil
        @state_mutex.synchronize do
          return self unless @running

          @running = false
          conn_to_close = @conn
        end
        @commands << [:stop]
        # Interrupt the blocking wait by closing the socket; the rescue in
        # wait_once sees @running == false and exits cleanly.
        begin
          conn_to_close&.close if conn_to_close.respond_to?(:close)
        rescue StandardError
          nil
        end
        @thread&.join(5)
        @thread = nil
        self
      end

      def add_queue(physical_queue)
        @commands << [:listen, physical_queue]
      end

      def remove_queue(physical_queue)
        @commands << [:unlisten, physical_queue]
      end

      # Public so the owning worker can detect a listener whose thread died
      # (run_loop hit a fatal error and cleared @running in its ensure) and
      # restart it. Guarded by @state_mutex like every other @running access.
      def running?
        @state_mutex.synchronize { @running }
      end

      private

      def run_loop
        conn = build_connection
        # Reject a connection that landed on a read-only replica before doing
        # anything else. After a failover, stale DNS can point this fresh
        # connection at the demoted master; NOTIFY fires only on the primary,
        # so we'd sit deaf forever. A replica here raises ReplicaConnectionError
        # and the rescue below runs the fatal/ensure path (worker-level startup
        # retry is a separate concern); a reconnect converges on the primary.
        PrimaryValidator.validate_primary!(conn)
        # One-shot delivery self-probe on the initial connection only. A pooler
        # or replica that silently breaks LISTEN/NOTIFY is surfaced here with an
        # actionable error; the listener still runs and degrades to polling.
        # Reconnects skip the probe to stay cheap.
        NotifyProbe.probe_notify_delivery!(conn, logger: @logger)
        @state_mutex.synchronize { @conn = conn }
        drain_commands

        loop do
          break unless running?

          drain_commands
          break unless running?

          wait_once
        end
      rescue StandardError => e
        @logger.error { "[Pgbus::NotifyListener] fatal: #{e.class}: #{e.message}" } if running?
      ensure
        # Clear @running so #start can spawn a fresh thread after a fatal exit
        # (e.g. build_connection raising at boot). Without this, the dead
        # thread's @running stays true and #start returns early forever.
        @state_mutex.synchronize { @running = false }
        safe_unlisten_all
        safe_close
      end

      def wait_once
        conn = @state_mutex.synchronize { @conn }
        return reconnect! unless conn

        timeout_s = @health_check_ms / 1000.0
        got_notify = conn.wait_for_notify(timeout_s) do |_channel, _pid, _payload|
          @on_wake.call
        end
        run_health_check(conn) unless got_notify
      rescue IOError, PG::Error => e
        return unless running?

        @logger.warn { "[Pgbus::NotifyListener] connection error (#{e.class}: #{e.message}) — reconnecting" }
        reconnect!
      end

      def drain_commands
        loop do
          cmd = @commands.pop(true)
          case cmd[0]
          when :listen   then do_listen(cmd[1])
          when :unlisten then do_unlisten(cmd[1])
          when :stop
            @state_mutex.synchronize { @running = false }
            return
          end
        rescue ThreadError
          return
        end
      end

      def do_listen(physical_queue)
        channel = channel_for(physical_queue)
        conn = @state_mutex.synchronize do
          return if @listening_to.include?(channel)

          @conn
        end
        return unless conn

        conn.exec(%(LISTEN "#{channel}"))
        @state_mutex.synchronize { @listening_to.add(channel) }
      end

      def do_unlisten(physical_queue)
        channel = channel_for(physical_queue)
        conn = @state_mutex.synchronize do
          return unless @listening_to.include?(channel)

          @conn
        end
        return unless conn

        conn.exec(%(UNLISTEN "#{channel}"))
        @state_mutex.synchronize { @listening_to.delete(channel) }
      end

      def run_health_check(conn)
        conn.exec("SELECT 1")
      end

      # Retry reconnect until either we succeed (new conn + every channel
      # re-LISTENed) or @running flips to false. Without the loop, a single
      # PG::Error during build/LISTEN left @conn nil and the listener degraded
      # silently — wait_once would re-enter and fail forever or run with an
      # incomplete subscription set.
      def reconnect!
        channels = @state_mutex.synchronize { @listening_to.to_a }
        loop do
          return unless running?

          safe_close
          new_conn = nil
          begin
            new_conn = build_connection
            # Reject a replica before re-LISTENing. A fresh PG.connect re-resolves
            # DNS, so backing off and retrying converges on the promoted master
            # once DNS catches up after a failover.
            PrimaryValidator.validate_primary!(new_conn)
            channels.each { |channel| new_conn.exec(%(LISTEN "#{channel}")) }
          rescue PG::Error, ReplicaConnectionError => e
            # build_connection may have succeeded before a later LISTEN raised,
            # or validate_primary! rejected a replica. Without this close, the
            # partially-built conn is orphaned and the next retry just allocates
            # another one — leaking PG connections on repeated failures.
            close_quietly(new_conn)
            @logger.error { "[Pgbus::NotifyListener] reconnect failed: #{e.class}: #{e.message}" }
            sleep RECONNECT_BACKOFF_SECONDS
            next
          end

          @state_mutex.synchronize do
            @conn = new_conn
            @listening_to = Set.new(channels)
          end
          return
        end
      end

      def build_connection
        require "pg" unless defined?(::PG::Connection)
        case @connection_options
        when String then ::PG.connect(@connection_options)
        when Hash   then ::PG.connect(**@connection_options)
        else
          raise Pgbus::ConfigurationError,
                "NotifyListener cannot build a PG connection from #{@connection_options.class}. " \
                "Set worker_notify_database_url / worker_notify_host / worker_notify_port, " \
                "or a base database_url, so the listener owns a dedicated connection."
        end
      end

      def safe_unlisten_all
        channels, conn = @state_mutex.synchronize { [@listening_to.to_a, @conn] }
        channels.each do |channel|
          conn&.exec(%(UNLISTEN "#{channel}"))
        rescue PG::Error
          nil
        end
        @state_mutex.synchronize { @listening_to.clear }
      end

      def safe_close
        conn = @state_mutex.synchronize do
          c = @conn
          @conn = nil
          c
        end
        close_quietly(conn)
      end

      # Close an unpublished PG::Connection (one that never made it into @conn,
      # e.g. a half-built reconnect attempt where LISTEN raised). Best-effort.
      def close_quietly(conn)
        conn&.close if conn.respond_to?(:close)
      rescue StandardError
        nil
      end

      def channel_for(physical_queue)
        "#{CHANNEL_PREFIX}#{physical_queue}#{CHANNEL_SUFFIX}"
      end
    end
  end
end
