# frozen_string_literal: true

module Pgbus
  module Process
    # Owns a single dedicated PG::Connection that LISTENs on the INSERT NOTIFY
    # channel of every queue a Worker/Consumer reads, and fires a WakeSignal the
    # moment any of them receives a row. This converts the worker/consumer loop
    # from "blind-read every polling_interval" into "sleep until a real insert,
    # poll only as a fallback" — eliminating the empty-read storm that dominates
    # DB load on idle queues (one pgmq.read per queue per polling tick → ~zero).
    #
    # This is the SPIKE component for NOTIFY-gated wakeups. It is opt-in via
    # `config.worker_notify_wakeup` and OFF by default. See
    # docs/design/notify-wakeup.md for the design, failure modes, and the
    # connection-budget validation that gates turning it on in production.
    #
    # ── Why a dedicated connection on (optionally) a direct port ────────────
    # pgmq-ruby's `wait_for_notify(queue, timeout:)` is single-queue and wraps
    # the wait in `with_connection`, which (a) only watches one channel and
    # (b) HOLDS the pooled connection for the whole wait. Neither fits a worker
    # that reads N queues and runs on a small shared pool. So, exactly like the
    # SSE Streamer's Listener, we own ONE raw PG::Connection and hand-roll
    # per-channel LISTEN on it.
    #
    # A persistent LISTEN connection silently dies under a transaction-pool
    # PgBouncer (LISTEN does not survive COMMIT boundaries). The fix is the same
    # as the streamer's: point this connection at a DIRECT port via
    # `config.worker_notify_*` overrides. The health-check-on-timeout below also
    # catches a connection killed out from under us (NAT/restart/idle reap) and
    # re-LISTENs everything.
    #
    # ── Threading ──────────────────────────────────────────────────────────
    #   - #start spawns ONE listener thread per Worker/Consumer (i.e. per fork).
    #   - The thread only ever runs `wait_for_notify`; LISTEN/UNLISTEN SQL for
    #     queue-set changes (wildcard refresh, circuit-breaker eviction) is fed
    #     through a command queue drained between waits.
    #   - On NOTIFY (or a queue going live) it calls `@on_wake.call` — the Worker
    #     passes its existing single-waiter WakeSignal#notify!. notify! is
    #     safe from any thread; only the worker's main loop calls #wait.
    #   - #stop closes the socket to interrupt a blocking wait, then joins.
    #
    # NOTIFY channel naming (pgmq trigger): PG_NOTIFY('pgmq.' || table || '.' ||
    # TG_OP). For queue `pgbus_default` the table is `q_pgbus_default`, so the
    # channel is `pgmq.q_pgbus_default.INSERT`. The queue names handed in here
    # are the PHYSICAL names (prefix already applied) so they match the trigger.
    class NotifyListener
      CHANNEL_PREFIX = "pgmq.q_"
      CHANNEL_SUFFIX = ".INSERT"

      attr_reader :listening_to

      # @param physical_queues [Array<String>] prefixed queue names to LISTEN on
      # @param on_wake [#call] invoked on any INSERT NOTIFY (e.g. wake_signal.notify!)
      # @param connection_options [String,Hash] libpq conninfo for the dedicated
      #   connection (typically config.worker_notify_connection_options)
      # @param health_check_ms [Integer] wait timeout; on expiry we SELECT 1 to
      #   detect a silently-dead connection, doubling as the poll fallback cadence.
      def initialize(physical_queues:, on_wake:, connection_options:,
                     health_check_ms: 1000, logger: Pgbus.logger)
        @physical_queues = Array(physical_queues)
        @on_wake = on_wake
        @connection_options = connection_options
        @health_check_ms = health_check_ms
        @logger = logger
        @listening_to = Set.new
        @commands = Queue.new
        @running = false
        @thread = nil
        @conn = nil
      end

      def start
        return self if @running

        @running = true
        @physical_queues.each { |q| @commands << [:listen, q] }
        @thread = Thread.new { run_loop }
        self
      end

      def stop
        return self unless @running

        @running = false
        @commands << [:stop]
        # Interrupt the blocking wait_for_notify by closing the socket; the
        # rescue in run_loop sees @running == false and exits cleanly.
        begin
          @conn&.close if @conn.respond_to?(:close)
        rescue StandardError
          # best effort — connection may already be gone
        end
        @thread&.join(5)
        @thread = nil
        self
      end

      # Add a queue to the LISTEN set at runtime (wildcard refresh picks up a
      # new queue). Asynchronous: the listener thread executes it before its
      # next wait. Safe to call from the worker's main loop thread.
      def add_queue(physical_queue)
        @commands << [:listen, physical_queue]
      end

      # Drop a queue from the LISTEN set (queue deleted / evicted).
      def remove_queue(physical_queue)
        @commands << [:unlisten, physical_queue]
      end

      private

      def run_loop
        @conn = build_connection
        drain_commands

        loop do
          break unless @running

          drain_commands
          break unless @running

          wait_once
        end
      rescue StandardError => e
        @logger.error { "[Pgbus::NotifyListener] fatal: #{e.class}: #{e.message}" } if @running
      ensure
        safe_unlisten_all
        safe_close
      end

      def wait_once
        timeout_s = @health_check_ms / 1000.0
        got_notify = @conn.wait_for_notify(timeout_s) do |_channel, _pid, _payload|
          # We don't care which queue fired — the worker re-reads all of them.
          # Coalescing every channel into a single wake is exactly what we want:
          # one wake, the loop drains whatever is ready across all queues.
          @on_wake.call
        end
        run_health_check unless got_notify
      rescue IOError, PG::Error => e
        # #stop closes the connection to interrupt the wait → IOError/PG::Error.
        return unless @running

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
            @running = false
            return
          end
        rescue ThreadError
          return # queue empty
        end
      end

      def do_listen(physical_queue)
        channel = channel_for(physical_queue)
        return if @listening_to.include?(channel)

        @conn.exec(%(LISTEN "#{channel}"))
        @listening_to.add(channel)
      end

      def do_unlisten(physical_queue)
        channel = channel_for(physical_queue)
        return unless @listening_to.include?(channel)

        @conn.exec(%(UNLISTEN "#{channel}"))
        @listening_to.delete(channel)
      end

      def run_health_check
        @conn.exec("SELECT 1")
      end

      def reconnect!
        safe_close
        @conn = build_connection
        # Re-LISTEN everything we were watching. Build the new set first so a
        # mid-loop failure doesn't lose channels not yet retried.
        to_relisten = @listening_to.to_a
        @listening_to = Set.new
        to_relisten.each do |channel|
          @conn.exec(%(LISTEN "#{channel}"))
          @listening_to.add(channel)
        end
      rescue PG::Error => e
        @logger.error { "[Pgbus::NotifyListener] reconnect failed: #{e.class}: #{e.message}" }
        sleep 0.5
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
        @listening_to.each do |channel|
          @conn&.exec(%(UNLISTEN "#{channel}"))
        rescue PG::Error
          # connection may be dead; nothing to do
        end
        @listening_to.clear
      end

      def safe_close
        @conn&.close if @conn.respond_to?(:close)
      rescue StandardError
        # best effort
      ensure
        @conn = nil
      end

      def channel_for(physical_queue)
        "#{CHANNEL_PREFIX}#{physical_queue}#{CHANNEL_SUFFIX}"
      end
    end
  end
end
