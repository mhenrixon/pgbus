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
    class NotifyListener
      CHANNEL_PREFIX = "pgmq.q_"
      CHANNEL_SUFFIX = ".INSERT"

      attr_reader :listening_to

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
        begin
          @conn&.close if @conn.respond_to?(:close)
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
          @on_wake.call
        end
        run_health_check unless got_notify
      rescue IOError, PG::Error => e
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
          return
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
          nil
        end
        @listening_to.clear
      end

      def safe_close
        @conn&.close if @conn.respond_to?(:close)
      rescue StandardError
        nil
      ensure
        @conn = nil
      end

      def channel_for(physical_queue)
        "#{CHANNEL_PREFIX}#{physical_queue}#{CHANNEL_SUFFIX}"
      end
    end
  end
end
