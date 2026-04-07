# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Owns a single dedicated PG::Connection running LISTEN against every
      # stream channel currently serving at least one SSE subscriber. On
      # NOTIFY, posts a WakeMessage into the dispatch queue; the Dispatcher
      # thread does the actual read_after + fanout.
      #
      # Threading:
      #   - #start spawns ONE listener thread
      #   - ensure_listening / remove_listening are called from the
      #     dispatcher thread, which means the listener thread itself is
      #     only running `wait_for_notify` — all LISTEN/UNLISTEN SQL goes
      #     through a command queue that the listener thread drains between
      #     notifies
      #   - #stop joins the thread cleanly
      #
      # Health check: `wait_for_notify(timeout)` returns nil on timeout. When
      # it does, the listener runs `SELECT 1` as a TCP keepalive. If that
      # raises, the connection is reset (`conn.reset`) and every channel in
      # `@listening_to` is re-LISTENed. This is the fix for design doc §11 #1
      # (silently dropped LISTEN connections from NAT / PG restart / network
      # blips).
      #
      # NOTIFY channel naming (from pgmq_v1.11.0.sql:1634):
      #   PG_NOTIFY('pgmq.' || TG_TABLE_NAME || '.' || TG_OP, NULL)
      # For a queue named `pgbus_stream_chat` the trigger table is
      # `q_pgbus_stream_chat`, so the channel is `pgmq.q_pgbus_stream_chat.INSERT`.
      class Listener
        WakeMessage = Data.define(:queue_name)

        CHANNEL_PREFIX = "pgmq.q_"
        CHANNEL_SUFFIX = ".INSERT"

        attr_reader :listening_to

        def initialize(pg_connection:, dispatch_queue:, health_check_ms:, logger: Pgbus.logger)
          @conn = pg_connection
          @dispatch_queue = dispatch_queue
          @health_check_ms = health_check_ms
          @logger = logger
          @listening_to = Set.new
          @commands = Queue.new
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
          @commands << [:stop]
          # Interrupt the blocking wait_for_notify by closing the PG
          # connection. Without this, the listener thread would sit
          # inside wait_for_notify until a NOTIFY arrived, which may
          # never happen. Closing the socket raises PG::Error inside
          # wait_for_notify; our rescue clause sees @running == false
          # on the next iteration and exits cleanly.
          begin
            @conn.close if @conn.respond_to?(:close)
          rescue StandardError
            # best effort — connection may already be gone
          end
          @thread&.join(5)
          @thread = nil
          self
        end

        # Post a LISTEN command into the listener thread's command queue. The
        # listener will drain the queue between notify polls.
        def ensure_listening(queue_name)
          @commands << [:listen, queue_name]
        end

        def remove_listening(queue_name)
          @commands << [:unlisten, queue_name]
        end

        private

        def run_loop
          loop do
            break unless @running

            drain_commands
            break unless @running

            timeout_s = @health_check_ms / 1000.0
            begin
              @conn.wait_for_notify(timeout_s) do |channel, _pid, _payload|
                handle_notify(channel)
              end || run_health_check
            rescue PG::Error => e
              break unless @running

              @logger.warn { "[Pgbus::Streamer::Listener] PG error (#{e.class}: #{e.message}) — reconnecting" }
              reconnect!
            end
          end
        ensure
          safe_unlisten_all
        end

        def drain_commands
          loop do
            cmd = @commands.pop(true)
            case cmd[0]
            when :listen   then do_listen(cmd[1])
            when :unlisten then do_unlisten(cmd[1])
            when :stop     then @running = false
                                return
            end
          rescue ThreadError
            # empty queue
            return
          end
        end

        def do_listen(queue_name)
          channel = channel_for(queue_name)
          return if @listening_to.include?(channel)

          @conn.exec(%(LISTEN "#{channel}"))
          @listening_to.add(channel)
        end

        def do_unlisten(queue_name)
          channel = channel_for(queue_name)
          return unless @listening_to.include?(channel)

          @conn.exec(%(UNLISTEN "#{channel}"))
          @listening_to.delete(channel)
        end

        def handle_notify(channel)
          queue_name = queue_name_from(channel)
          return unless queue_name

          @dispatch_queue << WakeMessage.new(queue_name: queue_name)
        end

        def run_health_check
          @conn.exec("SELECT 1")
        end

        def reconnect!
          @conn.reset
          to_relisten = @listening_to.to_a
          @listening_to.clear
          to_relisten.each do |channel|
            @conn.exec(%(LISTEN "#{channel}"))
            @listening_to.add(channel)
          end
        rescue PG::Error => e
          @logger.error { "[Pgbus::Streamer::Listener] reconnect failed: #{e.class}: #{e.message}" }
          sleep 0.5
        end

        def safe_unlisten_all
          @listening_to.each do |channel|
            @conn.exec(%(UNLISTEN "#{channel}"))
          rescue PG::Error
            # connection may be dead; nothing we can do
          end
          @listening_to.clear
        end

        def channel_for(queue_name)
          "#{CHANNEL_PREFIX}#{queue_name}#{CHANNEL_SUFFIX}"
        end

        def queue_name_from(channel)
          return nil unless channel.start_with?(CHANNEL_PREFIX) && channel.end_with?(CHANNEL_SUFFIX)

          channel[CHANNEL_PREFIX.length..-(CHANNEL_SUFFIX.length + 1)]
        end
      end
    end
  end
end
