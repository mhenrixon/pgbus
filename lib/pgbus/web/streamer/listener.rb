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
      #   - #start spawns ONE listener thread that sits in wait_for_notify
      #   - ensure_listening / remove_listening execute LISTEN/UNLISTEN SQL
      #     synchronously on the caller's thread, guarded by @conn_mutex
      #     so they interleave safely with the listener's wait_for_notify.
      #     The listener's run_loop does `sleep 0` between iterations to
      #     release the mutex long enough for the dispatcher thread to
      #     acquire it. Deferring LISTEN onto the listener thread would
      #     open a race where a NOTIFY fires before the listener processes
      #     the LISTEN command, silently dropping the notification.
      #   - #stop joins the thread cleanly
      #
      # Health check: `wait_for_notify(timeout)` returns nil on timeout. When
      # it does, the listener runs `SELECT 1` as a TCP keepalive. If that
      # raises, the connection is reset (`conn.reset`) and every channel in
      # `@listening_to` is re-LISTENed. This is the fix for silently dropped
      # LISTEN connections from NAT / PG restart / network blips.
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
          @running = false
          @thread = nil
          # Mutex to serialise PG::Connection access between the listener
          # thread (which sits in wait_for_notify) and the dispatcher thread
          # (which calls ensure_listening synchronously). libpq is not
          # thread-safe for concurrent calls on the same connection, so we
          # guard every call with this mutex. wait_for_notify holds the
          # lock for at most health_check_ms before releasing it, which
          # bounds dispatcher wait time.
          @conn_mutex = Mutex.new
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
          # Interrupt the blocking wait_for_notify by closing the PG
          # connection. Without this, the listener thread would sit
          # inside wait_for_notify until a NOTIFY arrived, which may
          # never happen. Closing the socket raises PG::Error / IOError
          # inside wait_for_notify; the rescue clause sees @running ==
          # false and exits cleanly.
          begin
            @conn.close if @conn.respond_to?(:close)
          rescue StandardError
            # best effort — connection may already be gone
          end
          @thread&.join(5)
          @thread = nil
          self
        end

        # Synchronously issues LISTEN on the PG connection. Called from
        # the Dispatcher thread during handle_connect — we CANNOT defer
        # this to the listener thread because the race window between
        # "listener has not yet drained the command queue" and "a new
        # broadcast fires a NOTIFY" would silently lose the NOTIFY
        # (PG only delivers NOTIFYs to sessions that were already
        # LISTENing when the NOTIFY was issued).
        def ensure_listening(queue_name)
          channel = channel_for(queue_name)
          @conn_mutex.synchronize do
            return if @listening_to.include?(channel)

            # Briefly acquires the mutex from under the listener thread's
            # wait_for_notify. The listener's run_loop does `sleep 0`
            # between iterations specifically to yield the scheduler
            # here, so this synchronize call waits at most
            # health_check_ms (default 5s) before the listener releases.
            @conn.exec(%(LISTEN "#{channel}"))
            @listening_to.add(channel)
          end
        end

        def remove_listening(queue_name)
          channel = channel_for(queue_name)
          @conn_mutex.synchronize do
            return unless @listening_to.include?(channel)

            @conn.exec(%(UNLISTEN "#{channel}"))
            @listening_to.delete(channel)
          end
        end

        private

        def run_loop
          timeout_s = @health_check_ms / 1000.0
          loop do
            break unless @running

            begin
              notified = false
              @conn_mutex.synchronize do
                break unless @running

                notified = @conn.wait_for_notify(timeout_s) do |channel, _pid, _payload|
                  handle_notify(channel)
                end
              end

              # Yield the scheduler between iterations. Without this, the
              # listener thread re-acquires the mutex for the next
              # wait_for_notify immediately and starves other threads (the
              # dispatcher calling ensure_listening) that want to grab the
              # lock between iterations. A zero-second sleep is enough to
              # trigger a Ruby thread reschedule.
              sleep 0

              run_health_check unless notified
            rescue PG::Error, IOError => e
              # During shutdown we expect wait_for_notify to raise because
              # we deliberately closed the connection to unblock this
              # thread (see #stop). In that case @running is already
              # false — exit cleanly, don't try to reconnect.
              break unless @running

              @logger.warn { "[Pgbus::Streamer::Listener] #{e.class} (#{e.message}) — reconnecting" }
              reconnect!
            end
          end
        ensure
          safe_unlisten_all
        end

        def handle_notify(channel)
          queue_name = queue_name_from(channel)
          return unless queue_name

          @dispatch_queue << WakeMessage.new(queue_name: queue_name)
        end

        def run_health_check
          @conn_mutex.synchronize { @conn.exec("SELECT 1") }
        end

        def reconnect!
          @conn_mutex.synchronize do
            @conn.reset
            to_relisten = @listening_to.to_a
            @listening_to.clear
            to_relisten.each do |channel|
              @conn.exec(%(LISTEN "#{channel}"))
              @listening_to.add(channel)
            end
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
