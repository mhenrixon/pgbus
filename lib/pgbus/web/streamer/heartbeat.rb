# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Periodic maintenance loop for SSE connections. Runs three sweeps
      # on every tick:
      #
      #   1. Write an SSE comment (": heartbeat <epoch>\n\n") to each
      #      connection. This keeps proxies and load balancers from timing
      #      out idle HTTP responses; most reverse proxies close HTTP
      #      responses that sit idle for 30-60s, which would silently drop
      #      SSE clients.
      #
      #   2. Mark connections that have been idle longer than the
      #      configured idle_timeout as dead. The Dispatcher's next pass
      #      picks them up via its disconnect path.
      #
      #   3. Post a DisconnectMessage for any connection already flagged
      #      dead (by IoWriter returning :closed / :blocked, or by the
      #      idle sweep above).
      #
      # The heartbeat runs on its own dedicated thread because it does
      # blocking writes (via IoWriter with a deadline) and we don't want
      # to delay the dispatcher. Writes are serialised per-connection by
      # the Connection's own mutex, so concurrent dispatcher + heartbeat
      # writes are safe.
      class Heartbeat
        def initialize(registry:, dispatch_queue:, interval:, idle_timeout:, logger: Pgbus.logger, clock: nil)
          @registry = registry
          @queue = dispatch_queue
          @interval = interval
          @idle_timeout = idle_timeout
          @logger = logger
          @clock = clock || -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) }
          @running = false
          @thread = nil
          @wake = ConditionVariable.new
          @wake_mutex = Mutex.new
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
          @wake_mutex.synchronize { @wake.broadcast }
          @thread&.join(5)
          @thread = nil
          self
        end

        # Runs a single sweep synchronously. Useful for tests — production
        # code goes through the background thread.
        def tick
          now = @clock.call
          @registry.each_connection do |connection|
            if connection.dead?
              # Already dead (e.g. IoWriter returned :closed on a previous
              # dispatcher write). Post the disconnect and skip the rest.
              enqueue_disconnect(connection)
              next
            end

            if connection.idle_for > @idle_timeout
              connection.mark_dead!
              enqueue_disconnect(connection)
              next
            end

            result = connection.write_comment("heartbeat #{now.to_i}")
            enqueue_disconnect(connection) if connection.dead? || result != :ok
          end
        end

        private

        def run_loop
          while @running
            begin
              tick
            rescue StandardError => e
              @logger.error { "[Pgbus::Streamer::Heartbeat] tick raised: #{e.class}: #{e.message}" }
            end

            @wake_mutex.synchronize do
              @wake.wait(@wake_mutex, @interval) if @running
            end
          end
        end

        def enqueue_disconnect(connection)
          @queue << Dispatcher::DisconnectMessage.new(connection: connection)
        end
      end
    end
  end
end
