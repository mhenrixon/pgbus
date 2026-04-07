# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Non-blocking IO writer with a per-call deadline, serialised through the
      # connection's own mutex. This is the bug-fix for puma/puma#576: a naive
      # `io.write(bytes)` on a dead or slow SSE client deadlocks the dispatcher
      # thread until the OS closes the socket (which can take minutes under a
      # TCP keepalive). The message_bus gem hit this in production; we copy the
      # pattern.
      #
      # The write loop uses write_nonblock + IO.select so a slow client at most
      # stalls *its own* mutex-protected write for `deadline_ms`, never the
      # dispatcher or heartbeat thread. When the deadline expires with bytes
      # still pending, we return :blocked; the caller (Connection#enqueue or
      # Connection#write_comment) translates that into mark_dead!, and the
      # heartbeat sweep eventually unregisters the connection.
      #
      # Returns:
      #   :ok       — all bytes written
      #   :closed   — peer gone (EPIPE / ECONNRESET / IOError on closed IO)
      #   :blocked  — deadline hit before all bytes could be written
      module IoWriter
        def self.write(connection, bytes, deadline_ms:)
          connection.mutex.synchronize do
            write_all(connection.io, bytes, deadline_ms)
          end
        end

        def self.write_all(io, bytes, deadline_ms)
          deadline = monotonic + (deadline_ms / 1000.0)
          offset = 0
          remaining = bytes.bytesize

          while remaining.positive?
            written = attempt_write(io, bytes, offset, remaining, deadline)
            return written if written.is_a?(Symbol) # :blocked / :closed

            offset += written
            remaining -= written
          end

          :ok
        end

        def self.attempt_write(io, bytes, offset, remaining, deadline)
          chunk = bytes.byteslice(offset, remaining)
          io.write_nonblock(chunk)
        rescue IO::WaitWritable
          wait = deadline - monotonic
          return :blocked if wait <= 0

          # io.wait_writable is fiber-scheduler-friendly (Falcon v1.1) and
          # functionally identical to IO.select on threaded Puma.
          ready = io.wait_writable(wait)
          return :blocked if ready.nil?

          retry
        rescue Errno::EPIPE, Errno::ECONNRESET, IOError
          :closed
        end

        def self.monotonic
          # Qualify ::Process because Pgbus::Process shadows it in this namespace.
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        end

        private_class_method :write_all, :attempt_write, :monotonic
      end
    end
  end
end
