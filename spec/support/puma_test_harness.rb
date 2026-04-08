# frozen_string_literal: true

require "puma"
require "puma/server"
require "puma/configuration"
require "puma/log_writer"
require "puma/events"
require "socket"

module SseTestSupport
  # Boots a Puma::Server on an ephemeral port in a background thread
  # and waits for it to accept TCP connections. Used by the streams
  # integration specs to exercise the `rack.hijack` path under a real
  # Puma — no shortcuts, no mocked env. If the hijack + thread-release
  # behavior we rely on (puma/puma#1009) is wrong, this is where we
  # find out.
  #
  # Usage:
  #
  #   harness = PumaTestHarness.boot(rack_app: app)
  #   url = "http://127.0.0.1:#{harness.port}/..."
  #   # ... do the test ...
  #   harness.shutdown
  #
  # Important: each test gets its own harness. Puma::Server is stateful
  # and we don't want one test's hijacked connections to leak into the
  # next test's assertions.
  class PumaTestHarness
    attr_reader :port, :server

    def self.boot(rack_app:, host: "127.0.0.1", max_threads: 8)
      harness = new(rack_app: rack_app, host: host, max_threads: max_threads)
      harness.start
      harness
    end

    def initialize(rack_app:, host:, max_threads:)
      @rack_app = rack_app
      @host     = host
      @max_threads = max_threads
      @port     = nil
      @server   = nil
      @thread   = nil
    end

    def start
      # Silence Puma's output entirely. The log_writer only exposes
      # writable IO-like objects so we point both stdout and stderr
      # at /dev/null equivalents.
      log_writer = Puma::LogWriter.new(StringIO.new, StringIO.new)
      events = Puma::Events.new
      @server = Puma::Server.new(@rack_app, events, min_threads: 0, max_threads: @max_threads, log_writer: log_writer)
      @server.add_tcp_listener(@host, 0) # 0 = ephemeral
      @port = @server.connected_ports.first

      @thread = Thread.new do
        Thread.current.abort_on_exception = false
        @server.run(true) # true = ensure @status is set before returning
      end

      wait_until_accepting(timeout: 5)
      self
    end

    def url(path = "/")
      "http://#{@host}:#{@port}#{path}"
    end

    def shutdown
      return unless @server

      # stop(true) waits for in-flight requests. For the SSE tests
      # with hijacked connections, Puma does NOT track hijacked IOs
      # in its thread pool, so stop returns quickly even if hijacked
      # connections are still open.
      begin
        @server.stop(true)
      rescue StandardError
        # nothing sensible to do — the server may already be halfway
        # through its own shutdown path.
      end
      @thread&.join(5)
      @thread = nil
      @server = nil
    end

    private

    def wait_until_accepting(timeout:)
      # Monotonic clock so the wait can't be fooled by NTP corrections
      # or system-time jumps mid-boot. Matches SseTestClient's existing
      # use of CLOCK_MONOTONIC for its own wait loops.
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        begin
          sock = TCPSocket.new(@host, @port)
          sock.close
          return
        rescue Errno::ECONNREFUSED, Errno::ECONNRESET
          sleep 0.02
        end
      end
      raise "PumaTestHarness failed to accept connections on port #{@port} within #{timeout}s"
    end
  end
end
