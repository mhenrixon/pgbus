# frozen_string_literal: true

require "socket"

module Pgbus
  # Dependency-free readiness probe for container HEALTHCHECKs (issue #386).
  #
  # exe/pgbus-health loads this file via require_relative and nothing else:
  # a docker HEALTHCHECK runs the probe every few seconds, so it must never
  # drag in Bundler, Zeitwerk, Rails, or the rest of the gem. Only Ruby's
  # bundled socket library is allowed here.
  #
  #   healthcheck:
  #     cmd: bin/pgbus-health            # port from PGBUS_HEALTH_PORT
  #     cmd: bin/pgbus-health --port 9394 --path /livez
  #
  # Exit codes: 0 healthy (HTTP 2xx), 1 unhealthy (non-2xx, refused, timeout),
  # 2 usage error (no/invalid port).
  class HealthProbe
    EXIT_OK = 0
    EXIT_UNHEALTHY = 1
    EXIT_USAGE = 2

    DEFAULT_PATH = "/readyz"
    DEFAULT_TIMEOUT = 2.0
    HOST = "127.0.0.1"

    USAGE = "usage: pgbus-health [--port PORT] [--path PATH] [--timeout SECONDS]\n       " \
            "port falls back to the PGBUS_HEALTH_PORT environment variable\n"

    def self.run(argv, env: ENV, out: $stdout, err: $stderr)
      new(argv, env: env, out: out, err: err).run
    end

    def initialize(argv, env: ENV, out: $stdout, err: $stderr)
      @out = out
      @err = err
      @path = DEFAULT_PATH
      @timeout = DEFAULT_TIMEOUT
      @port = env["PGBUS_HEALTH_PORT"]
      @usage_error = false
      parse(argv)
    end

    def run
      return usage_failure if @usage_error

      port = Integer(@port, exception: false)
      return usage_failure unless port

      probe(port)
    end

    private

    # Hand-rolled flag parsing: three flags do not justify optparse in a
    # script whose reason to exist is loading nothing.
    def parse(argv)
      args = argv.dup
      until args.empty?
        flag = args.shift
        value = args.shift
        case flag
        when "--port" then @port = value
        when "--path" then @path = value
        when "--timeout" then @timeout = value.to_f
        else
          return @usage_error = true
        end
        return @usage_error = true if value.nil?
      end
    end

    def usage_failure
      @err.write(USAGE)
      EXIT_USAGE
    end

    def probe(port)
      status = http_status(port)
      healthy = status&.between?(200, 299)
      @out.write("pgbus-health: #{@path} -> #{status || "no response"}\n")
      healthy ? EXIT_OK : EXIT_UNHEALTHY
    rescue SystemCallError, IOError => e
      @err.write("pgbus-health: #{@path} -> #{e.class}: #{e.message}\n")
      EXIT_UNHEALTHY
    end

    # Minimal HTTP/1.0 exchange: send the request, read just the status line.
    # The deadline covers connect and read together.
    def http_status(port)
      deadline = monotonic_now + @timeout
      Socket.tcp(HOST, port, connect_timeout: @timeout) do |sock|
        sock.write("GET #{@path} HTTP/1.0\r\nHost: #{HOST}\r\nConnection: close\r\n\r\n")
        line = read_status_line(sock, deadline)
        code = line&.split(" ", 3)&.fetch(1, nil)
        Integer(code, exception: false)
      end
    end

    def read_status_line(sock, deadline)
      buffer = +""
      until buffer.include?("\n")
        remaining = deadline - monotonic_now
        return nil if remaining <= 0 || !sock.wait_readable(remaining)

        chunk = sock.read_nonblock(1024, exception: false)
        return nil if chunk.nil? # EOF before a full status line
        next if chunk == :wait_readable # spurious wakeup — re-wait on the deadline

        buffer << chunk
      end
      buffer[/\A[^\r\n]*/]
    end

    # ::Process, not Process — inside the Pgbus namespace the bare constant
    # resolves to Pgbus::Process (the process model), which is also why this
    # file must never be renamed into that namespace.
    def monotonic_now
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end
end
