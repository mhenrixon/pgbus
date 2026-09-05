# frozen_string_literal: true

require "socket"
require "spec_helper"
require "pgbus/mcp/health_analyzer"

RSpec.describe Pgbus::Web::HealthServer do
  # Bind to an ephemeral port (0) on localhost so the suite never collides with
  # a real service and never needs a fixed port free.
  subject(:server) { described_class.new(port: 0, bind: "127.0.0.1", app: app) }

  let(:app) { Pgbus::Web::HealthApp.new(data_source: data_source) }
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  after { server.stop }

  # Speak the minimum HTTP/1.0 needed to get a response line + body back.
  def http_get(path)
    socket = TCPSocket.new("127.0.0.1", server.port)
    socket.write("GET #{path} HTTP/1.0\r\nHost: localhost\r\n\r\n")
    response = socket.read
    socket.close
    response
  end

  describe "#start" do
    it "binds a real port and reports it" do
      server.start

      expect(server.port).to be > 0
    end

    it "serves /livez with a 200 and an 'ok' body over a real socket" do
      server.start

      response = http_get("/livez")

      expect(response).to include("200")
      expect(response).to include("ok")
    end

    it "serves /readyz, dispatching to the app's readiness verdict" do
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).with(data_source)
                                                        .and_return(instance_double(Pgbus::MCP::HealthAnalyzer, verdict: { status: "OK" }))
      server.start

      response = http_get("/readyz")

      expect(response).to include("200")
      expect(response).to include("OK")
    end

    it "returns 404 for an unknown path" do
      server.start

      expect(http_get("/nope")).to include("404")
    end

    it "is idempotent — a second start does not raise or rebind" do
      server.start
      port = server.port

      expect { server.start }.not_to raise_error
      expect(server.port).to eq(port)
    end
  end

  describe "#stop" do
    it "closes the listening socket so the port stops accepting" do
      server.start
      port = server.port
      server.stop

      expect { TCPSocket.new("127.0.0.1", port).close }.to raise_error(SystemCallError)
    end

    it "is safe to call when never started" do
      expect { described_class.new(port: 0).stop }.not_to raise_error
    end

    it "is safe to call twice" do
      server.start
      server.stop

      expect { server.stop }.not_to raise_error
    end
  end

  describe "malformed requests" do
    it "does not crash the accept loop on a garbage request line" do
      server.start

      socket = TCPSocket.new("127.0.0.1", server.port)
      socket.write("garbage-no-crlf")
      socket.close

      # The loop survives: a well-formed request right after still works.
      expect(http_get("/livez")).to include("200")
    end
  end

  describe "a client that connects and never sends a request line" do
    # Without a bounded read the accept loop parks in gets forever and every
    # later probe queues behind it — the container goes unhealthy for good.
    # These specs hang (rather than fail) if the timeout regresses.
    subject(:server) do
      described_class.new(port: 0, bind: "127.0.0.1", app: app, read_timeout: 0.2)
    end

    it "does not starve a later probe" do
      server.start
      silent = TCPSocket.new("127.0.0.1", server.port)

      begin
        expect(http_get("/livez")).to include("200")
      ensure
        silent.close
      end
    end

    it "keeps serving once the silent client has timed out" do
      server.start
      silent = TCPSocket.new("127.0.0.1", server.port)

      begin
        http_get("/livez")

        expect(http_get("/livez")).to include("200")
      ensure
        silent.close
      end
    end
  end

  describe "#stop with a client parked mid-read" do
    # A read timeout longer than the join deadline: #stop must kill the thread
    # rather than return while it is still running.
    subject(:server) do
      described_class.new(port: 0, bind: "127.0.0.1", app: app, read_timeout: 60)
    end

    it "leaves no live accept-loop thread behind" do
      server.start
      silent = TCPSocket.new("127.0.0.1", server.port)
      wait_for_parked_reader(server)
      thread = server.instance_variable_get(:@thread)

      server.stop
      silent.close

      expect(thread).not_to be_alive
    end
  end

  # Blocked in accept and blocked in the client read are both "sleep", so wait
  # on the backtrace instead — otherwise #stop's socket close would free the
  # loop and the kill path would never be exercised.
  def wait_for_parked_reader(server)
    thread = server.instance_variable_get(:@thread)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5

    sleep(0.01) until thread.backtrace.to_a.any? { |f| f.include?("read_request_line") } ||
                      Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  end
end
