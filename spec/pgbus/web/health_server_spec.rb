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
end
