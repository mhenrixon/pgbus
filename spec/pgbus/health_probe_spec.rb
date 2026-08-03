# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::HealthProbe do
  let(:out) { StringIO.new }
  let(:err) { StringIO.new }

  def run_probe(argv, env: {})
    described_class.run(argv, env: env, out: out, err: err)
  end

  # A real HealthServer bound to an OS-assigned port, answering with a fixed
  # Rack response — the probe speaks actual HTTP to an actual socket.
  def with_server(status)
    app = ->(_env) { [status, { "Content-Type" => "application/json" }, ["{}"]] }
    server = Pgbus::Web::HealthServer.new(port: 0, app: app)
    server.start
    yield server.port
  ensure
    server&.stop
  end

  it "exits 0 when the endpoint answers 200" do
    with_server(200) do |port|
      expect(run_probe(["--port", port.to_s])).to eq(described_class::EXIT_OK)
    end
  end

  it "exits 1 when the endpoint answers 503" do
    with_server(503) do |port|
      expect(run_probe(["--port", port.to_s])).to eq(described_class::EXIT_UNHEALTHY)
    end
  end

  it "exits 1 when nothing listens on the port" do
    # Bind then release a port so we hold a port number that refuses connections.
    server = TCPServer.new("127.0.0.1", 0)
    port = server.addr[1]
    server.close

    expect(run_probe(["--port", port.to_s])).to eq(described_class::EXIT_UNHEALTHY)
  end

  it "reads the port from PGBUS_HEALTH_PORT when no flag is given" do
    with_server(200) do |port|
      expect(run_probe([], env: { "PGBUS_HEALTH_PORT" => port.to_s })).to eq(described_class::EXIT_OK)
    end
  end

  it "probes the given --path" do
    captured_path = nil
    app = lambda do |env|
      captured_path = env["PATH_INFO"]
      [200, { "Content-Type" => "text/plain" }, ["ok"]]
    end
    server = Pgbus::Web::HealthServer.new(port: 0, app: app)
    server.start

    run_probe(["--port", server.port.to_s, "--path", "/livez"])

    expect(captured_path).to eq("/livez")
  ensure
    server&.stop
  end

  it "defaults the path to /readyz" do
    captured_path = nil
    app = lambda do |env|
      captured_path = env["PATH_INFO"]
      [200, { "Content-Type" => "text/plain" }, ["ok"]]
    end
    server = Pgbus::Web::HealthServer.new(port: 0, app: app)
    server.start

    run_probe(["--port", server.port.to_s])

    expect(captured_path).to eq("/readyz")
  ensure
    server&.stop
  end

  it "exits 2 with usage on stderr when no port is available anywhere" do
    expect(run_probe([])).to eq(described_class::EXIT_USAGE)
    expect(err.string).to include("--port")
  end

  it "exits 2 on a non-numeric port" do
    expect(run_probe(["--port", "banana"])).to eq(described_class::EXIT_USAGE)
  end

  # The whole point of the probe: a docker HEALTHCHECK runs it every few
  # seconds, so it must never drag in Bundler, Zeitwerk, or the pgbus gem.
  it "loads standalone without pulling in the gem" do
    script = 'require_relative "lib/pgbus/health_probe"; ' \
             "exit(defined?(Pgbus::Client) || defined?(Zeitwerk) || defined?(Pgbus::Web) ? 1 : 0)"
    result = system(RbConfig.ruby, "--disable-gems", "-e", script,
                    chdir: File.expand_path("../..", __dir__))

    expect(result).to be true
  end
end
