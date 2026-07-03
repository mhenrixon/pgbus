# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Metrics::PrometheusExporter do
  subject(:app) { described_class.new(backend: backend) }

  let(:backend) { Pgbus::Metrics::Backends::Prometheus.new }

  def get(path = "/")
    app.call("REQUEST_METHOD" => "GET", "PATH_INFO" => path)
  end

  it "returns 200 with the Prometheus content type" do
    backend.increment("pgbus_messages_sent", 1, { queue: "default" })

    status, headers, body = get
    expect(status).to eq(200)
    expect(headers["content-type"]).to eq("text/plain; version=0.0.4")
    expect(body.join).to include('pgbus_messages_sent{queue="default"} 1')
  end

  it "returns the exposition text even when the registry is empty" do
    status, _headers, body = get
    expect(status).to eq(200)
    expect(body.join).to eq("\n")
  end

  context "when no explicit backend is injected" do
    subject(:app) { described_class.new }

    it "reads the configured Prometheus backend" do
      configured = Pgbus::Metrics::Backends::Prometheus.new
      configured.increment("pgbus_messages_sent", 7, { queue: "q" })
      allow(Pgbus.configuration).to receive(:metrics_backend).and_return(configured)

      _status, _headers, body = get
      expect(body.join).to include('pgbus_messages_sent{queue="q"} 7')
    end

    it "returns 503 when the configured backend is not a Prometheus backend" do
      allow(Pgbus.configuration).to receive(:metrics_backend).and_return(:statsd)

      status, _headers, body = get
      expect(status).to eq(503)
      expect(body.join).to match(/prometheus/i)
    end
  end
end
