# frozen_string_literal: true

require "json"
require "rack/mock"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::RackApp do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  let(:json_headers) do
    { "CONTENT_TYPE" => "application/json", "HTTP_ACCEPT" => "application/json" }
  end

  let(:health_call) do
    { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "pgbus_health", arguments: {} } }.to_json
  end

  before do
    allow(data_source).to receive_messages(queues_with_metrics: [], processes: [], queue_health_stats: {}, stream_queue_names: Set.new)
  end

  def mock_for(**kwargs)
    Rack::MockRequest.new(described_class.new(data_source: data_source, **kwargs))
  end

  describe "responds to #call as a Rack app" do
    it "answers a tools/list POST with the curated tool set" do
      app = mock_for
      body = { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} }.to_json

      response = app.post("/", input: body, **json_headers)
      names = JSON.parse(response.body).dig("result", "tools").map { |t| t["name"] }

      expect(response.status).to eq(200)
      expect(names).to include("pgbus_health", "pgbus_queues", "pgbus_processes")
    end

    it "answers a tools/call POST with a verdict from the injected data source" do
      app = mock_for
      response = app.post("/", input: health_call, **json_headers)

      inner = JSON.parse(JSON.parse(response.body).dig("result", "content", 0, "text"))
      expect(response.status).to eq(200)
      expect(inner["status"]).to eq("OK")
    end

    it "returns 406 when the client omits Accept: application/json (documented gotcha)" do
      app = mock_for
      response = app.post("/", input: health_call, "CONTENT_TYPE" => "application/json")

      expect(response.status).to eq(406)
    end
  end

  describe "token gate" do
    it "rejects requests with no Authorization header" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, **json_headers)

      expect(response.status).to eq(401)
      expect(JSON.parse(response.body).dig("error", "message")).to eq("Unauthorized")
    end

    it "rejects a wrong bearer token" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, "HTTP_AUTHORIZATION" => "Bearer nope", **json_headers)

      expect(response.status).to eq(401)
    end

    it "rejects a non-bearer Authorization scheme" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, "HTTP_AUTHORIZATION" => "Basic s3cret", **json_headers)

      expect(response.status).to eq(401)
    end

    it "allows a matching bearer token" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, "HTTP_AUTHORIZATION" => "Bearer s3cret", **json_headers)

      expect(response.status).to eq(200)
    end

    it "sends a WWW-Authenticate: Bearer challenge on 401" do
      app = mock_for(token: "s3cret")
      response = app.post("/", input: health_call, **json_headers)

      expect(response.headers["WWW-Authenticate"]).to eq("Bearer")
    end

    it "returns a fresh, mutable response array on each auth failure" do
      app = described_class.new(data_source: data_source, token: "s3cret")
      env = Rack::MockRequest.env_for("/", input: health_call, **json_headers)

      first = app.call(env)
      second = app.call(Rack::MockRequest.env_for("/", input: health_call, **json_headers))

      expect(first).not_to be_frozen
      expect(first[1]).not_to be_frozen
      expect(first).not_to equal(second)
    end

    # Regression for #304: the frozen UNAUTHORIZED triple raised FrozenError once
    # a downstream, response-mutating middleware ran — turning 401 into 500. Drive
    # the app through the real Rack::TempfileReaper (assigns response[2]) and
    # Rack::ETag (adds a header) to catch it the way Rails does.
    it "answers 401 through response-mutating middleware without a FrozenError" do
      require "rack/tempfile_reaper"
      require "rack/etag"
      stack = Rack::TempfileReaper.new(Rack::ETag.new(described_class.new(data_source: data_source, token: "s3cret")))

      response = Rack::MockRequest.new(stack).post("/", input: health_call, **json_headers)

      expect(response.status).to eq(401)
      expect(JSON.parse(response.body).dig("error", "message")).to eq("Unauthorized")
    end
  end

  describe "custom auth callable" do
    it "delegates to the auth proc, which receives a Rack::Request" do
      seen = nil
      auth = lambda do |req|
        seen = req
        req.get_header("HTTP_X_OK") == "yes"
      end
      app = mock_for(auth: auth)

      ok = app.post("/", input: health_call, "HTTP_X_OK" => "yes", **json_headers)
      denied = app.post("/", input: health_call, **json_headers)

      expect(ok.status).to eq(200)
      expect(denied.status).to eq(401)
      expect(seen).to be_a(Rack::Request)
    end

    it "takes precedence over token when both are given" do
      app = mock_for(token: "s3cret", auth: ->(_req) { true })
      # No bearer token sent, but the auth proc allows it.
      response = app.post("/", input: health_call, **json_headers)

      expect(response.status).to eq(200)
    end
  end

  describe "unauthenticated mounting" do
    it "warns when neither token nor auth is configured" do
      allow(Pgbus.logger).to receive(:warn)

      described_class.new(data_source: data_source)

      expect(Pgbus.logger).to have_received(:warn)
    end

    it "does not warn when a token is configured" do
      allow(Pgbus.logger).to receive(:warn)

      described_class.new(data_source: data_source, token: "s3cret")

      expect(Pgbus.logger).not_to have_received(:warn)
    end
  end

  describe "payload redaction over HTTP" do
    let(:rows) { [{ msg_id: 1, read_ct: 0, message: "{\"pii\":\"x\"}", headers: "{}" }] }

    def jobs_call(include_payloads)
      {
        jsonrpc: "2.0", id: 9, method: "tools/call",
        params: { name: "pgbus_jobs", arguments: { queue: "pgbus_default", include_payloads: include_payloads } }
      }.to_json
    end

    before { allow(data_source).to receive(:jobs).and_return(rows) }

    it "redacts message bodies by default even when include_payloads is requested" do
      app = mock_for
      response = app.post("/", input: jobs_call(true), **json_headers)

      inner = JSON.parse(JSON.parse(response.body).dig("result", "content", 0, "text"))
      expect(inner["jobs"].first["message"]).to eq(Pgbus::MCP::Redactor::REDACTED)
    end

    it "returns payloads when the app allows them and include_payloads is set" do
      app = mock_for(allow_payloads: true)
      response = app.post("/", input: jobs_call(true), **json_headers)

      inner = JSON.parse(JSON.parse(response.body).dig("result", "content", 0, "text"))
      expect(inner["jobs"].first["message"]).to eq("{\"pii\":\"x\"}")
    end
  end

  describe "Pgbus::MCP.rack_app" do
    it "returns a RackApp instance" do
      app = Pgbus::MCP.rack_app(data_source: data_source, token: "s3cret")
      expect(app).to be_a(described_class)
    end
  end
end
