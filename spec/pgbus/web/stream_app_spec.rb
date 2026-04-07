# frozen_string_literal: true

require "spec_helper"
require "rack/request"
require "socket"

RSpec.describe Pgbus::Web::StreamApp do
  subject(:app) do
    described_class.new(
      streamer: streamer,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL)
    )
  end

  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
  end

  after do
    Pgbus.configuration.streams_signed_name_secret = nil
    Pgbus::Web::Streamer.reset!
  end

  let(:registry) { Pgbus::Web::Streamer::Registry.new }

  # Lightweight Streamer stand-in that exposes the same interface as
  # Pgbus::Web::Streamer::Instance but doesn't spawn any threads. The
  # StreamApp only needs #registry and #register — keep the test
  # hermetic and avoid booting listener/dispatcher/heartbeat.
  let(:streamer) do
    Class.new do
      attr_reader :registry, :registered

      def initialize(registry)
        @registry = registry
        @registered = []
      end

      def register(connection)
        @registered << connection
      end
    end.new(registry)
  end

  def signed(name)
    Pgbus::Streams::SignedName.sign(name)
  end

  def get_env(path_info, extra = {})
    {
      "REQUEST_METHOD" => "GET",
      "PATH_INFO" => path_info,
      "QUERY_STRING" => "",
      "rack.input" => StringIO.new,
      "rack.errors" => StringIO.new,
      "rack.url_scheme" => "http",
      "SERVER_NAME" => "example.com",
      "SERVER_PORT" => "80"
    }.merge(extra)
  end

  def drain_until(reader_io, needle, timeout: 1)
    buffer = +""
    deadline = Time.now + timeout
    until buffer.include?(needle) || Time.now > deadline
      begin
        buffer << reader_io.read_nonblock(4096)
      rescue IO::WaitReadable
        reader_io.wait_readable(0.1)
      end
    end
    buffer
  end

  describe "error paths (no hijack required)" do
    it "returns 404 when PATH_INFO has no signed name" do
      env = get_env("/pgbus/streams/")
      status, _, body = app.call(env)
      expect(status).to eq(404)
      expect(body.first).to include("missing signed stream name")
    end

    it "returns 404 when the signed name is tampered" do
      env = get_env("/pgbus/streams/not-a-valid-token")
      status, _, body = app.call(env)
      expect(status).to eq(404)
      expect(body.first).to include("invalid signed stream name")
    end

    it "returns 405-as-404 for non-GET requests" do
      env = get_env("/pgbus/streams/#{signed("chat")}", "REQUEST_METHOD" => "POST")
      status, = app.call(env)
      expect(status).to eq(404)
    end

    it "returns 403 when the authorize hook returns false" do
      restricted = described_class.new(
        streamer: streamer,
        config: Pgbus.configuration,
        logger: Logger.new(IO::NULL),
        authorize: ->(_env, _name) { false }
      )
      env = get_env("/pgbus/streams/#{signed("chat")}")
      status, _, body = restricted.call(env)
      expect(status).to eq(403)
      expect(body.first).to include("forbidden")
    end

    it "returns 400 for an invalid ?since= cursor" do
      env = get_env("/pgbus/streams/#{signed("chat")}", "QUERY_STRING" => "since=abc")
      status, _, body = app.call(env)
      expect(status).to eq(400)
      expect(body.first).to include("cursor")
    end

    it "returns 501 when rack.hijack? is missing (not on Puma 6.1+/Falcon)" do
      env = get_env("/pgbus/streams/#{signed("chat")}")
      # Intentionally do not set env["rack.hijack?"]
      status, _, body = app.call(env)
      expect(status).to eq(501)
      expect(body.first).to include("rack.hijack not available")
    end

    it "returns 503 when the streamer is at capacity" do
      Pgbus.configuration.streams_max_connections = 0 # every request is over capacity
      env = get_env("/pgbus/streams/#{signed("chat")}", "rack.hijack?" => true)

      status, headers, body = app.call(env)

      expect(status).to eq(503)
      expect(headers["retry-after"]).to eq("2")
      expect(body.first).to include("over capacity")
    ensure
      Pgbus.configuration.streams_max_connections = 2_000
    end

    it "returns 500 on an unexpected internal error" do
      allow(Pgbus::Streams::SignedName).to receive(:verify!).and_raise("boom")
      env = get_env("/pgbus/streams/#{signed("chat")}")
      status, = app.call(env)
      expect(status).to eq(500)
    end
  end

  describe "happy path with a real hijack" do
    let(:sockets)   { UNIXSocket.pair }
    let(:writer_io) { sockets[0] }
    let(:reader_io) { sockets[1] }

    def hijacked_env(query: "", headers: {})
      hijack_called = [false]
      env = get_env(
        "/pgbus/streams/#{signed("chat")}",
        {
          "QUERY_STRING" => query,
          "rack.hijack?" => true,
          "rack.hijack" => lambda {
            hijack_called[0] = true
            writer_io
          },
          "rack.hijack_io" => writer_io
        }.merge(headers)
      )
      [env, hijack_called]
    end

    after do
      writer_io.close unless writer_io.closed?
      reader_io.close unless reader_io.closed?
    end

    it "returns Puma's async sentinel and invokes rack.hijack" do
      env, hijack_called = hijacked_env
      status, _, body = app.call(env)
      expect(status).to eq(-1)
      expect(body).to eq([])
      expect(hijack_called[0]).to be true
    end

    it "writes the SSE response headers and the opening comment" do
      env, = hijacked_env
      app.call(env)
      drained = drain_until(reader_io, "pgbus stream open")
      expect(drained).to include("HTTP/1.1 200 OK")
      expect(drained).to include("content-type: text/event-stream")
      expect(drained).to include("retry: 2000")
      expect(drained).to include("stream=chat")
    end

    it "registers a Connection with the streamer" do
      env, = hijacked_env
      app.call(env)
      expect(streamer.registered.length).to eq(1)
      expect(streamer.registered.first.stream_name).to eq("chat")
    end

    it "carries the since_id through to the registered connection" do
      env, = hijacked_env(query: "since=1247")
      app.call(env)
      expect(streamer.registered.first.last_msg_id_sent).to eq(1247)
    end

    it "prefers Last-Event-ID over ?since= on reconnect" do
      env, = hijacked_env(query: "since=100", headers: { "HTTP_LAST_EVENT_ID" => "200" })
      app.call(env)
      expect(streamer.registered.first.last_msg_id_sent).to eq(200)
    end
  end
end
