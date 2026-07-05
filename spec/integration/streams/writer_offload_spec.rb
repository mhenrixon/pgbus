# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Integration test for the off-thread durable fanout writer (issue #321).
# Proves the real Connection + IoWriter + socket path delivers durable
# broadcasts correctly when streams_writer_threads > 0 — the fanout write runs
# on a writer-pool thread, the dispatcher advances the cursor on the ack, and
# the SSE client receives every frame in order. The default-off path is covered
# by the rest of the streams integration suite.
RSpec.describe "Streams: writer offload delivery", :integration do
  before(:all) do
    @saved_listen_notify = Pgbus.configuration.listen_notify
    @saved_writer_threads = Pgbus.configuration.streams_writer_threads
    Pgbus.configuration.listen_notify = true
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    Pgbus.configuration.streams_listen_health_check_ms = 100
    Pgbus.configuration.streams_heartbeat_interval = 30
    Pgbus.configuration.streams_write_deadline_ms = 5_000
    Pgbus.configuration.streams_writer_threads = 2 # OFFLOAD ON
    Pgbus.reset_client!
  end

  after(:all) do
    Pgbus.configuration.listen_notify = @saved_listen_notify
    Pgbus.configuration.streams_writer_threads = @saved_writer_threads
    Pgbus.configuration.streams_signed_name_secret = nil
    Pgbus.reset_client!
  end

  let(:stream_name) { "wro_#{SecureRandom.hex(4)}" }

  let(:streamer) do
    Pgbus::Web::Streamer::Instance.new(
      client: Pgbus.client,
      config: Pgbus.configuration,
      pg_connection: build_pg_listen_connection,
      logger: Logger.new(IO::NULL)
    )
  end

  let(:stream_app) do
    Pgbus::Web::StreamApp.new(
      streamer: streamer,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL)
    )
  end

  let(:harness) { SseTestSupport::PumaTestHarness.boot(rack_app: stream_app) }

  before do
    Pgbus.client.ensure_stream_queue(stream_name)
    streamer.start
  end

  after do
    streamer.shutdown!
    harness.shutdown if defined?(@harness_started)
  end

  def signed(name)
    Pgbus::Streams::SignedName.sign(name)
  end

  def connect_sse_client(since_id:)
    @harness_started = true
    url = "#{harness.url("/#{signed(stream_name)}")}?since=#{since_id}"
    SseTestSupport::SseTestClient.connect(url: url, timeout: 5)
  end

  it "builds a real OutboundPump for the streamer" do
    expect(streamer.pump).to be_a(Pgbus::Web::Streamer::OutboundPump)
  end

  it "delivers a live durable broadcast through the writer pool, in order" do
    client = connect_sse_client(since_id: 0)
    client.wait_for_connection_id(timeout: 5)

    stream = Pgbus.stream(stream_name)
    stream.broadcast("<turbo-stream>a</turbo-stream>")
    stream.broadcast("<turbo-stream>b</turbo-stream>")
    stream.broadcast("<turbo-stream>c</turbo-stream>")

    events = client.wait_for_events(count: 3, timeout: 5)

    expect(events.map(&:data)).to eq([
                                       "<turbo-stream>a</turbo-stream>",
                                       "<turbo-stream>b</turbo-stream>",
                                       "<turbo-stream>c</turbo-stream>"
                                     ])
    client.close
  end

  it "replays the backlog on connect (connect-replay stays inline under offload)" do
    stream = Pgbus.stream(stream_name)
    stream.broadcast("<turbo-stream>backlog-1</turbo-stream>")
    stream.broadcast("<turbo-stream>backlog-2</turbo-stream>")

    client = connect_sse_client(since_id: 0)
    events = client.wait_for_events(count: 2, timeout: 5)

    expect(events.map(&:data)).to eq([
                                       "<turbo-stream>backlog-1</turbo-stream>",
                                       "<turbo-stream>backlog-2</turbo-stream>"
                                     ])
    client.close
  end
end
