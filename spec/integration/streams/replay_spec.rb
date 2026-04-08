# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Integration test for the `broadcasts_with_replay` feature: chat-history-
# as-a-stream. The `replay:` option on pgbus_stream_from controls what
# the initial connect replays:
#
#   :watermark (default) — only post-render broadcasts (page-born-stale fix)
#   :all                 — every message still in PGMQ retention
#   N (Integer)          — the last N messages
#
# The test exercises all three on the same stream to prove they compute
# different since_id cursors and the streamer replays the right subset.
RSpec.describe "Streams: replay option", :integration do
  before(:all) do
    @saved_listen_notify = Pgbus.configuration.listen_notify
    Pgbus.configuration.listen_notify = true
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    Pgbus.configuration.streams_listen_health_check_ms = 100
    Pgbus.configuration.streams_heartbeat_interval = 30
    Pgbus.configuration.streams_write_deadline_ms = 5_000
    Pgbus.reset_client!
  end

  after(:all) do
    Pgbus.configuration.listen_notify = @saved_listen_notify
    Pgbus.configuration.streams_signed_name_secret = nil
    Pgbus.reset_client!
  end

  let(:stream_name) { "rpl_#{SecureRandom.hex(4)}" }

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

  def build_pg_listen_connection
    require "pg"
    uri = URI.parse(PGBUS_DATABASE_URL)
    PG.connect(
      host: uri.host || "localhost",
      port: (uri.port || 5432).to_i,
      dbname: uri.path.delete_prefix("/"),
      user: uri.user || ENV.fetch("USER")
    )
  end

  def connect_sse_client(since_id:)
    @harness_started = true
    url = "#{harness.url("/#{signed(stream_name)}")}?since=#{since_id}"
    SseTestSupport::SseTestClient.connect(url: url, timeout: 5)
  end

  it "delivers the full backlog with replay: :all" do
    stream = Pgbus.stream(stream_name)

    stream.broadcast("<turbo-stream>backlog-1</turbo-stream>")
    stream.broadcast("<turbo-stream>backlog-2</turbo-stream>")
    stream.broadcast("<turbo-stream>backlog-3</turbo-stream>")

    # replay: :all computes since_id = 0 in the helper. Simulate that
    # by passing 0 directly to the SSE client.
    client = connect_sse_client(since_id: 0)
    events = client.wait_for_events(count: 3, timeout: 5)

    expect(events.size).to eq(3)
    expect(events.map(&:data)).to eq([
                                       "<turbo-stream>backlog-1</turbo-stream>",
                                       "<turbo-stream>backlog-2</turbo-stream>",
                                       "<turbo-stream>backlog-3</turbo-stream>"
                                     ])

    client.close
  end

  it "delivers only the last N messages with replay: N" do
    stream = Pgbus.stream(stream_name)

    stream.broadcast("<turbo-stream>old-1</turbo-stream>")
    stream.broadcast("<turbo-stream>old-2</turbo-stream>")
    stream.broadcast("<turbo-stream>old-3</turbo-stream>")
    stream.broadcast("<turbo-stream>keep-1</turbo-stream>")
    stream.broadcast("<turbo-stream>keep-2</turbo-stream>")

    # replay: 2 computes since_id = current_msg_id - 2 = 5 - 2 = 3
    # (msg_ids start at 1, so after 5 broadcasts current_msg_id is 5).
    # The streamer replays msg_id > 3, which is msg 4 and msg 5 —
    # the last two messages.
    watermark = stream.current_msg_id
    since_id = watermark - 2

    client = connect_sse_client(since_id: since_id)
    events = client.wait_for_events(count: 2, timeout: 5)

    expect(events.size).to eq(2)
    expect(events.map(&:data)).to eq([
                                       "<turbo-stream>keep-1</turbo-stream>",
                                       "<turbo-stream>keep-2</turbo-stream>"
                                     ])

    client.close
  end

  it "delivers nothing with replay: :watermark (current default) when no post-render broadcasts" do
    stream = Pgbus.stream(stream_name)

    stream.broadcast("<turbo-stream>old-1</turbo-stream>")
    stream.broadcast("<turbo-stream>old-2</turbo-stream>")

    # replay: :watermark computes since_id = current_msg_id = 2.
    # No broadcasts after that, so the client sees nothing on connect.
    watermark = stream.current_msg_id
    client = connect_sse_client(since_id: watermark)

    sleep 0.3
    expect(client.events).to be_empty

    client.close
  end

  it "correctly caps replay: N when N > message count (clamps to 0)" do
    stream = Pgbus.stream(stream_name)

    stream.broadcast("<turbo-stream>only-1</turbo-stream>")
    stream.broadcast("<turbo-stream>only-2</turbo-stream>")

    # Simulating replay: 1000 on a queue with only 2 messages: the
    # helper clamps to max(2 - 1000, 0) = 0, so the client gets both.
    client = connect_sse_client(since_id: 0)
    events = client.wait_for_events(count: 2, timeout: 5)

    expect(events.size).to eq(2)
    expect(events.map(&:data)).to eq([
                                       "<turbo-stream>only-1</turbo-stream>",
                                       "<turbo-stream>only-2</turbo-stream>"
                                     ])

    client.close
  end
end
