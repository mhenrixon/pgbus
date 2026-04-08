# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# THE headline integration test.
#
# This spec boots a real Puma server, a real Pgbus::Web::Streamer::Instance
# with a real PG LISTEN connection, and a real SSE client over real TCP,
# and proves that rails/rails#52420 (page-born-stale) is fixed.
#
# The race being exercised:
#
#   t=0    controller renders, captures watermark = MAX(msg_id) = W
#   t=1    response goes out
#   t=2    BEFORE the client connects, a broadcast publishes msg_id = W+1
#   t=3    client connects with ?since=W
#   t=4    server replays from PGMQ where msg_id > W — delivers W+1
#
# Without the watermark + replay, t=2 would be silently lost because
# no SSE client was yet subscribed to the stream. The headline assertion
# of this whole project is: that broadcast lands on the client exactly
# once, via the replay path, a few hundred milliseconds later than
# if it had been a live broadcast.
#
# If this spec fails, the whole turbo-streams subsystem is broken.
RSpec.describe "Streams: page-born-stale race fix", :integration do
  # The integration_helper sets listen_notify: false (intended for the
  # existing worker-level integration specs which don't use it). The
  # streamer subsystem requires listen_notify: true. Flip it here and
  # restore it after each test so we don't contaminate sibling specs.
  before(:all) do
    @saved_listen_notify = Pgbus.configuration.listen_notify
    Pgbus.configuration.listen_notify = true
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    Pgbus.configuration.streams_listen_health_check_ms = 50
    Pgbus.configuration.streams_heartbeat_interval = 30
    Pgbus.configuration.streams_write_deadline_ms = 5_000
    # Force a fresh client with the new listen_notify setting
    Pgbus.reset_client!
  end

  after(:all) do
    Pgbus.configuration.listen_notify = @saved_listen_notify
    Pgbus.configuration.streams_signed_name_secret = nil
    Pgbus.reset_client!
  end

  let(:stream_name) { "pbns_#{SecureRandom.hex(4)}" }

  let(:streamer) do
    pg_conn = build_pg_listen_connection
    Pgbus::Web::Streamer::Instance.new(
      client: Pgbus.client,
      config: Pgbus.configuration,
      pg_connection: pg_conn,
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
    harness.shutdown if @harness_started
  end

  def signed(name)
    Pgbus::Streams::SignedName.sign(name)
  end

  def connect_sse_client(since_id:, extra_headers: {})
    url = "#{harness.url("/#{signed(stream_name)}")}?since=#{since_id}"
    @harness_started = true
    SseTestSupport::SseTestClient.connect(url: url, headers: extra_headers, timeout: 5)
  end

  it "delivers broadcasts published in the gap between render and connect" do
    stream = Pgbus.stream(stream_name)

    # === Step 1: simulate the controller render by capturing the watermark ===
    rendered_watermark = stream.current_msg_id
    expect(rendered_watermark).to be >= 0

    # === Step 2: the RACE — broadcast TWICE before any client connects ===
    stream.broadcast('<turbo-stream action="replace" target="x">A</turbo-stream>')
    stream.broadcast('<turbo-stream action="replace" target="x">B</turbo-stream>')

    # Both broadcasts are now in PGMQ at msg_ids > rendered_watermark.
    # No SSE client has ever connected. Without the watermark + replay
    # logic in Dispatcher#handle_connect, both messages would be lost
    # the moment a client connected.

    # === Step 3: simulate the client connecting AFTER the broadcasts ===
    client = connect_sse_client(since_id: rendered_watermark)
    events = client.wait_for_events(count: 2, timeout: 5)

    # === ASSERTION 1: both gap-period broadcasts were delivered ===
    expect(events.size).to eq(2)
    expect(events.map(&:data)).to eq([
                                       '<turbo-stream action="replace" target="x">A</turbo-stream>',
                                       '<turbo-stream action="replace" target="x">B</turbo-stream>'
                                     ])

    # === ASSERTION 2: ids are strictly monotonic and above the watermark ===
    ids = events.map { |e| e.id.to_i }
    expect(ids).to all(be > rendered_watermark)
    expect(ids).to eq(ids.sort)

    # === ASSERTION 3: a broadcast AFTER connect arrives via the live path ===
    stream.broadcast('<turbo-stream action="replace" target="x">C</turbo-stream>')
    live_events = client.wait_for_events(count: 3, timeout: 5)
    expect(live_events.size).to eq(3)
    expect(live_events.last.data).to include(">C<")
    expect(live_events.last.id.to_i).to be > events.last.id.to_i

    client.close
  end
end
