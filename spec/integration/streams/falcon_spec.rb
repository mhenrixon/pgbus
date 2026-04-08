# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/falcon_test_harness"
require_relative "../../support/sse_test_client"

# Integration test that the turbo-streams subsystem works on Falcon.
# Phase 4 was designed around Puma's rack.hijack semantics (puma/puma#1009),
# but the research also noted that Falcon's protocol-rack adapter sets
# env["rack.hijack?"] = true the same way Puma does. This spec proves
# that equivalence end-to-end: the SAME StreamApp Rack app is driven
# by a FalconTestHarness instead of a PumaTestHarness, and the headline
# page-born-stale test passes with no code changes.
#
# If this spec fails, the "Puma 6.1+ only" claim in the README is
# actually accurate and we need to either ship a Falcon-specific code
# path in StreamApp or document Falcon as unsupported. If this spec
# passes, Falcon just works via the existing hijack path and the README
# should be updated.
RSpec.describe "Streams: Falcon server compatibility", :integration do
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

  let(:stream_name) { "fal_#{SecureRandom.hex(4)}" }

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

  let(:harness) { SseTestSupport::FalconTestHarness.boot(rack_app: stream_app) }

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

  it "delivers broadcasts through Falcon's hijack path" do
    stream = Pgbus.stream(stream_name)

    rendered_watermark = stream.current_msg_id
    stream.broadcast("<turbo-stream>A</turbo-stream>")
    stream.broadcast("<turbo-stream>B</turbo-stream>")

    client = connect_sse_client(since_id: rendered_watermark)
    events = client.wait_for_events(count: 2, timeout: 5)

    expect(events.size).to eq(2)
    expect(events.map(&:data)).to eq([
                                       "<turbo-stream>A</turbo-stream>",
                                       "<turbo-stream>B</turbo-stream>"
                                     ])

    stream.broadcast("<turbo-stream>C</turbo-stream>")
    live_events = client.wait_for_events(count: 3, timeout: 5)
    expect(live_events.size).to eq(3)
    expect(live_events.last.data).to include(">C<")

    client.close
  end
end
