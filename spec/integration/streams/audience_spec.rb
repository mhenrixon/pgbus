# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Integration test for server-side audience filtering. Two SSE clients
# subscribe to the same stream with different authorize-hook contexts;
# a broadcast labeled visible_to: :admin_only is delivered ONLY to the
# admin client, and an unlabeled broadcast is delivered to BOTH.
#
# Tests the full path:
#   1. Pgbus::Streams.filters.register at boot
#   2. Stream#broadcast(html, visible_to: :admin_only) wraps the label
#   3. PGMQ stores the wrapped JSON
#   4. Listener fires NOTIFY, Dispatcher's read_after reads the row
#   5. unwrap_stream_envelope extracts the visible_to label
#   6. visible_envelopes_for evaluates the predicate against each
#      Connection#context
#   7. Only matching connections receive the SSE frame
RSpec.describe "Streams: server-side audience filtering", :integration do
  before(:all) do
    @saved_listen_notify = Pgbus.configuration.listen_notify
    Pgbus.configuration.listen_notify = true
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    Pgbus.configuration.streams_listen_health_check_ms = 100
    Pgbus.configuration.streams_heartbeat_interval = 30
    Pgbus.configuration.streams_write_deadline_ms = 5_000
    Pgbus.reset_client!
    Pgbus::Streams.reset_filters!
    Pgbus::Streams.filters.register(:admin_only) { |ctx| ctx.is_a?(Hash) && ctx[:role] == "admin" }
  end

  after(:all) do
    Pgbus.configuration.listen_notify = @saved_listen_notify
    Pgbus.configuration.streams_signed_name_secret = nil
    Pgbus::Streams.reset_filters!
    Pgbus.reset_client!
  end

  let(:stream_name) { "aud_#{SecureRandom.hex(4)}" }

  let(:streamer) do
    Pgbus::Web::Streamer::Instance.new(
      client: Pgbus.client,
      config: Pgbus.configuration,
      pg_connection: build_pg_listen_connection,
      logger: Logger.new(IO::NULL)
    )
  end

  # Two stream apps, each with a different authorize hook simulating
  # an admin user vs. a viewer user. They share the same Streamer
  # instance so connections from both land in the same Registry.
  let(:admin_app) do
    Pgbus::Web::StreamApp.new(
      streamer: streamer,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL),
      authorize: ->(_env, _stream) { { role: "admin" } }
    )
  end

  let(:viewer_app) do
    Pgbus::Web::StreamApp.new(
      streamer: streamer,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL),
      authorize: ->(_env, _stream) { { role: "viewer" } }
    )
  end

  let(:admin_harness)  { SseTestSupport::PumaTestHarness.boot(rack_app: admin_app) }
  let(:viewer_harness) { SseTestSupport::PumaTestHarness.boot(rack_app: viewer_app) }

  before do
    Pgbus.client.ensure_stream_queue(stream_name)
    streamer.start
  end

  after do
    streamer.shutdown!
    admin_harness.shutdown if defined?(@admin_started)
    viewer_harness.shutdown if defined?(@viewer_started)
  end

  def signed(name)
    Pgbus::Streams::SignedName.sign(name)
  end

  def connect_admin(since_id:)
    @admin_started = true
    SseTestSupport::SseTestClient.connect(
      url: "#{admin_harness.url("/#{signed(stream_name)}")}?since=#{since_id}",
      timeout: 5
    )
  end

  def connect_viewer(since_id:)
    @viewer_started = true
    SseTestSupport::SseTestClient.connect(
      url: "#{viewer_harness.url("/#{signed(stream_name)}")}?since=#{since_id}",
      timeout: 5
    )
  end

  it "delivers admin-only broadcasts to the admin client and not the viewer" do
    stream = Pgbus.stream(stream_name)
    watermark = stream.current_msg_id

    admin = connect_admin(since_id: watermark)
    viewer = connect_viewer(since_id: watermark)

    # Two broadcasts: one public, one admin-only.
    stream.broadcast("<turbo-stream>public</turbo-stream>")
    stream.broadcast("<turbo-stream>secret</turbo-stream>", visible_to: :admin_only)

    admin_events = admin.wait_for_events(count: 2, timeout: 5)
    expect(admin_events.size).to eq(2)
    expect(admin_events.map(&:data)).to eq([
                                             "<turbo-stream>public</turbo-stream>",
                                             "<turbo-stream>secret</turbo-stream>"
                                           ])

    # Viewer waits for 2 events but only 1 is visible to them.
    viewer_events = viewer.wait_for_events(count: 2, timeout: 1)
    expect(viewer_events.size).to eq(1)
    expect(viewer_events.first.data).to eq("<turbo-stream>public</turbo-stream>")

    admin.close
    viewer.close
  end
end
