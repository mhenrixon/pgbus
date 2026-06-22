# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Integration test for connection-driven presence (issue #169). With a
# stream configured via streams_presence_patterns and an authorize hook
# that returns a member context, the streamer auto-joins on SSE connect,
# touches on the keepalive heartbeat, and auto-leaves on disconnect — no
# explicit join/leave calls from the app.
#
# Exercises the full path:
#   1. StreamApp authorize hook returns { member_id:, metadata: }
#   2. Dispatcher#handle_connect calls presence.join (auto)
#   3. Heartbeat posts PresenceTouchMessage; dispatcher touches
#   4. Client disconnect → Dispatcher#handle_disconnect calls presence.leave
RSpec.describe "Streams: connection-driven presence", :integration do
  before(:all) do
    @saved_listen_notify = Pgbus.configuration.listen_notify
    @saved_presence_patterns = Pgbus.configuration.streams_presence_patterns
    @saved_heartbeat = Pgbus.configuration.streams_heartbeat_interval
    Pgbus.configuration.listen_notify = true
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    Pgbus.configuration.streams_listen_health_check_ms = 100
    # Short heartbeat so the touch fires within the test window.
    Pgbus.configuration.streams_heartbeat_interval = 0.3
    Pgbus.configuration.streams_write_deadline_ms = 5_000
    # Every stream in this file is a presence stream.
    Pgbus.configuration.streams_presence_patterns = [/./]
    Pgbus.reset_client!
  end

  after(:all) do
    Pgbus.configuration.listen_notify = @saved_listen_notify
    Pgbus.configuration.streams_presence_patterns = @saved_presence_patterns
    Pgbus.configuration.streams_heartbeat_interval = @saved_heartbeat
    Pgbus.configuration.streams_signed_name_secret = nil
    Pgbus.reset_client!
  end

  let(:stream_name) { "autopres_#{SecureRandom.hex(4)}" }

  let(:streamer) do
    Pgbus::Web::Streamer::Instance.new(
      client: Pgbus.client,
      config: Pgbus.configuration,
      pg_connection: build_pg_listen_connection,
      logger: Logger.new(IO::NULL)
    )
  end

  # Authorize hook returns a member context: a Hash with :member_id.
  let(:stream_app) do
    Pgbus::Web::StreamApp.new(
      streamer: streamer,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL),
      authorize: ->(_env, _stream) { { member_id: "user-42", metadata: { name: "Dana" } } }
    )
  end

  let(:harness) { SseTestSupport::PumaTestHarness.boot(rack_app: stream_app) }

  before do
    Pgbus.client.ensure_stream_queue(stream_name)
    streamer.start
  end

  after do
    streamer.shutdown!
    @harness_started&.shutdown
  end

  def signed(name)
    Pgbus::Streams::SignedName.sign(name)
  end

  def connect_sse_client
    @harness_started = harness
    SseTestSupport::SseTestClient.connect(
      url: "#{@harness_started.url("/#{signed(stream_name)}")}?since=0",
      timeout: 5
    )
  end

  def wait_until(timeout: 5)
    deadline = Time.now + timeout
    sleep 0.02 until yield || Time.now > deadline
    yield
  end

  # rubocop:disable Style/CollectionQuerying -- Presence#count is a SQL
  # COUNT(*), not a collection query; .one?/.none? are not defined on it.
  it "auto-joins the member on connect and auto-leaves on disconnect" do
    presence = Pgbus.stream(stream_name).presence
    client = connect_sse_client

    # Auto-join happens on the dispatcher thread shortly after connect.
    expect(wait_until { presence.count == 1 }).to be(true)
    member = presence.members.first
    expect(member["id"]).to eq("user-42")
    expect(member["metadata"]).to eq("name" => "Dana")

    client.close

    # Auto-leave happens on disconnect (heartbeat marks the socket dead,
    # dispatcher fires handle_disconnect → presence.leave).
    expect(wait_until(timeout: 8) { presence.count.zero? }).to be(true)
  end

  it "touches the member on the heartbeat so it survives a sweep cutoff" do
    presence = Pgbus.stream(stream_name).presence
    client = connect_sse_client
    expect(wait_until { presence.count == 1 }).to be(true)

    # Let at least one heartbeat tick (interval 0.3s) run a touch.
    sleep 0.5

    server_cutoff = ActiveRecord::Base.connection.raw_connection
                                      .exec("SELECT NOW() - INTERVAL '200 milliseconds' AS cutoff")
                                      .first["cutoff"]
    expect(presence.sweep!(older_than: server_cutoff)).to eq(0)
    expect(presence.count).to eq(1)

    client.close
  end
  # rubocop:enable Style/CollectionQuerying
end
