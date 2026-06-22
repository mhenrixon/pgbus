# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Integration test for actor-echo suppression (issue #165). Two SSE
# clients subscribe to the same stream. The server sends each a
# `pgbus:connected` frame carrying its connection id. A broadcast that
# carries `exclude: <actor connection id>` is delivered to EVERY OTHER
# connection but NOT to the actor — the actor already applied the change
# via its action's HTTP response, so the SSE echo would double-apply.
#
# Tests the full path:
#   1. StreamApp mints a connection id and writes a pgbus:connected frame
#   2. The client reads its own connection id from that frame
#   3. Stream#broadcast(html, exclude: id) wraps the id in the payload
#   4. unwrap_stream_envelope extracts exclude
#   5. visible_envelopes_for skips the connection whose id == exclude
RSpec.describe "Streams: actor-echo suppression via exclude", :integration do
  before(:all) do
    @saved_listen_notify = Pgbus.configuration.listen_notify
    @saved_signed_name_secret = Pgbus.configuration.streams_signed_name_secret
    @saved_listen_health_check_ms = Pgbus.configuration.streams_listen_health_check_ms
    @saved_heartbeat_interval = Pgbus.configuration.streams_heartbeat_interval
    @saved_write_deadline_ms = Pgbus.configuration.streams_write_deadline_ms
    Pgbus.configuration.listen_notify = true
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    Pgbus.configuration.streams_listen_health_check_ms = 100
    Pgbus.configuration.streams_heartbeat_interval = 30
    Pgbus.configuration.streams_write_deadline_ms = 5_000
    Pgbus.reset_client!
  end

  after(:all) do
    Pgbus.configuration.listen_notify = @saved_listen_notify
    Pgbus.configuration.streams_signed_name_secret = @saved_signed_name_secret
    Pgbus.configuration.streams_listen_health_check_ms = @saved_listen_health_check_ms
    Pgbus.configuration.streams_heartbeat_interval = @saved_heartbeat_interval
    Pgbus.configuration.streams_write_deadline_ms = @saved_write_deadline_ms
    Pgbus.reset_client!
  end

  let(:stream_name) { "excl_#{SecureRandom.hex(4)}" }

  let(:streamer) do
    Pgbus::Web::Streamer::Instance.new(
      client: Pgbus.client,
      config: Pgbus.configuration,
      pg_connection: build_pg_listen_connection,
      logger: Logger.new(IO::NULL)
    )
  end

  let(:app) do
    Pgbus::Web::StreamApp.new(
      streamer: streamer,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL),
      authorize: ->(_env, _stream) { true }
    )
  end

  let(:harness) { SseTestSupport::PumaTestHarness.boot(rack_app: app) }

  before do
    Pgbus.client.ensure_stream_queue(stream_name)
    streamer.start
  end

  after do
    streamer.shutdown!
    @booted&.shutdown
  end

  def signed(name)
    Pgbus::Streams::SignedName.sign(name)
  end

  def connect(since_id:)
    @booted = harness
    SseTestSupport::SseTestClient.connect(
      url: "#{@booted.url("/#{signed(stream_name)}")}?since=#{since_id}",
      timeout: 5
    )
  end

  it "exposes a per-connection id via the pgbus:connected frame" do
    client = connect(since_id: Pgbus.stream(stream_name).current_msg_id)
    connection_id = client.wait_for_connection_id(timeout: 5)

    expect(connection_id).to be_a(String)
    expect(connection_id).not_to be_empty
    client.close
  end

  it "skips delivery to the excluded actor but delivers to the bystander" do
    stream = Pgbus.stream(stream_name)
    watermark = stream.current_msg_id

    actor = connect(since_id: watermark)
    bystander = connect(since_id: watermark)

    actor_id = actor.wait_for_connection_id(timeout: 5)
    bystander_id = bystander.wait_for_connection_id(timeout: 5)
    expect(actor_id).not_to eq(bystander_id) # distinct ids per connection

    # The actor broadcasts, excluding its own connection.
    stream.broadcast("<turbo-stream>typed-by-actor</turbo-stream>", exclude: actor_id)

    # Bystander receives it.
    bystander_events = bystander.wait_for_events(count: 1, timeout: 5)
    expect(bystander_events.size).to eq(1)
    expect(bystander_events.first.data).to eq("<turbo-stream>typed-by-actor</turbo-stream>")

    # Actor does NOT (its data-event list stays empty).
    expect(actor.wait_for_quiet(seconds: 0.5)).to be(true)
    expect(actor.events).to be_empty

    actor.close
    bystander.close
  end

  it "delivers to both when no exclude is set" do
    stream = Pgbus.stream(stream_name)
    watermark = stream.current_msg_id

    a = connect(since_id: watermark)
    b = connect(since_id: watermark)
    a.wait_for_connection_id(timeout: 5)
    b.wait_for_connection_id(timeout: 5)

    stream.broadcast("<turbo-stream>everyone</turbo-stream>")

    expect(a.wait_for_events(count: 1, timeout: 5).first.data).to eq("<turbo-stream>everyone</turbo-stream>")
    expect(b.wait_for_events(count: 1, timeout: 5).first.data).to eq("<turbo-stream>everyone</turbo-stream>")

    a.close
    b.close
  end
end
