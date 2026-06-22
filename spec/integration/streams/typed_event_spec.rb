# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Integration test for typed SSE event names (issue #170). A broadcast can
# set the SSE `event:` field (e.g. event: "presence") while keeping the
# payload a Turbo Stream. Default broadcasts still arrive as turbo-stream.
RSpec.describe "Streams: typed SSE event names", :integration do
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

  let(:stream_name) { "typed_#{SecureRandom.hex(4)}" }

  let(:streamer) do
    Pgbus::Web::Streamer::Instance.new(
      client: Pgbus.client,
      config: Pgbus.configuration,
      pg_connection: build_pg_listen_connection,
      logger: Logger.new(IO::NULL)
    )
  end

  let(:app) do
    Pgbus::Web::StreamApp.new(streamer: streamer, config: Pgbus.configuration, logger: Logger.new(IO::NULL))
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

  def connect
    @booted = harness
    SseTestSupport::SseTestClient.connect(
      url: "#{@booted.url("/#{signed(stream_name)}")}?since=#{Pgbus.stream(stream_name).current_msg_id}",
      timeout: 5
    )
  end

  it "delivers a frame with the typed SSE event name and the Turbo Stream payload" do
    stream = Pgbus.stream(stream_name)
    client = connect

    stream.broadcast("<turbo-stream>presence-pill</turbo-stream>", event: "presence")

    events = client.wait_for_events(count: 1, timeout: 5)
    expect(events.size).to eq(1)
    expect(events.first.event).to eq("presence")
    expect(events.first.data).to eq("<turbo-stream>presence-pill</turbo-stream>")

    client.close
  end

  it "delivers a default broadcast as turbo-stream" do
    stream = Pgbus.stream(stream_name)
    client = connect

    stream.broadcast("<turbo-stream>normal</turbo-stream>")

    events = client.wait_for_events(count: 1, timeout: 5)
    expect(events.first.event).to eq("turbo-stream")
    expect(events.first.data).to eq("<turbo-stream>normal</turbo-stream>")

    client.close
  end
end
