# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/falcon_test_harness"
require_relative "../../support/sse_test_client"

# Integration test that the Falcon-native streaming body path works.
# When streams_falcon_streaming_body is true, StreamApp returns
# [200, headers, Writable] instead of hijacking the socket. Falcon
# drives the response lifecycle via its fiber scheduler.
#
# This is separate from falcon_spec.rb which tests the hijack path.
RSpec.describe "Streams: Falcon native streaming body", :integration do
  before(:all) do
    @saved_listen_notify = Pgbus.configuration.listen_notify
    @saved_signed_name_secret = Pgbus.configuration.streams_signed_name_secret
    @saved_listen_health_check_ms = Pgbus.configuration.streams_listen_health_check_ms
    @saved_heartbeat_interval = Pgbus.configuration.streams_heartbeat_interval
    @saved_write_deadline_ms = Pgbus.configuration.streams_write_deadline_ms
    @saved_falcon_streaming_body = Pgbus.configuration.streams_falcon_streaming_body
    Pgbus.configuration.listen_notify = true
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    Pgbus.configuration.streams_listen_health_check_ms = 100
    Pgbus.configuration.streams_heartbeat_interval = 30
    Pgbus.configuration.streams_write_deadline_ms = 5_000
    Pgbus.configuration.streams_falcon_streaming_body = true
    Pgbus.reset_client!
  end

  after(:all) do
    Pgbus.configuration.listen_notify = @saved_listen_notify
    Pgbus.configuration.streams_signed_name_secret = @saved_signed_name_secret
    Pgbus.configuration.streams_listen_health_check_ms = @saved_listen_health_check_ms
    Pgbus.configuration.streams_heartbeat_interval = @saved_heartbeat_interval
    Pgbus.configuration.streams_write_deadline_ms = @saved_write_deadline_ms
    Pgbus.configuration.streams_falcon_streaming_body = @saved_falcon_streaming_body
    Pgbus.reset_client!
  end

  let(:stream_name) { "fsb_#{SecureRandom.hex(4)}" }

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
    @booted_harness&.shutdown
  end

  def signed(name)
    Pgbus::Streams::SignedName.sign(name)
  end

  def connect_sse_client(since_id:)
    @booted_harness = harness
    url = "#{@booted_harness.url("/#{signed(stream_name)}")}?since=#{since_id}"
    SseTestSupport::SseTestClient.connect(url: url, timeout: 5)
  end

  it "delivers broadcasts through Falcon's native streaming body" do
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

    replay_ids = events.map { |e| e.id.to_i }
    expect(replay_ids).to all(be > rendered_watermark)
    expect(replay_ids).to eq(replay_ids.sort)

    stream.broadcast("<turbo-stream>C</turbo-stream>")
    live_events = client.wait_for_events(count: 3, timeout: 5)
    expect(live_events.size).to eq(3)
    expect(live_events.last.data).to include(">C<")
    expect(live_events.last.id.to_i).to be > events.last.id.to_i

    client.close
  end
end
