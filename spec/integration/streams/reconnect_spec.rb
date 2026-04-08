# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Integration test for reconnect-with-Last-Event-ID semantics. Proves the
# reconnect half of the hotwired/turbo#1261 + turbo-rails#674 fixes:
#
#   1. A client connects, receives messages 1-3, remembers id=3
#   2. The client disconnects (network blip, process restart, tab sleep)
#   3. While disconnected, messages 4 and 5 are published
#   4. The client reconnects with Last-Event-ID: 3
#   5. The server replays messages 4 and 5 (not 1-3 — the client already
#      has those) from the PGMQ archive via Client#read_after
#   6. No duplicates, no gaps
#
# This uses the same harness + streamer setup as page_born_stale_spec but
# exercises the reconnect path which is structurally different: the replay
# cursor comes from the Last-Event-ID HTTP header rather than the ?since=
# query param, and the client-side state is carried across the gap by
# the test harness (in production it's persisted automatically by the
# browser's EventSource implementation).
RSpec.describe "Streams: reconnect with Last-Event-ID", :integration do
  before(:all) do
    @saved_listen_notify = Pgbus.configuration.listen_notify
    Pgbus.configuration.listen_notify = true
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    Pgbus.configuration.streams_listen_health_check_ms = 50
    Pgbus.configuration.streams_heartbeat_interval = 30
    Pgbus.configuration.streams_write_deadline_ms = 5_000
    Pgbus.reset_client!
  end

  after(:all) do
    Pgbus.configuration.listen_notify = @saved_listen_notify
    Pgbus.configuration.streams_signed_name_secret = nil
    Pgbus.reset_client!
  end

  let(:stream_name) { "rc_#{SecureRandom.hex(4)}" }

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

  def connect_sse_client(since_id: nil, last_event_id: nil)
    @harness_started = true
    url = harness.url("/#{signed(stream_name)}").to_s
    url += "?since=#{since_id}" if since_id
    headers = last_event_id ? { "Last-Event-ID" => last_event_id.to_s } : {}
    SseTestSupport::SseTestClient.connect(url: url, headers: headers, timeout: 5)
  end

  it "replays only messages after Last-Event-ID on reconnect" do
    stream = Pgbus.stream(stream_name)

    # === Phase 1: connect and receive the first batch ===
    # Broadcast three messages BEFORE the initial connect so they all
    # arrive via the replay path — that makes the cursor deterministic.
    stream.broadcast("<turbo-stream>first</turbo-stream>")
    stream.broadcast("<turbo-stream>second</turbo-stream>")
    stream.broadcast("<turbo-stream>third</turbo-stream>")

    client1 = connect_sse_client(since_id: 0)
    initial = client1.wait_for_events(count: 3, timeout: 5)
    expect(initial.size).to eq(3)
    expect(initial.map(&:data)).to eq([
                                        "<turbo-stream>first</turbo-stream>",
                                        "<turbo-stream>second</turbo-stream>",
                                        "<turbo-stream>third</turbo-stream>"
                                      ])
    last_seen_id = initial.last.id
    expect(last_seen_id).to eq("3")

    client1.close

    # === Phase 2: broadcasts land while disconnected ===
    stream.broadcast("<turbo-stream>fourth</turbo-stream>")
    stream.broadcast("<turbo-stream>fifth</turbo-stream>")

    # === Phase 3: reconnect with Last-Event-ID ===
    client2 = connect_sse_client(last_event_id: last_seen_id)
    replayed = client2.wait_for_events(count: 2, timeout: 5)

    # Assertion 1: exactly two events replayed (not 5 — the cursor works)
    expect(replayed.size).to eq(2)
    expect(replayed.map(&:data)).to eq([
                                         "<turbo-stream>fourth</turbo-stream>",
                                         "<turbo-stream>fifth</turbo-stream>"
                                       ])

    # Assertion 2: ids are strictly greater than last_seen_id
    expect(replayed.map(&:id).map(&:to_i)).to all(be > last_seen_id.to_i)

    client2.close
  end

  it "delivers nothing on reconnect when there is nothing new" do
    stream = Pgbus.stream(stream_name)

    stream.broadcast("<turbo-stream>one</turbo-stream>")
    stream.broadcast("<turbo-stream>two</turbo-stream>")

    client1 = connect_sse_client(since_id: 0)
    initial = client1.wait_for_events(count: 2, timeout: 5)
    expect(initial.size).to eq(2)
    last_seen_id = initial.last.id
    client1.close

    # Reconnect immediately. Nothing has been published since last_seen_id,
    # so the replay returns empty — the new connection just waits quietly.
    client2 = connect_sse_client(last_event_id: last_seen_id)
    # Give the server a brief window to deliver anything it would have
    # delivered. Expect zero events.
    sleep 0.2
    expect(client2.events).to be_empty
    client2.close
  end
end
