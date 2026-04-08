# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Integration test for the transactional broadcasts feature. This is the
# "no other Rails real-time stack can do this" differentiator: a broadcast
# issued inside an AR transaction that later rolls back must NEVER be
# delivered to any SSE client, because the underlying data state was never
# committed.
#
# Exercises three scenarios end-to-end with real AR + real PGMQ + real SSE:
#
#   1. Broadcast inside a committed transaction → delivered after commit.
#   2. Broadcast inside a rolled-back transaction → not delivered, ever.
#   3. Multiple broadcasts across commit + rollback in sequence → only the
#      committed ones are delivered.
RSpec.describe "Streams: transactional broadcasts", :integration do
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

  let(:stream_name) { "txn_#{SecureRandom.hex(4)}" }

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

  def connect_sse_client(last_event_id: nil)
    @harness_started = true
    url = harness.url("/#{signed(stream_name)}?since=0")
    headers = last_event_id ? { "Last-Event-ID" => last_event_id.to_s } : {}
    SseTestSupport::SseTestClient.connect(url: url, headers: headers, timeout: 5)
  end

  it "delivers a broadcast made inside a committed AR transaction" do
    stream = Pgbus.stream(stream_name)
    client = connect_sse_client

    ActiveRecord::Base.transaction do
      stream.broadcast("<turbo-stream>committed</turbo-stream>")
    end

    events = client.wait_for_events(count: 1, timeout: 5)
    expect(events.size).to eq(1)
    expect(events.first.data).to eq("<turbo-stream>committed</turbo-stream>")

    client.close
  end

  it "never delivers a broadcast issued inside a rolled-back transaction" do
    stream = Pgbus.stream(stream_name)
    client = connect_sse_client

    ActiveRecord::Base.transaction do
      stream.broadcast("<turbo-stream>rolled-back</turbo-stream>")
      raise ActiveRecord::Rollback
    end

    # Negative assertion: give any erroneously-delivered event time to
    # arrive through the SSE pipeline before checking. A rolled-back
    # transaction should produce zero events — the sleep bounds how
    # long we'll wait for the "never happens" assertion to become true.
    sleep 0.3
    expect(client.events).to be_empty

    rows = Pgbus.client.read_after(stream_name, after_id: 0, limit: 10)
    expect(rows).to be_empty

    client.close
  end

  it "delivers only the committed broadcasts in a sequence of commit/rollback/commit" do
    stream = Pgbus.stream(stream_name)
    client = connect_sse_client

    ActiveRecord::Base.transaction do
      stream.broadcast("<turbo-stream>first-committed</turbo-stream>")
    end

    ActiveRecord::Base.transaction do
      stream.broadcast("<turbo-stream>rolled-back</turbo-stream>")
      raise ActiveRecord::Rollback
    end

    ActiveRecord::Base.transaction do
      stream.broadcast("<turbo-stream>second-committed</turbo-stream>")
    end

    events = client.wait_for_events(count: 2, timeout: 5)
    expect(events.size).to eq(2)
    expect(events.map(&:data)).to eq([
                                       "<turbo-stream>first-committed</turbo-stream>",
                                       "<turbo-stream>second-committed</turbo-stream>"
                                     ])

    client.close
  end
end
