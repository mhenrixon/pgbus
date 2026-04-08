# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"

# Integration test for Pgbus::Streams::Presence — the join/leave/members
# tracking primitive. Exercises:
#
#   1. join → row inserted, broadcast fired (received by SSE client)
#   2. members reads back the row with metadata
#   3. join again with the same id → upsert refreshes last_seen_at,
#      no duplicate row
#   4. leave → row deleted, broadcast fired
#   5. sweep!(older_than: future) → expires the remaining members
RSpec.describe "Streams: presence", :integration do
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

  let(:stream_name) { "pres_#{SecureRandom.hex(4)}" }

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

  def connect_sse_client
    @harness_started = true
    SseTestSupport::SseTestClient.connect(
      url: "#{harness.url("/#{signed(stream_name)}")}?since=0",
      timeout: 5
    )
  end

  it "tracks join, members, and leave with broadcasts" do
    stream = Pgbus.stream(stream_name)
    presence = stream.presence
    client = connect_sse_client

    presence.join(member_id: "user-7", metadata: { name: "Alice" }) do |member|
      "<turbo-stream action=\"append\" target=\"presence\">joined: #{member["id"]}</turbo-stream>"
    end

    members = presence.members
    expect(members.size).to eq(1)
    expect(members.first["id"]).to eq("user-7")
    expect(members.first["metadata"]).to eq("name" => "Alice")

    join_event = client.wait_for_events(count: 1, timeout: 5).first
    expect(join_event.data).to include("joined: user-7")

    presence.join(member_id: "user-7", metadata: { name: "Alice Updated" })
    members = presence.members
    expect(members.size).to eq(1)
    expect(members.first["metadata"]).to eq("name" => "Alice Updated")

    presence.leave(member_id: "user-7") do |member|
      "<turbo-stream action=\"remove\" target=\"presence-#{member["id"]}\"></turbo-stream>"
    end
    expect(presence.members).to be_empty

    leave_event = client.wait_for_events(count: 2, timeout: 5).last
    expect(leave_event.data).to include("presence-user-7")

    client.close
  end

  it "sweep! expires stale members" do
    presence = Pgbus.stream(stream_name).presence

    presence.join(member_id: "user-1", metadata: {})
    presence.join(member_id: "user-2", metadata: {})
    expect(presence.count).to eq(2)

    expired = presence.sweep!(older_than: Time.now + 3600)
    expect(expired).to eq(2)
    expect(presence.members).to be_empty
  end

  it "touch keeps a member alive past a sweep cutoff" do
    presence = Pgbus.stream(stream_name).presence

    presence.join(member_id: "user-3", metadata: {})
    sleep 0.5
    presence.touch(member_id: "user-3")

    # Use a server-side cutoff so we don't fight Ruby ↔ PG clock skew.
    # The touch above set last_seen_at = PG's NOW(); we now ask "what
    # rows are older than 100ms ago, on the server?" — should be zero
    # because touch just ran a few ms ago.
    server_cutoff = ActiveRecord::Base.connection.raw_connection
                                      .exec("SELECT NOW() - INTERVAL '100 milliseconds' AS cutoff")
                                      .first["cutoff"]

    expect(presence.sweep!(older_than: server_cutoff)).to eq(0)
    expect(presence.members.size).to eq(1)
  end

  it "count returns the number of members without deserializing metadata" do
    presence = Pgbus.stream(stream_name).presence
    presence.join(member_id: "user-1", metadata: { large_blob: "x" * 10_000 })
    presence.join(member_id: "user-2", metadata: { large_blob: "y" * 10_000 })

    expect(presence.count).to eq(2)
  end
end
