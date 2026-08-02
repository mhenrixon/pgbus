# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/sse_test_client"
require "tmpdir"

# End-to-end for issue #382: the full SSE path under :master scope.
#
#   MasterHub (one LISTEN connection)
#     ← Unix socket → Instance A (FailoverListener → HubClient) → SSE client A
#     ← Unix socket → Instance B (FailoverListener → HubClient) → SSE client B
#
# Two streamer Instances stand in for two Puma workers (the process boundary
# itself is proven in master_hub_spec.rb; this spec proves the full
# Instance → HubClient → Dispatcher → hijacked-socket delivery chain), then
# the hub DIES mid-test and both instances keep delivering via their
# fallback listeners — the settled outage semantics: connections over loss.
RSpec.describe "Streams master hub end-to-end (issue #382)", :integration do
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

  let(:tmpdir) { Dir.mktmpdir("pgbus-hub-e2e") }
  let(:socket_path) { File.join(tmpdir, "hub.sock") }
  let(:stream_name) { "hube2e_#{SecureRandom.hex(4)}" }
  let(:hub) do
    Pgbus::Web::Streamer::MasterHub.new(
      config: Pgbus.configuration, socket_path: socket_path,
      status_interval: 0.5, logger: Logger.new(IO::NULL)
    )
  end

  def build_worker_instance
    Pgbus::Web::Streamer::Instance.new(
      client: Pgbus.client,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL)
    )
  end

  def build_app(streamer)
    Pgbus::Web::StreamApp.new(
      streamer: streamer,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL)
    )
  end

  def listen_backend_pids
    ActiveRecord::Base.connection.select_values(<<~SQL)
      SELECT pid FROM pg_stat_activity
      WHERE application_name = 'pgbus-listen' AND datname = current_database()
    SQL
  end

  around do |example|
    original = ENV.fetch("PGBUS_STREAMS_HUB_SOCKET", nil)
    ENV["PGBUS_STREAMS_HUB_SOCKET"] = socket_path
    example.run
  ensure
    original ? ENV["PGBUS_STREAMS_HUB_SOCKET"] = original : ENV.delete("PGBUS_STREAMS_HUB_SOCKET")
    FileUtils.remove_entry(tmpdir) if File.directory?(tmpdir)
  end

  before { Pgbus.client.ensure_stream_queue(stream_name) }

  def signed(name)
    Pgbus::Streams::SignedName.sign(name)
  end

  it "delivers SSE through one shared connection and keeps delivering after the hub dies" do
    baseline_pids = listen_backend_pids
    hub.start

    worker_a = build_worker_instance
    worker_b = build_worker_instance
    expect(worker_a.listener).to be_a(Pgbus::Web::Streamer::FailoverListener)
    expect(worker_b.listener).to be_a(Pgbus::Web::Streamer::FailoverListener)
    worker_a.start
    worker_b.start

    harness_a = SseTestSupport::PumaTestHarness.boot(rack_app: build_app(worker_a))
    harness_b = SseTestSupport::PumaTestHarness.boot(rack_app: build_app(worker_b))

    stream = Pgbus.stream(stream_name)
    watermark = stream.current_msg_id
    client_a = SseTestSupport::SseTestClient.connect(
      url: "#{harness_a.url("/#{signed(stream_name)}")}?since=#{watermark}", timeout: 5
    )
    client_b = SseTestSupport::SseTestClient.connect(
      url: "#{harness_b.url("/#{signed(stream_name)}")}?since=#{watermark}", timeout: 5
    )

    # Both workers served SSE — yet the whole "host" pins ONE connection.
    expect((listen_backend_pids - baseline_pids).size).to eq(1)

    stream.broadcast("<turbo-stream>via hub</turbo-stream>")
    expect(client_a.wait_for_events(count: 1, timeout: 5).map(&:data))
      .to eq(["<turbo-stream>via hub</turbo-stream>"])
    expect(client_b.wait_for_events(count: 1, timeout: 5).map(&:data))
      .to eq(["<turbo-stream>via hub</turbo-stream>"])

    # The hub dies. Both workers fail over to their own listeners (the
    # accepted, census-visible balloon) and SSE delivery continues.
    hub.stop
    sleep 0.5

    stream.broadcast("<turbo-stream>via fallback</turbo-stream>")
    expect(client_a.wait_for_events(count: 2, timeout: 10).map(&:data).last)
      .to eq("<turbo-stream>via fallback</turbo-stream>")
    expect(client_b.wait_for_events(count: 2, timeout: 10).map(&:data).last)
      .to eq("<turbo-stream>via fallback</turbo-stream>")

    expect((listen_backend_pids - baseline_pids).size).to eq(2)
  ensure
    client_a&.close
    client_b&.close
    worker_a&.shutdown!
    worker_b&.shutdown!
    harness_a&.shutdown
    harness_b&.shutdown
  end
end
