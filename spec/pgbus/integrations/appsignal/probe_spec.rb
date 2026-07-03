# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pgbus::Integrations::Appsignal::Probe" do
  let(:appsignal_class) do
    Class.new do
      class << self
        attr_accessor :gauges
      end
      self.gauges = []

      def self.set_gauge(name, value, tags = {})
        gauges << [name, value, tags]
      end
    end
  end

  let(:fake_data_source) do
    Class.new do
      def queues_with_metrics
        [
          { name: "pgbus_default", queue_length: 42, queue_visible_length: 30, oldest_msg_age_sec: 5.0, paused: false },
          { name: "pgbus_critical", queue_length: 0, queue_visible_length: 0, oldest_msg_age_sec: nil, paused: true }
        ]
      end

      def processes
        host = Socket.gethostname
        [
          { pid: 1, hostname: host },
          { pid: 2, hostname: host },
          { pid: 3, hostname: "other.example.com" }
        ]
      end

      def summary_stats
        {
          total_queues: 2,
          total_depth: 42,
          total_visible: 30,
          dlq_depth: 1,
          failed_count: 7,
          throughput_rate: 12.5,
          total_dead_tuples: 100,
          tables_needing_vacuum: 0,
          oldest_transaction_age_sec: 0.5
        }
      end

      def stream_stats_available?
        true
      end

      def stream_stats_summary(minutes: 60) # rubocop:disable Lint/UnusedMethodArgument
        {
          broadcasts: 200,
          connects: 30,
          disconnects: 5,
          active_estimate: 25,
          avg_fanout: 4.0,
          avg_broadcast_ms: 1.2,
          avg_connect_ms: 5.5
        }
      end
    end.new
  end

  let(:fake_client) do
    Class.new do
      def pool_stats
        { size: 8, available: 3, pool_timeout: 5 }
      end
    end.new
  end

  before do
    stub_const("Appsignal", appsignal_class)
    require "pgbus/integrations/appsignal/probe"
  end

  it "records queue depth gauges per queue without a hostname tag" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    names = appsignal_class.gauges.map(&:first)
    expect(names).to include(
      "pgbus_queue_depth",
      "pgbus_queue_visible_depth",
      "pgbus_queue_paused",
      "pgbus_queue_oldest_message_age_seconds"
    )

    depth_gauge = appsignal_class.gauges.find { |g| g[0] == "pgbus_queue_depth" && g[2][:queue] == "pgbus_default" }
    expect(depth_gauge[2]).to eq(queue: "pgbus_default")
  end

  it "records queue latency gauge per queue without a hostname tag" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    latency = appsignal_class.gauges.find { |g| g[0] == "pgbus_queue_latency" && g[2][:queue] == "pgbus_default" }
    expect(latency).not_to be_nil
    expect(latency[1]).to eq(5000.0)
    expect(latency[2]).to eq(queue: "pgbus_default")
  end

  it "skips queue latency when oldest_msg_age_sec is nil" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    critical_latency = appsignal_class.gauges.find { |g| g[0] == "pgbus_queue_latency" && g[2][:queue] == "pgbus_critical" }
    expect(critical_latency).to be_nil
  end

  it "records active_processes scoped to the current host with hostname tag" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    process_gauge = appsignal_class.gauges.find { |g| g[0] == "pgbus_active_processes" }
    expect(process_gauge[1]).to eq(2) # 2 local + 1 remote in the fake source
    expect(process_gauge[2]).to eq(hostname: Socket.gethostname)
  end

  it "records summary gauges without a hostname tag" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    names = appsignal_class.gauges.map(&:first)
    expect(names).to include(
      "pgbus_dlq_depth",
      "pgbus_failed_events_total",
      "pgbus_total_dead_tuples",
      "pgbus_oldest_transaction_age_seconds"
    )

    dlq_gauge = appsignal_class.gauges.find { |g| g[0] == "pgbus_dlq_depth" }
    expect(dlq_gauge[2]).to eq({})
  end

  it "records stream gauges without a hostname tag" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    names = appsignal_class.gauges.map(&:first)
    expect(names).to include("pgbus_stream_active_connections", "pgbus_stream_avg_fanout")

    stream_gauge = appsignal_class.gauges.find { |g| g[0] == "pgbus_stream_active_connections" }
    expect(stream_gauge[2]).to eq({})
  end

  it "skips stream gauges when stream stats are unavailable" do
    quiet_source = Class.new do
      def queues_with_metrics = []
      def processes = []

      def summary_stats
        { total_queues: 0, total_depth: 0, total_visible: 0, dlq_depth: 0, failed_count: 0,
          throughput_rate: 0, total_dead_tuples: 0, tables_needing_vacuum: 0, oldest_transaction_age_sec: 0 }
      end

      def stream_stats_available? = false
    end.new

    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: quiet_source)
    runner.call

    names = appsignal_class.gauges.map(&:first)
    expect(names).not_to include("pgbus_stream_active_connections")
  end

  it "records pool_size and pool_available gauges scoped to the current host" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source, client: fake_client)
    runner.call

    size_gauge = appsignal_class.gauges.find { |g| g[0] == "pgbus_pool_size" }
    available_gauge = appsignal_class.gauges.find { |g| g[0] == "pgbus_pool_available" }

    expect(size_gauge[1]).to eq(8)
    expect(size_gauge[2]).to eq(hostname: Socket.gethostname)
    expect(available_gauge[1]).to eq(3)
    expect(available_gauge[2]).to eq(hostname: Socket.gethostname)
  end

  it "skips pool gauges when pool_stats is empty" do
    empty_client = Class.new { def pool_stats = {} }.new
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source, client: empty_client)
    runner.call

    names = appsignal_class.gauges.map(&:first)
    expect(names).not_to include("pgbus_pool_size", "pgbus_pool_available")
  end

  it "is resilient to a failing pool_stats read" do
    flaky_client = Class.new { def pool_stats = raise("boom") }.new
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source, client: flaky_client)

    expect { runner.call }.not_to raise_error
  end

  it "is resilient to data source errors" do
    flaky_source = Class.new do
      def queues_with_metrics = raise("boom")
      def processes = raise("boom")
      def summary_stats = raise("boom")
      def stream_stats_available? = raise("boom")
    end.new

    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: flaky_source)

    expect { runner.call }.not_to raise_error
  end
end
