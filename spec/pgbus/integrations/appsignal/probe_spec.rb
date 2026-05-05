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
        [{ pid: 1 }, { pid: 2 }]
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

  before do
    stub_const("Appsignal", appsignal_class)
    require "pgbus/integrations/appsignal/probe"
  end

  it "records queue depth gauges per queue" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    names = appsignal_class.gauges.map(&:first)
    expect(names).to include(
      "pgbus_queue_depth",
      "pgbus_queue_visible_depth",
      "pgbus_queue_paused",
      "pgbus_queue_oldest_message_age_seconds"
    )
  end

  it "records process count" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    expect(appsignal_class.gauges).to include(["pgbus_active_processes", 2, {}])
  end

  it "records summary gauges" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    names = appsignal_class.gauges.map(&:first)
    expect(names).to include(
      "pgbus_dlq_depth",
      "pgbus_failed_events_total",
      "pgbus_total_dead_tuples",
      "pgbus_oldest_transaction_age_seconds"
    )
  end

  it "records stream gauges when stream stats are available" do
    runner = Pgbus::Integrations::Appsignal::Probe::Runner.new(data_source: fake_data_source)
    runner.call

    names = appsignal_class.gauges.map(&:first)
    expect(names).to include("pgbus_stream_active_connections", "pgbus_stream_avg_fanout")
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
