# frozen_string_literal: true

require_relative "../../../lib/pgbus/web/data_source"

RSpec.describe Pgbus::Web::DataSource do
  # Stub the Client class to avoid loading pgmq-ruby
  let(:mock_client) { double("Pgbus::Client", pgmq: double("pgmq")) }

  subject(:data_source) { described_class.new(client: mock_client) }

  describe "#queues_with_metrics" do
    it "returns formatted metrics" do
      metrics = [
        double(
          queue_name: "pgbus_default",
          queue_length: "5",
          queue_visible_length: "3",
          oldest_msg_age_sec: "120",
          newest_msg_age_sec: "10",
          total_messages: "1000"
        )
      ]
      allow(mock_client).to receive(:metrics).with(no_args).and_return(metrics)

      result = data_source.queues_with_metrics
      expect(result.size).to eq(1)
      expect(result.first[:name]).to eq("pgbus_default")
      expect(result.first[:queue_length]).to eq(5)
      expect(result.first[:queue_visible_length]).to eq(3)
      expect(result.first[:total_messages]).to eq(1000)
    end

    it "returns empty array on error" do
      allow(mock_client).to receive(:metrics).with(no_args).and_raise(StandardError)

      expect(data_source.queues_with_metrics).to eq([])
    end
  end

  describe "#queue_detail" do
    it "returns formatted metrics for a single queue" do
      metric = double(
        queue_name: "pgbus_critical",
        queue_length: "10",
        queue_visible_length: "8",
        oldest_msg_age_sec: "60",
        newest_msg_age_sec: "5",
        total_messages: "500"
      )
      allow(mock_client).to receive(:metrics).with("critical").and_return(metric)

      result = data_source.queue_detail("critical")
      expect(result[:name]).to eq("pgbus_critical")
      expect(result[:queue_length]).to eq(10)
    end

    it "returns nil when queue not found" do
      allow(mock_client).to receive(:metrics).with("missing").and_return(nil)

      expect(data_source.queue_detail("missing")).to be_nil
    end
  end

  describe "#summary_stats" do
    it "computes aggregate stats" do
      metrics = [
        double(queue_name: "pgbus_default", queue_length: "10", queue_visible_length: "8",
          oldest_msg_age_sec: nil, newest_msg_age_sec: nil, total_messages: "100"),
        double(queue_name: "pgbus_default_dlq", queue_length: "2", queue_visible_length: "2",
          oldest_msg_age_sec: nil, newest_msg_age_sec: nil, total_messages: "5")
      ]
      allow(mock_client).to receive(:metrics).with(no_args).and_return(metrics)
      allow(data_source).to receive(:failed_events_count).and_return(3)
      allow(data_source).to receive(:processes).and_return([{ id: 1 }, { id: 2 }])

      stats = data_source.summary_stats
      expect(stats[:total_queues]).to eq(2)
      expect(stats[:total_depth]).to eq(12)
      expect(stats[:dlq_depth]).to eq(2)
      expect(stats[:active_processes]).to eq(2)
      expect(stats[:failed_count]).to eq(3)
    end
  end

  describe "#registered_subscribers" do
    it "returns subscriber info from registry" do
      handler_class = Class.new(Pgbus::EventBus::Handler)
      stub_const("MyHandler", handler_class)

      registry = Pgbus::EventBus::Registry.instance
      registry.clear!
      registry.subscribe("orders.#", handler_class)

      result = data_source.registered_subscribers
      expect(result.size).to eq(1)
      expect(result.first[:pattern]).to eq("orders.#")
      expect(result.first[:handler_class]).to eq("MyHandler")
    end
  end
end
