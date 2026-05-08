# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::DataSource do
  subject(:data_source) { described_class.new(client: mock_client) }

  let(:mock_client) { build_mock_client }
  let(:conn) { double("ActiveRecord::Connection") }

  before do
    allow(data_source).to receive_messages(connection: conn, paused_queue_names: [])
  end

  describe "#queues_with_metrics" do
    it "fetches all queue metrics in a single batched query" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_default pgbus_mailers])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      allow(conn).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: [
                             { "queue_name" => "pgbus_default", "queue_length" => "5",
                               "queue_visible_length" => "3", "newest_msg_age_sec" => "10",
                               "oldest_msg_age_sec" => "100", "total_messages" => "500" },
                             { "queue_name" => "pgbus_mailers", "queue_length" => "2",
                               "queue_visible_length" => "2", "newest_msg_age_sec" => "5",
                               "oldest_msg_age_sec" => "50", "total_messages" => "200" }
                           ]))

      result = data_source.queues_with_metrics

      expect(result.length).to eq(2)
      expect(result.first[:name]).to eq("pgbus_default")
      expect(result.first[:queue_length]).to eq(5)
      expect(result.last[:name]).to eq("pgbus_mailers")
      expect(result.last[:total_messages]).to eq(200)
    end

    it "handles empty queue list" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return([])

      result = data_source.queues_with_metrics
      expect(result).to eq([])
    end

    it "handles errors gracefully" do
      allow(conn).to receive(:select_values).and_raise(StandardError, "boom")

      result = data_source.queues_with_metrics
      expect(result).to eq([])
    end

    it "does not make N+1 queries" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[q1 q2 q3])

      allow(conn).to receive(:quote) { |v| "'#{v}'" }

      allow(conn).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: []))

      data_source.queues_with_metrics

      # select_values for queue names + select_all for batched metrics = 2 queries total
      expect(conn).to have_received(:select_values).once
      expect(conn).to have_received(:select_all).once
    end
  end
end
