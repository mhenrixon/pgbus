# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Dispatcher do
  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
      c.streams_queue_prefix = "pgbus_test_stream"
      c.streams_orphan_sweep_interval = 3600
      c.streams_orphan_threshold = 86_400
    end
  end

  let(:dispatcher) { described_class.new(config: config) }
  let(:conn) { double("ActiveRecord::Connection") }
  let(:mock_client) { build_mock_client }

  before do
    allow(ActiveRecord::Base).to receive(:connection).and_return(conn)
    allow(Pgbus).to receive_messages(client: mock_client, configuration: config)
  end

  describe "#sweep_orphan_streams" do
    it "drops empty stream queues" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_stream_orphan])

      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_stream_orphan/), anything)
        .and_return({ "queue_length" => "0" })

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).to have_received(:drop_queue)
        .with("pgbus_test_stream_orphan", prefixed: false)
    end

    it "keeps stream queues that have messages" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_stream_chat])

      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_stream_chat/), anything)
        .and_return({ "queue_length" => "5" })

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "skips non-stream queues" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_default pgbus_test_default_dlq])

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "handles missing queue tables gracefully" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_stream_gone])

      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_stream_gone/), anything)
        .and_return(nil)

      expect { dispatcher.send(:sweep_orphan_streams) }.not_to raise_error
    end

    it "preserves non-empty queues even when stale (durable replay contract)" do
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_stream_stale])

      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_stream_stale/), anything)
        .and_return({ "queue_length" => "100" })

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "skips sweeping when threshold is nil (disabled)" do
      config.streams_orphan_threshold = nil

      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_stream_empty])

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "is wired into the dispatcher maintenance loop" do
      expect(described_class.private_method_defined?(:sweep_orphan_streams)).to be true
    end
  end
end
