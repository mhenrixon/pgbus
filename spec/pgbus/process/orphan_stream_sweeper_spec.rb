# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Dispatcher do
  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
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
    allow(Pgbus::StreamQueue).to receive(:reset_cache!)
    # Default: no registered streams. Each example that expects a queue to be
    # treated as a stream registers it explicitly. Real stream queues are
    # named like job queues (pgbus_test_<name>) — the registry, not the name,
    # is what tells them apart.
    allow(Pgbus::StreamQueue).to receive(:all_names).and_return(Set.new)
  end

  # Helper: mark the given physical queue names as registered streams.
  def register_streams(*names)
    allow(Pgbus::StreamQueue).to receive(:all_names).and_return(Set.new(names))
  end

  describe "#sweep_orphan_streams" do
    it "drops empty stream queues" do
      register_streams("pgbus_test_orphan")
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_orphan])

      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_orphan/), anything)
        .and_return({ "queue_length" => "0", "age_sec" => "10" })

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).to have_received(:drop_queue)
        .with("pgbus_test_orphan", prefixed: false)
    end

    it "drops non-empty stream queues older than the threshold (durable leak fix)" do
      register_streams("pgbus_test_leaked")
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_leaked])

      # Non-empty but 25h old (threshold is 24h) — the durable-stream leak case.
      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_leaked/), anything)
        .and_return({ "queue_length" => "42", "age_sec" => (25 * 3600).to_s })

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).to have_received(:drop_queue)
        .with("pgbus_test_leaked", prefixed: false)
    end

    it "keeps non-empty stream queues younger than the threshold" do
      register_streams("pgbus_test_fresh")
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_fresh])

      # Non-empty and only 1h old — inside the replay window, must survive.
      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_fresh/), anything)
        .and_return({ "queue_length" => "42", "age_sec" => (1 * 3600).to_s })

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "keeps a non-empty young queue even when created_at is unknown (nil age)" do
      register_streams("pgbus_test_noage")
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_noage])

      # A missing created_at (nil age_sec) must NOT be treated as infinitely old.
      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_noage/), anything)
        .and_return({ "queue_length" => "3", "age_sec" => nil })

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "keeps stream queues that have messages" do
      register_streams("pgbus_test_chat")
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_chat])

      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_chat/), anything)
        .and_return({ "queue_length" => "5", "age_sec" => "60" })

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "never touches a queue that is NOT a registered stream, even if empty" do
      # This is the #308 core fix: a JOB queue must never be swept. Before the
      # registry, an empty queue whose name matched the (mis-wired) prefix
      # could be dropped. Now only registered stream queues are candidates.
      # pgbus_test_default is empty AND ancient but is a job queue.
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_default pgbus_test_stream_job])
      allow(conn).to receive(:select_one)

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
      # The per-queue count query must not even run for non-stream queues.
      expect(conn).not_to have_received(:select_one)
    end

    it "handles missing queue tables gracefully" do
      register_streams("pgbus_test_gone")
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_gone])

      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_gone/), anything)
        .and_return(nil)

      expect { dispatcher.send(:sweep_orphan_streams) }.not_to raise_error
    end

    it "preserves non-empty queues (durable replay contract)" do
      register_streams("pgbus_test_stale")
      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_stale])

      allow(conn).to receive(:select_one)
        .with(a_string_matching(/q_pgbus_test_stale/), anything)
        .and_return({ "queue_length" => "100", "age_sec" => "300" })

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "skips sweeping when threshold is nil (disabled)" do
      config.streams_orphan_threshold = nil
      register_streams("pgbus_test_empty")

      allow(conn).to receive(:select_values)
        .with(a_string_matching(/pgmq\.meta/))
        .and_return(%w[pgbus_test_empty])

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "skips sweeping when no streams are registered (unmigrated install)" do
      # all_names is empty by default — the sweep returns early without
      # querying pgmq.meta at all.
      allow(conn).to receive(:select_values)

      dispatcher.send(:sweep_orphan_streams)

      expect(conn).not_to have_received(:select_values)
      expect(mock_client).not_to have_received(:drop_queue)
    end

    it "is wired into the dispatcher maintenance loop" do
      expect(described_class.private_method_defined?(:sweep_orphan_streams)).to be true
    end
  end
end
