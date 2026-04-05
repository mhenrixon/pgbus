# frozen_string_literal: true

require "spec_helper"

require_relative "../../../lib/pgbus/web/data_source"

RSpec.describe Pgbus::Web::DataSource do
  subject(:data_source) { described_class.new(client: mock_client) }

  let(:mock_client) { double("Pgbus::Client", pgmq: double("pgmq")) }
  let(:mock_connection) { double("ActiveRecord::Connection") }

  before do
    allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection)
  end

  describe "#queues_with_metrics" do
    it "returns formatted metrics via SQL" do
      allow(mock_connection).to receive(:select_values).and_return(["pgbus_default"])
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Queue Metrics")
        .and_return({
                      "queue_length" => 5,
                      "queue_visible_length" => 3,
                      "oldest_msg_age_sec" => 120,
                      "newest_msg_age_sec" => 10,
                      "total_messages" => 1000
                    })

      result = data_source.queues_with_metrics
      expect(result.size).to eq(1)
      expect(result.first[:name]).to eq("pgbus_default")
      expect(result.first[:queue_length]).to eq(5)
      expect(result.first[:queue_visible_length]).to eq(3)
      expect(result.first[:total_messages]).to eq(1000)
    end

    it "returns empty array on error" do
      allow(mock_connection).to receive(:select_values).and_raise(StandardError)

      expect(data_source.queues_with_metrics).to eq([])
    end
  end

  describe "#queue_detail" do
    it "returns formatted metrics for a single queue" do
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Queue Metrics")
        .and_return({
                      "queue_length" => 10,
                      "queue_visible_length" => 8,
                      "oldest_msg_age_sec" => 60,
                      "newest_msg_age_sec" => 5,
                      "total_messages" => 500
                    })

      result = data_source.queue_detail("pgbus_critical")
      expect(result[:name]).to eq("pgbus_critical")
      expect(result[:queue_length]).to eq(10)
    end

    it "returns nil when metrics query returns nil" do
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Queue Metrics")
        .and_return(nil)

      expect(data_source.queue_detail("missing")).to be_nil
    end
  end

  describe "#summary_stats" do
    it "computes aggregate stats" do
      allow(mock_connection).to receive(:select_values).and_return(%w[pgbus_default pgbus_default_dlq])

      allow(mock_connection).to receive(:select_one).with(anything, "Pgbus Queue Metrics").and_return(
        { "queue_length" => 10, "queue_visible_length" => 8,
          "oldest_msg_age_sec" => nil, "newest_msg_age_sec" => nil, "total_messages" => 100 },
        { "queue_length" => 2, "queue_visible_length" => 2,
          "oldest_msg_age_sec" => nil, "newest_msg_age_sec" => nil, "total_messages" => 5 }
      )

      allow(data_source).to receive_messages(failed_events_count: 3, processes: [{ id: 1 }, { id: 2 }])

      stats = data_source.summary_stats
      expect(stats[:total_queues]).to eq(2)
      expect(stats[:total_depth]).to eq(12)
      expect(stats[:dlq_depth]).to eq(2)
      expect(stats[:active_processes]).to eq(2)
      expect(stats[:failed_count]).to eq(3)
    end
  end

  describe "#purge_queue" do
    it "passes the queue name directly without re-prefixing" do
      allow(mock_client).to receive(:purge_queue)
      allow(mock_connection).to receive(:select_all).and_return([])

      data_source.purge_queue("pgbus_default")

      expect(mock_client).to have_received(:purge_queue).with("pgbus_default", prefixed: false)
    end
  end

  describe "#drop_queue" do
    it "passes the queue name directly without re-prefixing" do
      allow(mock_client).to receive(:drop_queue)
      allow(mock_connection).to receive(:select_all).and_return([])

      data_source.drop_queue("pgbus_default")

      expect(mock_client).to have_received(:drop_queue).with("pgbus_default", prefixed: false)
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

  describe "#recurring_tasks" do
    it "returns formatted recurring tasks" do
      mock_record = double("RecurringTask",
                           id: 1, key: "daily_cleanup", class_name: "CleanupJob",
                           command: nil, schedule: "0 2 * * *", queue_name: "maintenance",
                           arguments: nil, priority: 0, description: "Cleanup",
                           enabled: true, static: true,
                           created_at: Time.now, updated_at: Time.now)

      relation = double("relation", to_a: [mock_record])
      allow(Pgbus::RecurringTask).to receive(:order).with(:key).and_return(relation)

      # Mock the aggregate query for last runs
      empty_scope = double("scope")
      allow(Pgbus::RecurringExecution).to receive(:where).and_return(empty_scope)
      allow(empty_scope).to receive_messages(select: empty_scope, group: empty_scope, index_by: {})

      result = data_source.recurring_tasks
      expect(result.size).to eq(1)
      expect(result.first[:key]).to eq("daily_cleanup")
      expect(result.first[:class_name]).to eq("CleanupJob")
      expect(result.first[:enabled]).to be true
    end

    it "returns empty array on error" do
      allow(Pgbus::RecurringTask).to receive(:order).and_raise(StandardError)

      expect(data_source.recurring_tasks).to eq([])
    end
  end

  describe "#recurring_tasks_count" do
    it "returns the count of recurring tasks" do
      allow(Pgbus::RecurringTask).to receive(:count).and_return(5)

      expect(data_source.recurring_tasks_count).to eq(5)
    end

    it "returns 0 on error" do
      allow(Pgbus::RecurringTask).to receive(:count).and_raise(StandardError)

      expect(data_source.recurring_tasks_count).to eq(0)
    end
  end

  describe "#toggle_recurring_task" do
    it "returns :disabled when toggling an enabled task" do
      mock_record = double("RecurringTask")
      allow(Pgbus::RecurringTask).to receive(:find_by).with(id: 1).and_return(mock_record)
      allow(mock_record).to receive(:enabled).and_return(true, false)
      allow(mock_record).to receive(:update!).with(enabled: false).and_return(true)

      expect(data_source.toggle_recurring_task(1)).to eq(:disabled)
    end

    it "returns :enabled when toggling a disabled task" do
      mock_record = double("RecurringTask")
      allow(Pgbus::RecurringTask).to receive(:find_by).with(id: 2).and_return(mock_record)
      allow(mock_record).to receive(:enabled).and_return(false, true)
      allow(mock_record).to receive(:update!).with(enabled: true).and_return(true)

      expect(data_source.toggle_recurring_task(2)).to eq(:enabled)
    end

    it "returns nil when task not found" do
      allow(Pgbus::RecurringTask).to receive(:find_by).with(id: 99).and_return(nil)

      expect(data_source.toggle_recurring_task(99)).to be_nil
    end

    it "returns nil when update fails" do
      mock_record = double("RecurringTask", enabled: true)
      allow(Pgbus::RecurringTask).to receive(:find_by).with(id: 3).and_return(mock_record)
      allow(mock_record).to receive(:update!).and_raise(StandardError, "boom")

      expect(data_source.toggle_recurring_task(3)).to be_nil
    end
  end

  describe "#discard_job" do
    it "archives the message and releases the uniqueness lock" do
      allow(mock_client).to receive(:archive_message)

      # Mock reading the message to extract uniqueness key
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42])
        .and_return({
                      "msg_id" => 42, "read_ct" => 0,
                      "enqueued_at" => Time.now.to_s, "vt" => Time.now.to_s,
                      "message" => '{"job_class":"ImportJob","pgbus_uniqueness_key":"import-42"}',
                      "headers" => nil, "last_read_at" => nil
                    })

      allow(Pgbus::UniquenessKey).to receive(:release!).and_return(1)

      data_source.discard_job("pgbus_default", 42)

      expect(mock_client).to have_received(:archive_message).with("pgbus_default", 42, prefixed: false)
      expect(Pgbus::UniquenessKey).to have_received(:release!).with("import-42")
    end

    it "does not release lock when message has no uniqueness key" do
      allow(mock_client).to receive(:archive_message)

      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42])
        .and_return({
                      "msg_id" => 42, "read_ct" => 0,
                      "enqueued_at" => Time.now.to_s, "vt" => Time.now.to_s,
                      "message" => '{"job_class":"PlainJob"}',
                      "headers" => nil, "last_read_at" => nil
                    })

      allow(Pgbus::UniquenessKey).to receive(:release!).and_return(1)

      data_source.discard_job("pgbus_default", 42)

      expect(Pgbus::UniquenessKey).not_to have_received(:release!)
    end
  end

  describe "#discard_failed_event" do
    it "deletes the event and releases the uniqueness lock" do
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Failed Event", [1])
        .and_return({
                      "id" => 1,
                      "payload" => '{"job_class":"FailedJob","pgbus_uniqueness_key":"failed-1"}'
                    })

      allow(mock_connection).to receive(:exec_delete)
      allow(Pgbus::UniquenessKey).to receive(:release!).and_return(1)

      data_source.discard_failed_event(1)

      expect(mock_connection).to have_received(:exec_delete)
      expect(Pgbus::UniquenessKey).to have_received(:release!).with("failed-1")
    end
  end

  describe "#discard_all_failed" do
    it "collects uniqueness keys before deleting and releases locks" do
      allow(mock_connection).to receive(:select_all)
        .with("SELECT payload FROM pgbus_failed_events", "Pgbus Collect Failed Keys")
        .and_return([
                      { "payload" => '{"pgbus_uniqueness_key":"k1"}' },
                      { "payload" => '{"pgbus_uniqueness_key":"k2"}' },
                      { "payload" => '{"job_class":"PlainJob"}' }
                    ])

      result_double = double("result", cmd_tuples: 3)
      allow(mock_connection).to receive(:execute)
        .with("DELETE FROM pgbus_failed_events")
        .and_return(result_double)

      allow(Pgbus::UniquenessKey).to receive(:where).and_return(double(delete_all: 2))

      count = data_source.discard_all_failed

      expect(count).to eq(3)
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: %w[k1 k2])
    end
  end

  describe "#discard_all_enqueued" do
    before do
      allow(mock_connection).to receive(:select_values).and_return(["pgbus_default"])
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Queue Metrics")
        .and_return({
                      "queue_length" => 2, "queue_visible_length" => 2,
                      "oldest_msg_age_sec" => nil, "newest_msg_age_sec" => nil,
                      "total_messages" => 10
                    })

      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Queue Messages", anything)
        .and_return([
                      { "msg_id" => 1, "read_ct" => 0, "enqueued_at" => Time.now.to_s,
                        "vt" => Time.now.to_s, "last_read_at" => nil, "headers" => nil,
                        "message" => '{"job_class":"ImportJob","pgbus_uniqueness_key":"k1"}' },
                      { "msg_id" => 2, "read_ct" => 0, "enqueued_at" => Time.now.to_s,
                        "vt" => Time.now.to_s, "last_read_at" => nil, "headers" => nil,
                        "message" => '{"job_class":"PlainJob"}' }
                    ])

      allow(mock_client).to receive(:archive_batch).and_return([1, 2])
      allow(Pgbus::UniquenessKey).to receive(:where).and_return(double(delete_all: 1))
    end

    it "archives all messages from non-DLQ queues and releases locks" do
      count = data_source.discard_all_enqueued

      expect(count).to eq(2)
      expect(mock_client).to have_received(:archive_batch).with("pgbus_default", [1, 2], prefixed: false)
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: ["k1"])
    end
  end

  describe "#discard_lock" do
    it "deletes the lock by key" do
      allow(Pgbus::UniquenessKey).to receive(:where).and_return(double(delete_all: 1))

      result = data_source.discard_lock("import-42")

      expect(result).to eq(1)
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: "import-42")
    end

    it "returns 0 on error" do
      allow(Pgbus::UniquenessKey).to receive(:where).and_raise(StandardError)

      expect(data_source.discard_lock("import-42")).to eq(0)
    end
  end

  describe "#discard_locks" do
    it "deletes multiple locks by keys" do
      allow(Pgbus::UniquenessKey).to receive(:where).and_return(double(delete_all: 3))

      result = data_source.discard_locks(%w[k1 k2 k3])

      expect(result).to eq(3)
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: %w[k1 k2 k3])
    end

    it "returns 0 for empty array" do
      expect(data_source.discard_locks([])).to eq(0)
    end

    it "returns 0 on error" do
      allow(Pgbus::UniquenessKey).to receive(:where).and_raise(StandardError)

      expect(data_source.discard_locks(%w[k1])).to eq(0)
    end
  end

  describe "#discard_all_locks" do
    it "deletes all locks" do
      allow(Pgbus::UniquenessKey).to receive(:delete_all).and_return(5)

      result = data_source.discard_all_locks

      expect(result).to eq(5)
      expect(Pgbus::UniquenessKey).to have_received(:delete_all)
    end

    it "returns 0 on error" do
      allow(Pgbus::UniquenessKey).to receive(:delete_all).and_raise(StandardError)

      expect(data_source.discard_all_locks).to eq(0)
    end
  end
end
