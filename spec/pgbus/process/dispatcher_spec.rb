# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Dispatcher do
  let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
  let(:mock_client) { build_mock_client }
  let(:dispatcher) { described_class.new }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
    allow(Pgbus).to receive(:client).and_return(mock_client)
  end

  describe "constants" do
    it "has a cleanup interval of 1 hour" do
      expect(described_class::CLEANUP_INTERVAL).to eq(3600)
    end

    it "has a reap interval of 5 minutes" do
      expect(described_class::REAP_INTERVAL).to eq(300)
    end

    it "has a concurrency interval of 5 minutes" do
      expect(described_class::CONCURRENCY_INTERVAL).to eq(300)
    end

    it "has a batch cleanup interval of 1 hour" do
      expect(described_class::BATCH_CLEANUP_INTERVAL).to eq(3600)
    end

    it "has a recurring cleanup interval of 1 hour" do
      expect(described_class::RECURRING_CLEANUP_INTERVAL).to eq(3600)
    end
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down flag" do
      dispatcher.graceful_shutdown
      expect(dispatcher.instance_variable_get(:@shutting_down)).to be true
    end
  end

  describe "#immediate_shutdown" do
    it "sets shutting_down flag" do
      dispatcher.immediate_shutdown
      expect(dispatcher.instance_variable_get(:@shutting_down)).to be true
    end
  end

  describe "#cleanup_processed_events (private)" do
    it "deletes expired processed events" do
      scope = double("scope", delete_all: 5)
      allow(Pgbus::ProcessedEvent).to receive(:expired).and_return(scope)

      dispatcher.send(:cleanup_processed_events)

      expect(Pgbus::ProcessedEvent).to have_received(:expired).with(a_kind_of(Time))
      expect(scope).to have_received(:delete_all)
    end

    it "returns early when idempotency_ttl is not set" do
      original_ttl = dispatcher.config.idempotency_ttl
      dispatcher.config.idempotency_ttl = nil
      allow(Pgbus::ProcessedEvent).to receive(:expired)

      dispatcher.send(:cleanup_processed_events)

      expect(Pgbus::ProcessedEvent).not_to have_received(:expired)
    ensure
      dispatcher.config.idempotency_ttl = original_ttl
    end

    it "rescues StandardError and logs a warning" do
      allow(Pgbus::ProcessedEvent).to receive(:expired).and_raise(StandardError, "db error")
      expect { dispatcher.send(:cleanup_processed_events) }.not_to raise_error
    end
  end

  describe "#reap_stale_processes (private)" do
    it "deletes stale processes" do
      scope = double("scope", delete_all: 2)
      allow(Pgbus::ProcessEntry).to receive(:stale).and_return(scope)

      dispatcher.send(:reap_stale_processes)

      expect(Pgbus::ProcessEntry).to have_received(:stale).with(a_kind_of(Time))
      expect(scope).to have_received(:delete_all)
    end

    it "rescues StandardError and logs a warning" do
      allow(Pgbus::ProcessEntry).to receive(:stale).and_raise(StandardError, "db error")
      expect { dispatcher.send(:reap_stale_processes) }.not_to raise_error
    end
  end

  def past_monotonic(seconds_ago)
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - seconds_ago
  end

  describe "#run_maintenance (private)" do
    it "skips cleanup when interval not elapsed" do
      allow(dispatcher).to receive(:cleanup_processed_events)
      allow(dispatcher).to receive(:reap_stale_processes)

      dispatcher.send(:run_maintenance)

      expect(dispatcher).not_to have_received(:cleanup_processed_events)
      expect(dispatcher).not_to have_received(:reap_stale_processes)
    end

    it "runs cleanup when cleanup interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_processed_events)
      allow(dispatcher).to receive(:reap_stale_processes)

      dispatcher.instance_variable_set(:@last_cleanup_at, past_monotonic(described_class::CLEANUP_INTERVAL + 1))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:cleanup_processed_events)
    end

    it "runs reap when reap interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_processed_events)
      allow(dispatcher).to receive(:reap_stale_processes)

      dispatcher.instance_variable_set(:@last_reap_at, past_monotonic(described_class::REAP_INTERVAL + 1))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:reap_stale_processes)
    end

    it "runs concurrency cleanup when concurrency interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_concurrency)

      dispatcher.instance_variable_set(:@last_concurrency_at, past_monotonic(described_class::CONCURRENCY_INTERVAL + 1))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:cleanup_concurrency)
    end

    it "runs batch cleanup when batch interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_batches)

      dispatcher.instance_variable_set(:@last_batch_cleanup_at, past_monotonic(described_class::BATCH_CLEANUP_INTERVAL + 1))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:cleanup_batches)
    end

    it "rescues errors from maintenance methods" do
      dispatcher.instance_variable_set(:@last_cleanup_at, past_monotonic(described_class::CLEANUP_INTERVAL + 1))
      allow(dispatcher).to receive(:cleanup_processed_events).and_raise(StandardError, "boom")
      expect { dispatcher.send(:run_maintenance) }.not_to raise_error
    end
  end

  describe "#cleanup_concurrency (private)" do
    it "expires stale semaphores and promotes blocked executions" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_return([{ "key" => "TestJob-42" }])
      allow(Pgbus::Concurrency::BlockedExecution).to receive_messages(expire_stale: 0, promote_next: false)

      dispatcher.send(:cleanup_concurrency)

      expect(Pgbus::Concurrency::Semaphore).to have_received(:expire_stale)
      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next).with("TestJob-42", client: mock_client)
      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:expire_stale)
    end

    it "promotes blocked executions atomically" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_return([{ "key" => "TestJob-42" }])
      allow(Pgbus::Concurrency::BlockedExecution).to receive_messages(expire_stale: 0, promote_next: true)

      dispatcher.send(:cleanup_concurrency)

      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next).with("TestJob-42", client: mock_client)
    end

    it "rescues errors gracefully" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_raise(StandardError, "db error")
      expect { dispatcher.send(:cleanup_concurrency) }.not_to raise_error
    end
  end

  describe "#cleanup_batches (private)" do
    it "cleans up finished batches older than 7 days" do
      allow(Pgbus::Batch).to receive(:cleanup).and_return(3)

      dispatcher.send(:cleanup_batches)

      expect(Pgbus::Batch).to have_received(:cleanup).with(older_than: a_kind_of(Time))
    end

    it "rescues errors gracefully" do
      allow(Pgbus::Batch).to receive(:cleanup).and_raise(StandardError, "db error")
      expect { dispatcher.send(:cleanup_batches) }.not_to raise_error
    end
  end

  describe "#compact_archives (private)" do
    let(:connection) { double("connection") }

    before do
      dispatcher.config.archive_retention = 7 * 24 * 3600
      prefix = dispatcher.config.queue_prefix
      allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
      allow(connection).to receive(:select_values).and_return(["#{prefix}_default", "#{prefix}_events"])
      allow(mock_client).to receive(:purge_archive).and_return(0)
    end

    it "purges archive entries older than retention period" do
      allow(mock_client).to receive(:purge_archive).and_return(5)

      dispatcher.send(:compact_archives)

      expect(mock_client).to have_received(:purge_archive).with("default", older_than: a_kind_of(Time), batch_size: 1000)
      expect(mock_client).to have_received(:purge_archive).with("events", older_than: a_kind_of(Time), batch_size: 1000)
    end

    it "skips when archive_retention is nil" do
      dispatcher.config.archive_retention = nil

      dispatcher.send(:compact_archives)

      expect(mock_client).not_to have_received(:purge_archive)
    end

    it "rescues errors gracefully" do
      allow(connection).to receive(:select_values).and_raise(StandardError, "db error")
      expect { dispatcher.send(:compact_archives) }.not_to raise_error
    end

    it "rescues per-queue errors without stopping others" do
      call_count = 0
      allow(mock_client).to receive(:purge_archive) do |queue_name, **_opts|
        call_count += 1
        raise StandardError, "fail" if queue_name == "default"

        3
      end

      dispatcher.send(:compact_archives)

      expect(call_count).to eq(2)
    end
  end

  describe "#run_maintenance includes archive compaction" do
    it "runs compact_archives when interval has elapsed" do
      allow(dispatcher).to receive(:compact_archives)

      dispatcher.instance_variable_set(:@last_archive_compaction_at, past_monotonic(3601))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:compact_archives)
    end
  end

  describe "#cleanup_recurring_executions (private)" do
    it "deletes old recurring execution records" do
      relation = instance_double(ActiveRecord::Relation, delete_all: 5)
      allow(Pgbus::RecurringExecution).to receive(:older_than).and_return(relation)

      dispatcher.send(:cleanup_recurring_executions)

      expect(Pgbus::RecurringExecution).to have_received(:older_than).with(a_kind_of(Time))
      expect(relation).to have_received(:delete_all)
    end

    it "rescues errors gracefully" do
      allow(Pgbus::RecurringExecution).to receive(:older_than).and_raise(StandardError, "db error")
      expect { dispatcher.send(:cleanup_recurring_executions) }.not_to raise_error
    end

    it "skips when retention is not positive" do
      allow(Pgbus.configuration).to receive(:recurring_execution_retention).and_return(0)
      allow(Pgbus::RecurringExecution).to receive(:older_than)

      dispatcher.send(:cleanup_recurring_executions)

      expect(Pgbus::RecurringExecution).not_to have_received(:older_than)
    end
  end

  describe "#cleanup_stats (private)" do
    before do
      stub_const("Pgbus::JobStat", Class.new) unless defined?(Pgbus::JobStat)
      allow(Pgbus::JobStat).to receive(:cleanup!).and_return(10)
    end

    it "cleans up old job stats" do
      dispatcher.send(:cleanup_stats)

      expect(Pgbus::JobStat).to have_received(:cleanup!).with(older_than: a_kind_of(Time))
    end

    it "skips when stats_enabled is false" do
      allow(dispatcher.config).to receive(:stats_enabled).and_return(false)

      dispatcher.send(:cleanup_stats)

      expect(Pgbus::JobStat).not_to have_received(:cleanup!)
    end

    it "skips when stats_retention is not positive" do
      allow(dispatcher.config).to receive(:stats_retention).and_return(0)

      dispatcher.send(:cleanup_stats)

      expect(Pgbus::JobStat).not_to have_received(:cleanup!)
    end
  end

  describe "#start_heartbeat (private)" do
    it "creates and starts a heartbeat" do
      dispatcher.send(:start_heartbeat)
      expect(Pgbus::Process::Heartbeat).to have_received(:new).with(kind: "dispatcher", metadata: { pid: Process.pid })
      expect(heartbeat).to have_received(:start)
    end
  end
end
