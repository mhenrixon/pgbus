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
    context "when ActiveRecord is not defined" do
      before { hide_const("ActiveRecord") }

      it "returns early" do
        expect(dispatcher.send(:cleanup_processed_events)).to be_nil
      end
    end

    context "when ActiveRecord is defined" do
      let(:connection) { double("AR::Connection") }

      before do
        stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
      end

      it "deletes expired processed events with bind params" do
        allow(connection).to receive(:delete).and_return(5)
        dispatcher.send(:cleanup_processed_events)
        expect(connection).to have_received(:delete).with(
          "DELETE FROM pgbus_processed_events WHERE processed_at < $1",
          "Pgbus Idempotency Cleanup",
          [an_instance_of(Time)]
        )
      end

      it "returns early when idempotency_ttl is not set" do
        original_ttl = dispatcher.config.idempotency_ttl
        dispatcher.config.idempotency_ttl = nil
        allow(connection).to receive(:delete)
        dispatcher.send(:cleanup_processed_events)
        expect(connection).not_to have_received(:delete)
      ensure
        dispatcher.config.idempotency_ttl = original_ttl
      end

      it "rescues StandardError and logs a warning" do
        allow(connection).to receive(:delete).and_raise(StandardError, "db error")
        expect { dispatcher.send(:cleanup_processed_events) }.not_to raise_error
      end
    end
  end

  describe "#reap_stale_processes (private)" do
    context "when ActiveRecord is not defined" do
      before { hide_const("ActiveRecord") }

      it "returns early" do
        expect(dispatcher.send(:reap_stale_processes)).to be_nil
      end
    end

    context "when ActiveRecord is defined" do
      let(:connection) { double("AR::Connection") }

      before do
        stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
      end

      it "deletes stale processes with bind params" do
        allow(connection).to receive(:delete).and_return(2)
        dispatcher.send(:reap_stale_processes)
        expect(connection).to have_received(:delete).with(
          "DELETE FROM pgbus_processes WHERE last_heartbeat_at < $1",
          "Pgbus Stale Process Reap",
          [an_instance_of(Time)]
        )
      end

      it "rescues StandardError and logs a warning" do
        allow(connection).to receive(:delete).and_raise(StandardError, "db error")
        expect { dispatcher.send(:reap_stale_processes) }.not_to raise_error
      end
    end
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

      dispatcher.instance_variable_set(:@last_cleanup_at, Time.now - described_class::CLEANUP_INTERVAL - 1)
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:cleanup_processed_events)
    end

    it "runs reap when reap interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_processed_events)
      allow(dispatcher).to receive(:reap_stale_processes)

      dispatcher.instance_variable_set(:@last_reap_at, Time.now - described_class::REAP_INTERVAL - 1)
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:reap_stale_processes)
    end

    it "runs concurrency cleanup when concurrency interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_concurrency)

      dispatcher.instance_variable_set(:@last_concurrency_at, Time.now - described_class::CONCURRENCY_INTERVAL - 1)
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:cleanup_concurrency)
    end

    it "rescues errors from maintenance methods" do
      dispatcher.instance_variable_set(:@last_cleanup_at, Time.now - described_class::CLEANUP_INTERVAL - 1)
      allow(dispatcher).to receive(:cleanup_processed_events).and_raise(StandardError, "boom")
      expect { dispatcher.send(:run_maintenance) }.not_to raise_error
    end
  end

  describe "#cleanup_concurrency (private)" do
    it "expires stale semaphores and releases blocked executions" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_return([{ "key" => "TestJob-42" }])
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:expire_stale).and_return(0)
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:release_next).and_return(nil)

      dispatcher.send(:cleanup_concurrency)

      expect(Pgbus::Concurrency::Semaphore).to have_received(:expire_stale)
      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:release_next).with("TestJob-42")
      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:expire_stale)
    end

    it "enqueues released blocked executions" do
      released = { queue_name: "default", payload: { "job_class" => "TestJob" } }
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_return([{ "key" => "TestJob-42" }])
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:expire_stale).and_return(0)
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:release_next).and_return(released)

      dispatcher.send(:cleanup_concurrency)

      expect(mock_client).to have_received(:send_message).with("default", { "job_class" => "TestJob" })
    end

    it "rescues errors gracefully" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_raise(StandardError, "db error")
      expect { dispatcher.send(:cleanup_concurrency) }.not_to raise_error
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
