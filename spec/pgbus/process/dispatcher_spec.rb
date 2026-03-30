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
    it "calls cleanup_processed_events and reap_stale_processes" do
      allow(dispatcher).to receive_messages(cleanup_processed_events: nil, reap_stale_processes: nil)
      dispatcher.send(:run_maintenance)
      expect(dispatcher).to have_received(:cleanup_processed_events)
      expect(dispatcher).to have_received(:reap_stale_processes)
    end

    it "rescues errors from maintenance methods" do
      allow(dispatcher).to receive(:cleanup_processed_events).and_raise(StandardError, "boom")
      expect { dispatcher.send(:run_maintenance) }.not_to raise_error
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
