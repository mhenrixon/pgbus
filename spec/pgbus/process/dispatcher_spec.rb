# frozen_string_literal: true

require "spec_helper"
require "json"
require "socket"

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

  describe "#dispatch_scheduled (private)" do
    context "when ActiveRecord is not defined" do
      before { hide_const("ActiveRecord") }

      it "returns 0" do
        expect(dispatcher.send(:dispatch_scheduled)).to eq(0)
      end
    end

    context "when ActiveRecord is defined" do
      let(:connection) { double("AR::Connection") }
      let(:due_events) do
        [
          { "queue_name" => "pgbus_test_default", "payload" => '{"job_class":"TestJob"}', "headers" => nil }
        ]
      end

      before do
        stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
        allow(connection).to receive(:execute).and_return(due_events)
      end

      it "fetches due events and enqueues them" do
        result = dispatcher.send(:dispatch_scheduled)
        expect(result).to eq(1)
        expect(mock_client).to have_received(:send_message).with("pgbus_test_default", { "job_class" => "TestJob" }, headers: nil)
      end

      it "returns 0 and logs when enqueue raises" do
        allow(mock_client).to receive(:send_message).and_raise(StandardError, "enqueue failed")
        allow(connection).to receive(:quote) { |v| "'#{v}'" }

        result = dispatcher.send(:dispatch_scheduled)
        expect(result).to eq(0)
      end
    end

    context "when fetch_due_events raises" do
      let(:connection) { double("AR::Connection") }

      before do
        stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
        allow(connection).to receive(:execute).and_raise(StandardError, "connection lost")
      end

      it "returns 0" do
        expect(dispatcher.send(:dispatch_scheduled)).to eq(0)
      end
    end
  end

  describe "#enqueue_event (private)" do
    it "sends a message via Pgbus.client with parsed payload and headers" do
      row = { "queue_name" => "pgbus_test_jobs", "payload" => '{"foo":"bar"}', "headers" => '{"h":"v"}' }
      dispatcher.send(:enqueue_event, row)
      expect(mock_client).to have_received(:send_message).with("pgbus_test_jobs", { "foo" => "bar" }, headers: { "h" => "v" })
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
