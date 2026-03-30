# frozen_string_literal: true

require "spec_helper"
require "json"
require "socket"

RSpec.describe Pgbus::Process::Heartbeat do
  let(:timer) { instance_double(Concurrent::TimerTask, execute: true, shutdown: true) }
  let(:connection) { double("AR::Connection", execute: nil, exec_insert: [{ "id" => 42 }]) }
  let(:heartbeat) { described_class.new(kind: "worker", metadata: { queues: %w[default] }) }

  before do
    allow(Concurrent::TimerTask).to receive(:new).and_return(timer)
  end

  describe "#start" do
    context "when ActiveRecord is defined" do
      before do
        stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
      end

      it "registers the process via ActiveRecord" do
        heartbeat.start
        expect(connection).to have_received(:exec_insert)
      end

      it "creates and executes a timer task" do
        heartbeat.start
        expect(Concurrent::TimerTask).to have_received(:new).with(execution_interval: described_class::INTERVAL)
        expect(timer).to have_received(:execute)
      end
    end

    context "when ActiveRecord is not defined" do
      before { hide_const("ActiveRecord") }

      it "still creates and executes a timer task" do
        heartbeat.start
        expect(timer).to have_received(:execute)
      end
    end
  end

  describe "#beat" do
    context "when process_id is set and ActiveRecord is defined" do
      before do
        stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
        heartbeat.start
      end

      it "updates the heartbeat timestamp with bind params" do
        heartbeat.beat
        expect(connection).to have_received(:execute).with(
          "UPDATE pgbus_processes SET last_heartbeat_at = NOW() WHERE id = $1",
          "Pgbus Heartbeat",
          [42]
        )
      end
    end

    context "when process_id is nil" do
      before { hide_const("ActiveRecord") }

      it "does nothing" do
        expect { heartbeat.beat }.not_to raise_error
      end
    end

    context "when ActiveRecord raises an error" do
      before do
        stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
        heartbeat.start
        # After registration succeeds, make execute raise for beat
        allow(connection).to receive(:execute).and_raise(StandardError, "connection lost")
      end

      it "logs a warning instead of raising" do
        expect { heartbeat.beat }.not_to raise_error
      end
    end
  end

  describe "#stop" do
    before do
      stub_const("ActiveRecord::Base", double("ActiveRecord::Base", connection: connection))
      heartbeat.start
    end

    it "shuts down the timer" do
      heartbeat.stop
      expect(timer).to have_received(:shutdown)
    end

    it "deregisters the process with bind params" do
      heartbeat.stop
      expect(connection).to have_received(:execute).with(
        "DELETE FROM pgbus_processes WHERE id = $1",
        "Pgbus Deregister Process",
        [42]
      )
    end
  end
end
