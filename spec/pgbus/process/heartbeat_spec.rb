# frozen_string_literal: true

require "spec_helper"
require "json"
require "socket"

RSpec.describe Pgbus::Process::Heartbeat do
  let(:timer) { instance_double(Concurrent::TimerTask, execute: true, shutdown: true) }
  let(:process_record) { double("ProcessEntry", id: 42) }
  let(:heartbeat) { described_class.new(kind: "worker", metadata: { queues: %w[default] }) }

  before do
    allow(Concurrent::TimerTask).to receive(:new).and_return(timer)
  end

  describe "#start" do
    before do
      allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
    end

    it "registers the process via ProcessEntry" do
      heartbeat.start

      expect(Pgbus::ProcessEntry).to have_received(:create!).with(
        hash_including(kind: "worker", pid: Process.pid)
      )
    end

    it "creates and executes a timer task" do
      heartbeat.start

      expect(Concurrent::TimerTask).to have_received(:new).with(execution_interval: described_class::INTERVAL)
      expect(timer).to have_received(:execute)
    end
  end

  describe "#beat" do
    context "when process_id is set" do
      let(:scope) { double("scope", update_all: 1) }

      before do
        allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
        allow(Pgbus::ProcessEntry).to receive(:where).with(id: 42).and_return(scope)
        heartbeat.start
      end

      it "updates the heartbeat timestamp" do
        heartbeat.beat

        expect(scope).to have_received(:update_all).with(last_heartbeat_at: an_instance_of(Time))
      end
    end

    context "when process_id is nil" do
      it "does nothing" do
        expect { heartbeat.beat }.not_to raise_error
      end
    end

    context "when update raises an error" do
      before do
        allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        heartbeat.start
      end

      it "logs a warning instead of raising" do
        expect { heartbeat.beat }.not_to raise_error
      end
    end
  end

  describe "#stop" do
    let(:scope) { double("scope", delete_all: 1) }

    before do
      allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
      allow(Pgbus::ProcessEntry).to receive(:where).with(id: 42).and_return(scope)
      heartbeat.start
    end

    it "shuts down the timer" do
      heartbeat.stop

      expect(timer).to have_received(:shutdown)
    end

    it "deregisters the process" do
      heartbeat.stop

      expect(scope).to have_received(:delete_all)
    end
  end
end
