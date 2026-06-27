# frozen_string_literal: true

require "spec_helper"
require "json"
require "socket"

RSpec.describe Pgbus::Process::Heartbeat do
  describe "#beat with loop_tick_supplier" do
    let(:timer) { instance_double(Concurrent::TimerTask, execute: true, shutdown: true) }
    let(:process_record) { double("ProcessEntry", id: 42) }
    let(:scope) { double("scope") }

    before do
      allow(Concurrent::TimerTask).to receive(:new).and_return(timer)
      allow(Pgbus::ProcessEntry).to receive(:create!).and_return(process_record)
      allow(Pgbus::ProcessEntry).to receive(:where).with(id: 42).and_return(scope)
      allow(scope).to receive(:update_all).and_return(1)
    end

    context "when loop_tick_supplier is provided" do
      let(:tick_value) { 12_345.678 }
      let(:supplier) { -> { tick_value } }
      let(:heartbeat) do
        described_class.new(
          kind: "worker",
          metadata: { queues: %w[default], threads: 5 },
          loop_tick_supplier: supplier
        )
      end

      before { heartbeat.start }

      it "includes loop_tick_at in metadata on beat" do
        heartbeat.beat

        expect(scope).to have_received(:update_all) do |args|
          expect(args[:last_heartbeat_at]).to be_a(Time)
          expect(args[:metadata]).to include("loop_tick_at" => tick_value)
        end
      end

      it "preserves original metadata keys" do
        heartbeat.beat

        expect(scope).to have_received(:update_all) do |args|
          expect(args[:metadata]).to include(queues: %w[default], threads: 5)
        end
      end
    end

    context "when loop_tick_supplier returns nil" do
      let(:heartbeat) do
        described_class.new(
          kind: "worker",
          metadata: { queues: %w[default] },
          loop_tick_supplier: -> {}
        )
      end

      before { heartbeat.start }

      it "writes nil loop_tick_at" do
        heartbeat.beat

        expect(scope).to have_received(:update_all) do |args|
          expect(args[:metadata]).to include("loop_tick_at" => nil)
        end
      end
    end

    context "when loop_tick_supplier is not provided" do
      let(:heartbeat) do
        described_class.new(kind: "dispatcher", metadata: { pid: 123 })
      end

      before { heartbeat.start }

      it "does not include metadata in update" do
        heartbeat.beat

        expect(scope).to have_received(:update_all) do |args|
          expect(args).not_to have_key(:metadata)
          expect(args[:last_heartbeat_at]).to be_a(Time)
        end
      end
    end
  end
end
