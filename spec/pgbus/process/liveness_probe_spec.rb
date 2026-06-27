# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Worker do
  describe "liveness probe" do
    let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
    let(:mock_client) { build_mock_client }
    let(:executor) { instance_double(Pgbus::ActiveJob::Executor) }
    let(:pool) do
      instance_double(
        Pgbus::ExecutionPools::ThreadPool,
        capacity: 5, available_capacity: 5, idle?: true,
        shutdown: true, kill: true, wait_for_termination: true,
        metadata: { mode: :threads, capacity: 5, busy: 0 }
      )
    end
    let(:circuit_breaker) { instance_double(Pgbus::CircuitBreaker, paused?: false, record_success: nil, record_failure: nil) }

    before do
      allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(Pgbus::ActiveJob::Executor).to receive(:new).and_return(executor)
      allow(Pgbus::ExecutionPools).to receive(:build).and_return(pool)
      allow(Pgbus::CircuitBreaker).to receive(:new).and_return(circuit_breaker)
    end

    describe "loop beacon" do
      let(:worker) { described_class.new(queues: %w[default], threads: 5) }

      it "initializes @loop_tick_at as an AtomicReference" do
        tick = worker.instance_variable_get(:@loop_tick_at)
        expect(tick).to be_a(Concurrent::AtomicReference)
        expect(tick.get).to be_nil
      end

      it "passes loop_tick_supplier when starting heartbeat" do
        captured_args = nil
        allow(Pgbus::Process::Heartbeat).to receive(:new) do |**kwargs|
          captured_args = kwargs
          heartbeat
        end

        w = described_class.new(queues: %w[default], threads: 5)
        w.send(:start_heartbeat)

        expect(captured_args).to include(loop_tick_supplier: an_instance_of(Proc))
      end

      it "updates @loop_tick_at on each loop tick" do
        tick_ref = worker.instance_variable_get(:@loop_tick_at)
        expect(tick_ref.get).to be_nil

        tick_ref.set(123.456)
        expect(tick_ref.get).to eq(123.456)
      end
    end
  end
end
