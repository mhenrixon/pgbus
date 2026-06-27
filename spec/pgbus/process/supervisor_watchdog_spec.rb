# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Supervisor do
  describe "#check_stalled_workers" do
    let(:mock_heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: nil, stop: nil) }
    let(:config) { Pgbus.configuration }
    let(:supervisor) { described_class.new }

    before do
      allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(mock_heartbeat)
      config.stall_threshold = 90
    end

    after do
      config.stall_threshold = 90
    end

    describe "check_stalled_workers (private)" do
      let(:stalled_entry) do
        double("ProcessEntry",
               kind: "worker",
               pid: 1001,
               metadata: { "loop_tick_at" => (Time.now.to_f - 120) })
      end

      let(:healthy_entry) do
        double("ProcessEntry",
               kind: "worker",
               pid: 1002,
               metadata: { "loop_tick_at" => Time.now.to_f })
      end

      before do
        supervisor.instance_variable_set(:@forks, {
                                           1001 => { type: :worker, config: { queues: ["default"] } },
                                           1002 => { type: :worker, config: { queues: ["priority"] } }
                                         })
        supervisor.instance_variable_set(:@last_watchdog_at, 0.0)
      end

      it "kills a worker whose loop_tick_at exceeds stall_threshold" do
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(kind: "worker", pid: [1001, 1002])
          .and_return(double(to_a: [stalled_entry, healthy_entry]))
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Process).to have_received(:kill).with("KILL", 1001)
        expect(Process).not_to have_received(:kill).with("KILL", 1002)
      end

      it "does not kill a worker within threshold" do
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(kind: "worker", pid: [1001, 1002])
          .and_return(double(to_a: [healthy_entry]))
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "skips when stall_threshold is nil" do
        config.stall_threshold = nil
        allow(Pgbus::ProcessEntry).to receive(:where)
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Pgbus::ProcessEntry).not_to have_received(:where)
        expect(Process).not_to have_received(:kill)
      end

      it "handles Errno::ESRCH when process is already gone" do
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(kind: "worker", pid: [1001, 1002])
          .and_return(double(to_a: [stalled_entry]))
        allow(Process).to receive(:kill).with("KILL", 1001).and_raise(Errno::ESRCH)

        expect { supervisor.send(:check_stalled_workers) }.not_to raise_error
      end

      it "skips entries without loop_tick_at in metadata" do
        entry_no_tick = double("ProcessEntry",
                               kind: "worker", pid: 1001,
                               metadata: { "queues" => ["default"] })
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(kind: "worker", pid: [1001, 1002])
          .and_return(double(to_a: [entry_no_tick]))
        allow(Process).to receive(:kill)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "respects the WATCHDOG_INTERVAL rate limiter" do
        supervisor.instance_variable_set(:@last_watchdog_at,
                                         Process.clock_gettime(Process::CLOCK_MONOTONIC))
        allow(Pgbus::ProcessEntry).to receive(:where)

        supervisor.send(:check_stalled_workers)

        expect(Pgbus::ProcessEntry).not_to have_received(:where)
      end

      it "does not check non-worker forks" do
        supervisor.instance_variable_set(:@forks, {
                                           2001 => { type: :dispatcher },
                                           2002 => { type: :scheduler }
                                         })

        allow(Pgbus::ProcessEntry).to receive(:where)

        supervisor.send(:check_stalled_workers)

        expect(Pgbus::ProcessEntry).not_to have_received(:where)
      end

      it "rescues and logs DB errors" do
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")

        expect { supervisor.send(:check_stalled_workers) }.not_to raise_error
      end
    end
  end
end
