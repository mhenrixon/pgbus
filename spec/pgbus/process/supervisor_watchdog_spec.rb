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

    # Pipe-based liveness fallback: when the database is unreachable the
    # Heartbeat can't write loop_tick_at and the watchdog DB read raises.
    # The OS pipe channel keeps stall detection working during the outage.
    describe "pipe-liveness fallback (DB-independent)" do
      let(:now) { Process.clock_gettime(Process::CLOCK_MONOTONIC) }

      before do
        supervisor.instance_variable_set(:@last_watchdog_at, 0.0)
        allow(Process).to receive(:kill)
      end

      def set_forks(pid, last_pipe_tick_at:, pipe_seen:)
        supervisor.instance_variable_set(:@forks, {
                                           pid => { type: :worker, config: { queues: ["default"] },
                                                    liveness_reader: nil,
                                                    last_pipe_tick_at: last_pipe_tick_at,
                                                    pipe_seen: pipe_seen }
                                         })
      end

      it "SIGKILLs a worker with a stale pipe tick when the DB read raises" do
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        set_forks(1001, last_pipe_tick_at: now - 120, pipe_seen: true)

        supervisor.send(:check_stalled_workers)

        expect(Process).to have_received(:kill).with("KILL", 1001)
      end

      it "does NOT kill a worker with a fresh pipe tick when the DB read raises" do
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        set_forks(1001, last_pipe_tick_at: now - 1, pipe_seen: true)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "does NOT kill a worker whose pipe is unarmed (booting, no byte seen yet)" do
        allow(Pgbus::ProcessEntry).to receive(:where).and_raise(StandardError, "connection lost")
        # Seeded far in the past but never armed — a slow-booting worker.
        set_forks(1001, last_pipe_tick_at: now - 999, pipe_seen: false)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "keeps a worker alive on a stale DB row when the pipe tick is fresh (fresh-wins)" do
        stale_db = double("ProcessEntry", kind: "worker", pid: 1001,
                                          metadata: { "loop_tick_at" => (Time.now.to_f - 300) })
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(kind: "worker", pid: [1001])
          .and_return(double(to_a: [stale_db]))
        set_forks(1001, last_pipe_tick_at: now - 1, pipe_seen: true)

        supervisor.send(:check_stalled_workers)

        expect(Process).not_to have_received(:kill)
      end

      it "kills a worker when BOTH the DB row and the pipe tick are stale" do
        stale_db = double("ProcessEntry", kind: "worker", pid: 1001,
                                          metadata: { "loop_tick_at" => (Time.now.to_f - 300) })
        allow(Pgbus::ProcessEntry).to receive(:where)
          .with(kind: "worker", pid: [1001])
          .and_return(double(to_a: [stale_db]))
        set_forks(1001, last_pipe_tick_at: now - 120, pipe_seen: true)

        supervisor.send(:check_stalled_workers)

        expect(Process).to have_received(:kill).with("KILL", 1001)
      end
    end

    # The watchdog draining side: monitor_loop advances last_pipe_tick_at and
    # arms pipe_seen when a worker's pipe has readable bytes.
    describe "drain_liveness_pipes (private)" do
      it "advances last_pipe_tick_at and arms pipe_seen when bytes are readable" do
        reader, writer = IO.pipe
        writer.write("\0\0")
        supervisor.instance_variable_set(:@forks, {
                                           1001 => { type: :worker, liveness_reader: reader,
                                                     last_pipe_tick_at: 0.0, pipe_seen: false }
                                         })

        supervisor.send(:drain_liveness_pipes)

        info = supervisor.instance_variable_get(:@forks)[1001]
        expect(info[:pipe_seen]).to be(true)
        expect(info[:last_pipe_tick_at]).to be > 0.0
      ensure
        reader.close unless reader.closed?
        writer.close unless writer.closed?
      end

      it "does not arm pipe_seen when the pipe is empty" do
        reader, writer = IO.pipe
        supervisor.instance_variable_set(:@forks, {
                                           1001 => { type: :worker, liveness_reader: reader,
                                                     last_pipe_tick_at: 0.0, pipe_seen: false }
                                         })

        supervisor.send(:drain_liveness_pipes)

        expect(supervisor.instance_variable_get(:@forks)[1001][:pipe_seen]).to be(false)
      ensure
        reader.close unless reader.closed?
        writer.close unless writer.closed?
      end

      it "does not raise when a reader was closed by a racing reap" do
        reader, writer = IO.pipe
        reader.close
        writer.close
        supervisor.instance_variable_set(:@forks, {
                                           1001 => { type: :worker, liveness_reader: reader,
                                                     last_pipe_tick_at: 0.0, pipe_seen: false }
                                         })

        expect { supervisor.send(:drain_liveness_pipes) }.not_to raise_error
      end
    end
  end
end
