# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Supervisor do
  let(:mock_heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: nil, stop: nil) }
  let(:config) { Pgbus.configuration }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(mock_heartbeat)
  end

  describe "#initialize" do
    it "stores config and initializes empty forks" do
      supervisor = described_class.new

      expect(supervisor.config).to eq(config)
      expect(supervisor.instance_variable_get(:@forks)).to eq({})
      expect(supervisor.instance_variable_get(:@shutting_down)).to be false
    end
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down and signals children with TERM" do
      supervisor = described_class.new
      supervisor.instance_variable_set(:@forks, { 1001 => { type: :worker }, 1002 => { type: :dispatcher } })

      allow(Process).to receive(:kill)
      supervisor.graceful_shutdown

      expect(supervisor.instance_variable_get(:@shutting_down)).to be true
      expect(Process).to have_received(:kill).with("TERM", 1001)
      expect(Process).to have_received(:kill).with("TERM", 1002)
    end
  end

  describe "#immediate_shutdown" do
    it "sets shutting_down and signals children with QUIT" do
      supervisor = described_class.new
      supervisor.instance_variable_set(:@forks, { 2001 => { type: :worker } })

      allow(Process).to receive(:kill)
      supervisor.immediate_shutdown

      expect(supervisor.instance_variable_get(:@shutting_down)).to be true
      expect(Process).to have_received(:kill).with("QUIT", 2001)
    end
  end

  describe "signal_children (private)" do
    it "handles Errno::ESRCH when a child process is already gone" do
      supervisor = described_class.new
      supervisor.instance_variable_set(:@forks, { 9999 => { type: :worker } })

      allow(Process).to receive(:kill).with("TERM", 9999).and_raise(Errno::ESRCH)

      expect { supervisor.send(:signal_children, "TERM") }.not_to raise_error
    end
  end

  describe "reap_children (private)" do
    let(:supervisor) { described_class.new }
    let(:status_double) { instance_double(Process::Status, exitstatus: 1, success?: false) }

    before do
      supervisor.instance_variable_set(:@forks, { 3001 => { type: :worker, config: { queues: ["default"] } } })
    end

    it "restarts a child when not shutting down" do
      allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, status_double], nil)
      allow(supervisor).to receive(:fork).and_return(4001)

      supervisor.send(:reap_children)

      expect(supervisor).to have_received(:fork)
    end

    it "does NOT restart a child when shutting_down is true" do
      supervisor.instance_variable_set(:@shutting_down, true)
      allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, status_double], nil)

      supervisor.send(:reap_children)

      forks = supervisor.instance_variable_get(:@forks)
      expect(forks).not_to have_key(3001)
    end
  end

  # A child that dies right after forking (bad config, unreachable DB,
  # crashing initializer) must not be re-forked in a tight loop — that
  # burns CPU and floods logs. Crashes within the stable-uptime window
  # get an exponentially backed-off restart; stable children and clean
  # exits (worker recycling) restart immediately.
  describe "restart backoff for crash-looping children" do
    let(:supervisor) { described_class.new }
    let(:crash_status) { instance_double(Process::Status, exitstatus: 1, success?: false) }
    let(:clean_status) { instance_double(Process::Status, exitstatus: 0, success?: true) }
    let(:worker_config) { { queues: ["default"], threads: 5 } }

    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    before do
      allow(supervisor).to receive(:fork).and_return(6001)
      allow(Pgbus.logger).to receive(:warn)
    end

    it "restarts immediately when the crashed child had a stable uptime" do
      supervisor.instance_variable_set(
        :@forks, { 3001 => { type: :worker, config: worker_config, spawned_at: now - 120 } }
      )
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)

      supervisor.send(:reap_children)

      expect(supervisor).to have_received(:fork).once
    end

    it "restarts immediately on a clean exit even with short uptime (worker recycling)" do
      supervisor.instance_variable_set(
        :@forks, { 3001 => { type: :worker, config: worker_config, spawned_at: now - 5 } }
      )
      allow(Process).to receive(:waitpid2).and_return([3001, clean_status], nil)

      supervisor.send(:reap_children)

      expect(supervisor).to have_received(:fork).once
    end

    it "delays the restart when the child crashed shortly after forking" do
      base = now
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      supervisor.instance_variable_set(
        :@forks, { 3001 => { type: :worker, config: worker_config, spawned_at: base - 1 } }
      )
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)

      supervisor.send(:reap_children)
      supervisor.send(:process_pending_restarts)

      expect(supervisor).not_to have_received(:fork)
    end

    it "keeps separate crash streaks for identically-configured sibling workers" do
      messages = []
      allow(Pgbus.logger).to receive(:warn) { |&blk| messages << blk.call }
      allow(supervisor).to receive(:fork).and_return(6001, 6002, 6003)

      base = now
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      supervisor.instance_variable_set(:@forks, sibling_forks(base))

      reap(3002, crash_status) # slot 1 crashes fast — crash #1, 1s backoff
      reap(3001, clean_status) # slot 0 recycles cleanly — must NOT reset slot 1's streak

      # Slot 1's restart fires, then it crashes fast again — crash #2, 2s backoff.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)
      restarted_pid = supervisor.instance_variable_get(:@forks).find { |_, i| i[:slot] == 1 }.first
      reap(restarted_pid, crash_status)

      expect(messages.join("\n")).to include("restarting in 2s")
    end

    def sibling_forks(base)
      { 3001 => { type: :worker, config: worker_config, slot: 0, spawned_at: base },
        3002 => { type: :worker, config: worker_config, slot: 1, spawned_at: base } }
    end

    def reap(pid, status)
      allow(Process).to receive(:waitpid2).and_return([pid, status], nil)
      supervisor.send(:reap_children)
    end

    it "fires due pending restarts from the monitor loop" do
      allow(supervisor).to receive(:fork).and_return(7001)
      allow(Process).to receive(:waitpid2).and_return(nil)
      supervisor.instance_variable_set(
        :@pending_restarts, [{ info: { type: :dispatcher }, at: now - 1 }]
      )
      # Stop the loop after one pass.
      allow(supervisor).to receive(:interruptible_sleep) do
        supervisor.instance_variable_set(:@shutting_down, true)
        supervisor.instance_variable_set(:@forks, {})
      end
      allow(supervisor).to receive(:check_stalled_workers)

      supervisor.send(:monitor_loop)

      expect(supervisor).to have_received(:fork).once
    end

    it "does not fire pending restarts once shutting down" do
      allow(supervisor).to receive(:fork)
      supervisor.instance_variable_set(:@shutting_down, true)
      supervisor.instance_variable_set(
        :@forks, { 8001 => { type: :worker, config: worker_config, spawned_at: now } }
      )
      supervisor.instance_variable_set(
        :@pending_restarts, [{ info: { type: :dispatcher }, at: now - 1 }]
      )
      allow(Process).to receive(:waitpid2).and_return([8001, crash_status], nil)
      allow(supervisor).to receive(:interruptible_sleep)

      supervisor.send(:monitor_loop)

      expect(supervisor).not_to have_received(:fork)
    end

    it "runs the pending restart once its backoff has elapsed" do
      base = now
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      supervisor.instance_variable_set(
        :@forks, { 3001 => { type: :worker, config: worker_config, spawned_at: base } }
      )
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)

      supervisor.send(:reap_children)
      expect(supervisor).not_to have_received(:fork)

      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)

      expect(supervisor).to have_received(:fork)
    end

    it "escalates the backoff on consecutive rapid crashes" do
      messages = []
      allow(Pgbus.logger).to receive(:warn) { |&blk| messages << blk.call }

      base = now
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      supervisor.instance_variable_set(
        :@forks, { 3001 => { type: :worker, config: worker_config, spawned_at: base } }
      )
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)
      supervisor.send(:reap_children)

      # First backoff elapses; the child is restarted, then crashes again fast.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)
      allow(Process).to receive(:waitpid2).and_return([6001, crash_status], nil)
      supervisor.send(:reap_children)

      joined = messages.join("\n")
      expect(joined).to include("restarting in 1s")
      expect(joined).to include("restarting in 2s")
    end

    it "resets the crash streak after a stable run" do
      messages = []
      allow(Pgbus.logger).to receive(:warn) { |&blk| messages << blk.call }

      base = now
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      supervisor.instance_variable_set(
        :@forks, { 3001 => { type: :worker, config: worker_config, spawned_at: base } }
      )
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)
      supervisor.send(:reap_children)

      # Backoff elapses, child restarts and then runs well past the stable window.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)

      # Stable crash: restarts immediately and clears the streak...
      allow(supervisor).to receive(:monotonic_now).and_return(base + 200)
      allow(Process).to receive(:waitpid2).and_return([6001, crash_status], nil)
      supervisor.send(:reap_children)

      # ...so the next rapid crash starts back at the base backoff.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 201)
      allow(Process).to receive(:waitpid2).and_return([6001, crash_status], nil)
      supervisor.send(:reap_children)

      expect(messages.join("\n").scan("restarting in 1s").size).to eq(2)
    end
  end

  describe "restart_child (private)" do
    let(:supervisor) { described_class.new }

    before do
      allow(supervisor).to receive(:fork).and_return(5001)
    end

    it "routes :worker to fork_worker" do
      info = { type: :worker, config: { queues: ["default"], threads: 5 } }
      supervisor.send(:restart_child, info)

      expect(supervisor.instance_variable_get(:@forks)).to have_key(5001)
      expect(supervisor.instance_variable_get(:@forks)[5001][:type]).to eq(:worker)
    end

    it "routes :dispatcher to fork_dispatcher" do
      info = { type: :dispatcher }
      supervisor.send(:restart_child, info)

      expect(supervisor.instance_variable_get(:@forks)).to have_key(5001)
      expect(supervisor.instance_variable_get(:@forks)[5001][:type]).to eq(:dispatcher)
    end

    it "routes :consumer to fork_consumer" do
      info = { type: :consumer, config: { topics: ["orders.#"], threads: 3 } }
      supervisor.send(:restart_child, info)

      expect(supervisor.instance_variable_get(:@forks)).to have_key(5001)
      expect(supervisor.instance_variable_get(:@forks)[5001][:type]).to eq(:consumer)
    end

    it "routes :scheduler to fork_scheduler" do
      info = { type: :scheduler }
      supervisor.send(:restart_child, info)

      expect(supervisor.instance_variable_get(:@forks)).to have_key(5001)
      expect(supervisor.instance_variable_get(:@forks)[5001][:type]).to eq(:scheduler)
    end
  end

  describe "bootstrap_queues (private)" do
    let(:supervisor) { described_class.new }
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive(:ensure_all_queues)
    end

    it "calls ensure_all_queues on the client" do
      supervisor.send(:bootstrap_queues)

      expect(mock_client).to have_received(:ensure_all_queues).once
    end

    it "rescues errors and logs them" do
      allow(mock_client).to receive(:ensure_all_queues).and_raise(StandardError, "connection failed")
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.send(:bootstrap_queues) }.not_to raise_error
      expect(Pgbus.logger).to have_received(:error).at_least(:once)
    end
  end

  describe "pre-fork bootstrap" do
    let(:supervisor) { described_class.new }
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive(:ensure_all_queues)
      allow(mock_client).to receive(:verify_connection!).and_return(true)
    end

    it "calls bootstrap_queues before boot_processes" do
      call_order = []
      allow(supervisor).to receive(:bootstrap_queues) { call_order << :bootstrap }
      allow(supervisor).to receive(:boot_processes) { call_order << :boot }
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      supervisor.run

      expect(call_order).to eq(%i[bootstrap boot])
    end

    it "verifies the database connection once before bootstrapping queues" do
      call_order = []
      allow(mock_client).to receive(:verify_connection!) { call_order << :verify }
      allow(supervisor).to receive(:bootstrap_queues) { call_order << :bootstrap }
      allow(supervisor).to receive(:boot_processes) { call_order << :boot }
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      supervisor.run

      expect(call_order).to eq(%i[verify bootstrap boot])
      expect(mock_client).to have_received(:verify_connection!).once
    end

    it "propagates a ConfigurationError from verification without booting children" do
      allow(mock_client).to receive(:verify_connection!)
        .and_raise(Pgbus::ConfigurationError, "Database connection failed via database_url: nope")
      allow(supervisor).to receive(:bootstrap_queues)
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      expect { supervisor.run }.to raise_error(Pgbus::ConfigurationError, /database_url/)

      expect(supervisor).not_to have_received(:bootstrap_queues)
      expect(supervisor).not_to have_received(:boot_processes)
    end

    it "rescues bootstrap errors in the parent without aborting" do
      allow(supervisor).to receive(:bootstrap_queues).and_call_original
      allow(mock_client).to receive(:ensure_all_queues).and_raise(StandardError, "pg down")
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.run }.not_to raise_error
    end
  end

  describe "recurring_tasks_configured? (private)" do
    let(:supervisor) { described_class.new }

    it "returns true when recurring_tasks are set in config" do
      config.recurring_tasks = { "task1" => { "class" => "MyJob", "schedule" => "0 * * * *" } }
      expect(supervisor.send(:recurring_tasks_configured?)).to be true
      config.recurring_tasks = nil
    end

    it "returns false when nothing is configured" do
      config.recurring_tasks = nil
      config.recurring_tasks_file = nil
      expect(supervisor.send(:recurring_tasks_configured?)).to be false
    end
  end
end
