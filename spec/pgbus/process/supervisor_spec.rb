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
    let(:status_double) { instance_double(Process::Status, exitstatus: 1) }

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
