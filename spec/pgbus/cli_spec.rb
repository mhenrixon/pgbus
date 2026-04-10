# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::CLI do
  let(:mock_client) { build_mock_client }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
  end

  describe ".start" do
    it "routes 'version' to version output" do
      expect { described_class.start(["version"]) }.to output("pgbus #{Pgbus::VERSION}\n").to_stdout
    end

    it "routes 'help' to help text" do
      expect { described_class.start(["help"]) }.to output(/Usage: pgbus/).to_stdout
    end

    it "routes unknown command to help text and exits with 1" do
      expect { described_class.start(["bogus"]) }.to raise_error(SystemExit) do |error|
        expect(error.status).to eq(1)
      end
    end

    it "defaults to help when no args" do
      expect { described_class.start([]) }.to output(/Usage: pgbus/).to_stdout
    end
  end

  describe ".start with start command" do
    let(:supervisor) { instance_double(Pgbus::Process::Supervisor, run: nil) }

    before do
      allow(Pgbus::Process::Supervisor).to receive(:new).and_return(supervisor)
      allow(Pgbus.logger).to receive(:info)
    end

    context "with --queues flag" do
      it "overrides Pgbus.configuration.workers from the CLI" do
        original_workers = Pgbus.configuration.workers
        described_class.start(["start", "--queues", "critical: 5; default: 10"])
        expect(Pgbus.configuration.workers.size).to eq(2)
        expect(Pgbus.configuration.workers.map { |c| c[:name] }).to eq(%w[critical default])
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
      end

      it "supports --queues=STRING with equals sign" do
        original_workers = Pgbus.configuration.workers
        described_class.start(["start", "--queues=*: 7"])
        # Wildcard capsules are anonymous (no auto-assigned :name) — see
        # the long comment on Pgbus::Configuration#workers= for the why.
        expect(Pgbus.configuration.workers).to eq([{ queues: ["*"], threads: 7 }])
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
      end

      it "raises a parse error for invalid queue strings" do
        expect do
          described_class.start(["start", "--queues", "default: 0"])
        end.to raise_error(Pgbus::Configuration::CapsuleDSL::ParseError)
      end
    end

    context "with --capsule flag" do
      it "filters workers to only the named capsule" do
        original_workers = Pgbus.configuration.workers
        Pgbus.configuration.instance_variable_set(:@workers, nil)
        Pgbus.configuration.capsule(:critical, queues: %w[critical], threads: 5)
        Pgbus.configuration.capsule(:default, queues: %w[default], threads: 10)

        described_class.start(["start", "--capsule", "critical"])

        expect(Pgbus.configuration.workers.size).to eq(1)
        expect(Pgbus.configuration.workers.first[:name]).to eq("critical")
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
      end

      it "raises when the named capsule does not exist" do
        original_workers = Pgbus.configuration.workers
        Pgbus.configuration.instance_variable_set(:@workers, nil)
        Pgbus.configuration.capsule(:critical, queues: %w[critical], threads: 5)

        expect do
          described_class.start(["start", "--capsule", "missing"])
        end.to raise_error(/no capsule named.*missing/i)
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
      end
    end

    context "without flags" do
      it "starts the supervisor with the existing workers config" do
        described_class.start(["start"])
        expect(supervisor).to have_received(:run)
      end

      it "leaves config.roles nil (boot every role)" do
        original_roles = Pgbus.configuration.roles
        described_class.start(["start"])
        expect(Pgbus.configuration.roles).to be_nil
      ensure
        Pgbus.configuration.roles = original_roles
      end
    end

    context "with --workers-only flag" do
      it "sets config.roles to [:workers]" do
        original_roles = Pgbus.configuration.roles
        described_class.start(["start", "--workers-only"])
        expect(Pgbus.configuration.roles).to eq([:workers])
      ensure
        Pgbus.configuration.roles = original_roles
      end
    end

    context "with --scheduler-only flag" do
      it "sets config.roles to [:scheduler]" do
        original_roles = Pgbus.configuration.roles
        described_class.start(["start", "--scheduler-only"])
        expect(Pgbus.configuration.roles).to eq([:scheduler])
      ensure
        Pgbus.configuration.roles = original_roles
      end
    end

    context "with --dispatcher-only flag" do
      it "sets config.roles to [:dispatcher]" do
        original_roles = Pgbus.configuration.roles
        described_class.start(["start", "--dispatcher-only"])
        expect(Pgbus.configuration.roles).to eq([:dispatcher])
      ensure
        Pgbus.configuration.roles = original_roles
      end
    end

    context "with --execution-mode flag" do
      it "sets config.execution_mode to :async" do
        original_mode = Pgbus.configuration.execution_mode
        described_class.start(["start", "--execution-mode", "async"])
        expect(Pgbus.configuration.execution_mode).to eq(:async)
      ensure
        Pgbus.configuration.execution_mode = original_mode
      end

      it "sets config.execution_mode to :threads" do
        original_mode = Pgbus.configuration.execution_mode
        described_class.start(["start", "--execution-mode", "threads"])
        expect(Pgbus.configuration.execution_mode).to eq(:threads)
      ensure
        Pgbus.configuration.execution_mode = original_mode
      end
    end

    context "with multiple --*-only flags (mutually exclusive)" do
      it "raises ArgumentError when --workers-only and --scheduler-only are both passed" do
        expect do
          described_class.start(["start", "--workers-only", "--scheduler-only"])
        end.to raise_error(ArgumentError, /mutually exclusive/i)
      end

      it "raises ArgumentError when all three are passed" do
        expect do
          described_class.start(["start", "--workers-only", "--scheduler-only", "--dispatcher-only"])
        end.to raise_error(ArgumentError, /mutually exclusive/i)
      end
    end

    context "when composing role flags with --capsule" do
      it "allows --workers-only --capsule critical" do
        original_workers = Pgbus.configuration.workers
        original_roles = Pgbus.configuration.roles
        Pgbus.configuration.instance_variable_set(:@workers, nil)
        Pgbus.configuration.capsule(:critical, queues: %w[critical], threads: 5)

        described_class.start(["start", "--workers-only", "--capsule", "critical"])

        expect(Pgbus.configuration.roles).to eq([:workers])
        expect(Pgbus.configuration.workers.first[:name]).to eq("critical")
      ensure
        Pgbus.configuration.instance_variable_set(:@workers, original_workers)
        Pgbus.configuration.roles = original_roles
      end
    end
  end

  describe ".show_status" do
    it "prints processes table when processes exist" do
      process = double("ProcessEntry",
                       kind: "supervisor", hostname: "web-1", pid: "100",
                       last_heartbeat_at: "2026-01-01 00:00:00", metadata: "{}")
      scope = double("scope", none?: false, each: nil)
      allow(scope).to receive(:each).and_yield(process)
      allow(Pgbus::ProcessEntry).to receive_message_chain(:order, :select).and_return(scope) # rubocop:disable RSpec/MessageChain

      output = capture_stdout { described_class.show_status }

      expect(output).to include("KIND")
      expect(output).to include("supervisor")
      expect(output).to include("web-1")
    end

    it "prints 'no processes' when result is empty" do
      scope = double("scope", none?: true)
      allow(Pgbus::ProcessEntry).to receive_message_chain(:order, :select).and_return(scope) # rubocop:disable RSpec/MessageChain

      output = capture_stdout { described_class.show_status }

      expect(output).to include("No Pgbus processes running.")
    end
  end

  describe ".list_queues" do
    it "prints formatted table with queue metrics" do
      metric = double("metric",
                      queue_name: "pgbus_test_default",
                      queue_length: 10,
                      queue_visible_length: 8,
                      oldest_msg_age_sec: 42,
                      total_messages: 100)
      allow(mock_client).to receive(:metrics).and_return([metric])

      output = capture_stdout { described_class.list_queues }

      expect(output).to include("QUEUE")
      expect(output).to include("pgbus_test_default")
      expect(output).to include("10")
    end
  end

  private

  def capture_stdout
    original = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = original
  end
end
