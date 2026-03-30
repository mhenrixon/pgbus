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

  describe ".show_status" do
    context "when ActiveRecord is defined" do
      let(:connection_double) { double("connection") }

      before do
        stub_const("ActiveRecord::Base", Class.new)
        allow(ActiveRecord::Base).to receive(:connection).and_return(connection_double)
      end

      it "prints processes table when processes exist" do
        rows = [
          { "kind" => "supervisor", "hostname" => "web-1", "pid" => "100",
            "last_heartbeat_at" => "2026-01-01 00:00:00", "metadata" => "{}" }
        ]
        allow(connection_double).to receive(:execute).and_return(rows)

        output = capture_stdout { described_class.show_status }

        expect(output).to include("KIND")
        expect(output).to include("supervisor")
        expect(output).to include("web-1")
      end

      it "prints 'no processes' when result is empty" do
        allow(connection_double).to receive(:execute).and_return([])

        output = capture_stdout { described_class.show_status }

        expect(output).to include("No Pgbus processes running.")
      end
    end

    context "when ActiveRecord is not defined" do
      it "prints 'not available' message" do
        hide_const("ActiveRecord::Base")

        output = capture_stdout { described_class.show_status }

        expect(output).to include("ActiveRecord not available")
      end
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
