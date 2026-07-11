# frozen_string_literal: true

require "spec_helper"
require "json"
require "pgbus/cli"

RSpec.describe Pgbus::CLI::Dashboard do
  describe ".start" do
    it "prints the main dashboard as import-ready JSON by default" do
      json = JSON.parse(capture_stdout { described_class.start([]) })

      expect(json.keys).to contain_exactly("title", "description", "visuals")
      expect(json["title"]).to eq("Pgbus")
      expect(json["visuals"]).to be_an(Array)
    end

    it "strips the automated-dashboard metric_keys wrapper" do
      json = JSON.parse(capture_stdout { described_class.start([]) })

      expect(json).not_to have_key("metric_keys")
      expect(json).not_to have_key("dashboard")
    end

    it "prints a named extra dashboard" do
      json = JSON.parse(capture_stdout { described_class.start(["health"]) })

      expect(json["title"]).to eq("Pgbus — Health")
    end

    it "lists available dashboards with --list" do
      output = capture_stdout { described_class.start(["--list"]) }

      expect(output).to include("main", "health", "streams", "throughput")
      expect(output).to include("Pgbus — Throughput & Latency")
    end

    it "exits 1 and names the available dashboards on an unknown name" do
      expect do
        expect { described_class.start(["bogus"]) }
          .to output(/Unknown dashboard.*main, health, streams, throughput/m).to_stderr
      end.to raise_error(SystemExit) { |e| expect(e.status).to eq(1) }
    end
  end

  describe "routing from Pgbus::CLI" do
    it "routes 'dashboard' to the dashboard command" do
      json = JSON.parse(capture_stdout { Pgbus::CLI.start(["dashboard"]) })

      expect(json["title"]).to eq("Pgbus")
    end

    it "routes 'dashboard <name>' with arguments" do
      json = JSON.parse(capture_stdout { Pgbus::CLI.start(%w[dashboard streams]) })

      expect(json["title"]).to eq("Pgbus — Streams")
    end
  end

  def capture_stdout
    original = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = original
  end
end
