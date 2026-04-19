# frozen_string_literal: true

require "spec_helper"
require "tempfile"
require "fileutils"

RSpec.describe Pgbus::Recurring::ConfigLoader, ".load_all" do
  let(:tmpdir) { Dir.mktmpdir("pgbus-recurring") }

  after { FileUtils.rm_rf(tmpdir) }

  def write_yaml(name, content)
    path = File.join(tmpdir, name)
    File.write(path, content)
    path
  end

  describe "multi-file merging" do
    it "merges tasks from multiple files" do
      shared = write_yaml("shared.yml", <<~YAML)
        cleanup:
          class: CleanupJob
          schedule: "0 2 * * *"
      YAML

      overlay = write_yaml("bills.yml", <<~YAML)
        exchange_rates:
          class: Bills::UpdateExchangeRatesJob
          schedule: "0 2 * * *"
      YAML

      tasks = described_class.load_all([shared, overlay])

      expect(tasks.keys).to contain_exactly("cleanup", "exchange_rates")
      expect(tasks["cleanup"]["class"]).to eq("CleanupJob")
      expect(tasks["exchange_rates"]["class"]).to eq("Bills::UpdateExchangeRatesJob")
    end

    it "resolves task-name collisions with last-file-wins" do
      base = write_yaml("base.yml", <<~YAML)
        sync:
          class: OldSyncJob
          schedule: "0 * * * *"
      YAML

      override = write_yaml("override.yml", <<~YAML)
        sync:
          class: NewSyncJob
          schedule: "*/5 * * * *"
      YAML

      tasks = described_class.load_all([base, override])

      expect(tasks["sync"]["class"]).to eq("NewSyncJob")
      expect(tasks["sync"]["schedule"]).to eq("*/5 * * * *")
    end

    it "logs debug message when a task is overridden" do
      base = write_yaml("base.yml", <<~YAML)
        sync:
          class: OldSyncJob
          schedule: "0 * * * *"
      YAML

      override = write_yaml("override.yml", <<~YAML)
        sync:
          class: NewSyncJob
          schedule: "*/5 * * * *"
      YAML

      debug_messages = []
      allow(Pgbus.logger).to receive(:debug) { |&blk| debug_messages << blk.call }

      described_class.load_all([base, override])

      expect(debug_messages).to include(a_string_matching(/Recurring task 'sync' overridden by .*override\.yml/))
    end
  end

  describe "missing file handling" do
    it "skips missing files with a warning" do
      existing = write_yaml("existing.yml", <<~YAML)
        cleanup:
          class: CleanupJob
          schedule: "0 2 * * *"
      YAML

      missing = File.join(tmpdir, "nonexistent.yml")

      allow(Pgbus.logger).to receive(:warn)

      tasks = described_class.load_all([existing, missing])

      expect(tasks.keys).to eq(["cleanup"])
      expect(Pgbus.logger).to have_received(:warn)
    end

    it "returns empty hash when all files are missing" do
      tasks = described_class.load_all(["/nonexistent/a.yml", "/nonexistent/b.yml"])

      expect(tasks).to eq({})
    end
  end

  describe "parse error resilience" do
    it "logs error and continues loading remaining files when one file has bad YAML" do
      good = write_yaml("good.yml", <<~YAML)
        cleanup:
          class: CleanupJob
          schedule: "0 2 * * *"
      YAML

      bad = write_yaml("bad.yml", "{{invalid yaml: [")

      allow(Pgbus.logger).to receive(:error)

      tasks = described_class.load_all([bad, good])

      expect(tasks.keys).to eq(["cleanup"])
      expect(Pgbus.logger).to have_received(:error)
    end

    it "logs error and skips when env subtree is not a Hash" do
      good = write_yaml("good.yml", <<~YAML)
        cleanup:
          class: CleanupJob
          schedule: "0 2 * * *"
      YAML

      bad_structure = write_yaml("bad_structure.yml", <<~YAML)
        production: []
      YAML

      error_messages = []
      allow(Pgbus.logger).to receive(:error) { |&blk| error_messages << blk.call }

      tasks = described_class.load_all([bad_structure, good], env: "production")

      expect(tasks.keys).to eq(["cleanup"])
      expect(error_messages).to include(a_string_matching(/Invalid recurring config/))
    end

    it "logs error and continues when ERB raises" do
      good = write_yaml("good.yml", <<~YAML)
        cleanup:
          class: CleanupJob
          schedule: "0 2 * * *"
      YAML

      bad_erb = write_yaml("bad_erb.yml", <<~YAML)
        task:
          class: Foo
          schedule: "<%= raise 'boom' %>"
      YAML

      allow(Pgbus.logger).to receive(:error)

      tasks = described_class.load_all([bad_erb, good])

      expect(tasks.keys).to eq(["cleanup"])
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "empty input" do
    it "returns empty hash for empty file list" do
      expect(described_class.load_all([])).to eq({})
    end

    it "returns empty hash for nil" do
      expect(described_class.load_all(nil)).to eq({})
    end
  end

  describe "environment scoping" do
    it "respects env parameter across files" do
      shared = write_yaml("shared.yml", <<~YAML)
        production:
          prod_task:
            class: ProdJob
            schedule: "0 2 * * *"
        development:
          dev_task:
            class: DevJob
            schedule: "* * * * *"
      YAML

      overlay = write_yaml("overlay.yml", <<~YAML)
        production:
          bills_task:
            class: BillsJob
            schedule: "0 3 * * *"
      YAML

      tasks = described_class.load_all([shared, overlay], env: "production")

      expect(tasks.keys).to contain_exactly("prod_task", "bills_task")
    end
  end
end
