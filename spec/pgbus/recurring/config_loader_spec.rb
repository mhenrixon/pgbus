# frozen_string_literal: true

require "spec_helper"
require "tempfile"

RSpec.describe Pgbus::Recurring::ConfigLoader do
  describe ".load" do
    it "loads recurring tasks from a YAML file" do
      yaml_content = <<~YAML
        daily_cleanup:
          class: CleanupJob
          schedule: "0 2 * * *"
          queue: maintenance
        hourly_sync:
          class: SyncJob
          schedule: "0 * * * *"
      YAML

      file = Tempfile.new(["recurring", ".yml"])
      file.write(yaml_content)
      file.rewind

      tasks = described_class.load(file.path)

      expect(tasks).to be_a(Hash)
      expect(tasks.size).to eq(2)
      expect(tasks["daily_cleanup"]["class"]).to eq("CleanupJob")
      expect(tasks["hourly_sync"]["schedule"]).to eq("0 * * * *")
    ensure
      file&.close
      file&.unlink
    end

    it "loads environment-scoped config" do
      yaml_content = <<~YAML
        production:
          prod_task:
            class: ProdJob
            schedule: "0 2 * * *"
        development:
          dev_task:
            class: DevJob
            schedule: "* * * * *"
      YAML

      file = Tempfile.new(["recurring", ".yml"])
      file.write(yaml_content)
      file.rewind

      tasks = described_class.load(file.path, env: "production")

      expect(tasks.size).to eq(1)
      expect(tasks.keys).to eq(["prod_task"])
    ensure
      file&.close
      file&.unlink
    end

    it "falls back to full config when environment key not found" do
      yaml_content = <<~YAML
        daily_cleanup:
          class: CleanupJob
          schedule: "0 2 * * *"
      YAML

      file = Tempfile.new(["recurring", ".yml"])
      file.write(yaml_content)
      file.rewind

      tasks = described_class.load(file.path, env: "staging")

      expect(tasks.size).to eq(1)
      expect(tasks.keys).to eq(["daily_cleanup"])
    ensure
      file&.close
      file&.unlink
    end

    it "processes ERB in the YAML file" do
      yaml_content = <<~YAML
        cleanup:
          class: CleanupJob
          schedule: "<%= '0 2 * * *' %>"
      YAML

      file = Tempfile.new(["recurring", ".yml"])
      file.write(yaml_content)
      file.rewind

      tasks = described_class.load(file.path)

      expect(tasks["cleanup"]["schedule"]).to eq("0 2 * * *")
    ensure
      file&.close
      file&.unlink
    end

    it "returns empty hash for empty file" do
      file = Tempfile.new(["recurring", ".yml"])
      file.write("")
      file.rewind

      tasks = described_class.load(file.path)
      expect(tasks).to eq({})
    ensure
      file&.close
      file&.unlink
    end

    it "returns empty hash when file doesn't exist" do
      tasks = described_class.load("/nonexistent/recurring.yml")
      expect(tasks).to eq({})
    end
  end
end
