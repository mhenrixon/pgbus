# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_job_stats_latency_generator"

RSpec.describe Pgbus::Generators::AddJobStatsLatencyGenerator do
  describe "generator class wiring" do
    it "is a Rails::Generators::Base subclass" do
      expect(described_class.ancestors).to include(Rails::Generators::Base)
    end

    it "mixes in the shared MigrationPath path logic" do
      expect(described_class.ancestors).to include(Pgbus::Generators::MigrationPath)
    end

    it "exposes a --database option defaulting to nil" do
      option = described_class.class_options[:database]
      expect(option).not_to be_nil
      expect(option.default).to be_nil
    end

    it "has a description mentioning latency" do
      expect(described_class.desc).to match(/latency/i)
    end
  end

  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/add_job_stats_latency.rb.erb", __dir__)
    end
    let(:content) { File.read(template_path) }

    it "exists" do
      expect(File.exist?(template_path)).to be(true)
    end

    it "defines a versioned migration class" do
      expect(content).to include(
        "class AddPgbusJobStatsLatency < ActiveRecord::Migration<%= migration_version %>"
      )
    end

    it "adds the enqueue_latency_ms column as bigint" do
      expect(content).to include("add_column :pgbus_job_stats, :enqueue_latency_ms, :bigint")
    end

    it "adds the retry_count column defaulting to 0" do
      expect(content).to include("add_column :pgbus_job_stats, :retry_count, :integer, default: 0")
    end

    it "adds the queue latency index idempotently" do
      expect(content).to include("add_index :pgbus_job_stats, [:queue_name, :created_at]")
      expect(content).to include('name: "idx_pgbus_job_stats_queue_time"')
      expect(content).to include("if_not_exists: true")
    end
  end
end
