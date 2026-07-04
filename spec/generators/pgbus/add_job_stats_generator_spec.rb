# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_job_stats_generator"

RSpec.describe Pgbus::Generators::AddJobStatsGenerator do
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

    it "has a description mentioning job stats" do
      expect(described_class.desc).to match(/job stats/i)
    end
  end

  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/add_job_stats.rb.erb", __dir__)
    end
    let(:content) { File.read(template_path) }

    it "exists" do
      expect(File.exist?(template_path)).to be(true)
    end

    it "defines a versioned migration class" do
      expect(content).to include("class AddPgbusJobStats < ActiveRecord::Migration<%= migration_version %>")
    end

    it "creates the pgbus_job_stats table" do
      expect(content).to include("create_table :pgbus_job_stats")
    end

    it "declares the stats columns" do
      expect(content).to include("t.string :job_class, null: false")
      expect(content).to include("t.string :queue_name, null: false")
      expect(content).to include("t.string :status, null: false")
      expect(content).to include("t.integer :duration_ms, null: false, default: 0")
    end

    it "indexes created_at for time-window queries" do
      expect(content).to include("add_index :pgbus_job_stats, :created_at")
      expect(content).to include('name: "idx_pgbus_job_stats_time"')
    end

    it "indexes (job_class, created_at)" do
      expect(content).to include("add_index :pgbus_job_stats, [:job_class, :created_at]")
      expect(content).to include('name: "idx_pgbus_job_stats_class_time"')
    end

    it "indexes (status, created_at)" do
      expect(content).to include("add_index :pgbus_job_stats, [:status, :created_at]")
      expect(content).to include('name: "idx_pgbus_job_stats_status_time"')
    end
  end
end
