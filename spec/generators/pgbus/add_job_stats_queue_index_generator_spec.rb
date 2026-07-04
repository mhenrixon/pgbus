# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_job_stats_queue_index_generator"

RSpec.describe Pgbus::Generators::AddJobStatsQueueIndexGenerator do
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

    it "has a description mentioning the composite index" do
      expect(described_class.desc).to include("pgbus_job_stats")
    end
  end

  describe "generated migration" do
    let(:basename) { "_add_pgbus_job_stats_queue_index.rb" }

    it "writes the migration into db/migrate by default" do
      generate_migration(described_class, basename: basename) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "routes into db/pgbus_migrate when --database is set" do
      generate_migration(
        described_class,
        options: { database: "pgbus" },
        migrate_dir: "db/pgbus_migrate",
        basename: basename
      ) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "defines a versioned migration class" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusJobStatsQueueIndex < ActiveRecord::Migration\[\d+\.\d+\]/)
      end
    end

    it "adds an idempotent composite index on (queue_name, created_at)" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_index :pgbus_job_stats, %i[queue_name created_at]")
        expect(content).to include('name: "idx_pgbus_job_stats_queue_time"')
        expect(content).to include("if_not_exists: true")
      end
    end
  end
end
