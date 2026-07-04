# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_job_stats_generator"

RSpec.describe Pgbus::Generators::AddJobStatsGenerator do
  it_behaves_like "a pgbus generator", /job stats/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_job_stats.rb" }

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

    it "defines a versioned migration class creating pgbus_job_stats" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusJobStats < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_job_stats")
      end
    end

    it "declares the stats columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("t.string :job_class, null: false")
        expect(content).to include("t.string :queue_name, null: false")
        expect(content).to include("t.string :status, null: false")
        expect(content).to include("t.integer :duration_ms, null: false, default: 0")
      end
    end

    it "indexes created_at, (job_class, created_at), and (status, created_at)" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_index :pgbus_job_stats, :created_at")
        expect(content).to include('name: "idx_pgbus_job_stats_time"')
        expect(content).to include("add_index :pgbus_job_stats, [:job_class, :created_at]")
        expect(content).to include('name: "idx_pgbus_job_stats_class_time"')
        expect(content).to include("add_index :pgbus_job_stats, [:status, :created_at]")
        expect(content).to include('name: "idx_pgbus_job_stats_status_time"')
      end
    end
  end
end
