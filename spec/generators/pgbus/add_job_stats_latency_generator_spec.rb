# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_job_stats_latency_generator"

RSpec.describe Pgbus::Generators::AddJobStatsLatencyGenerator do
  it_behaves_like "a pgbus generator", /latency/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_job_stats_latency.rb" }

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
        expect(content).to match(/class AddPgbusJobStatsLatency < ActiveRecord::Migration\[\d+\.\d+\]/)
      end
    end

    it "adds the enqueue_latency_ms and retry_count columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_column :pgbus_job_stats, :enqueue_latency_ms, :bigint")
        expect(content).to include("add_column :pgbus_job_stats, :retry_count, :integer, default: 0")
      end
    end

    it "adds the queue latency index idempotently" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_index :pgbus_job_stats, [:queue_name, :created_at]")
        expect(content).to include('name: "idx_pgbus_job_stats_queue_time"')
        expect(content).to include("if_not_exists: true")
      end
    end
  end
end
