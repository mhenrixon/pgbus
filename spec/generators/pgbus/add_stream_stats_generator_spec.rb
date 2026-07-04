# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_stream_stats_generator"

RSpec.describe Pgbus::Generators::AddStreamStatsGenerator do
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

    it "has a description mentioning stream stats" do
      expect(described_class.desc).to match(/stream stats/i)
    end
  end

  describe "generated migration" do
    let(:basename) { "_add_pgbus_stream_stats.rb" }

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

    it "creates the pgbus_stream_stats table" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusStreamStats < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_stream_stats")
      end
    end

    it "declares the stream stat columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("t.string :stream_name, null: false")
        expect(content).to include("t.string :event_type, null: false")
        expect(content).to include("t.integer :duration_ms, null: false, default: 0")
        expect(content).to include("t.integer :fanout")
      end
    end

    it "adds the time, stream, and event-type indexes" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_index :pgbus_stream_stats, :created_at")
        expect(content).to include('name: "idx_pgbus_stream_stats_time"')
        expect(content).to include("add_index :pgbus_stream_stats, %i[stream_name created_at]")
        expect(content).to include('name: "idx_pgbus_stream_stats_stream_time"')
        expect(content).to include("add_index :pgbus_stream_stats, %i[event_type created_at]")
        expect(content).to include('name: "idx_pgbus_stream_stats_type_time"')
      end
    end
  end
end
