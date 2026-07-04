# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_queue_states_generator"

RSpec.describe Pgbus::Generators::AddQueueStatesGenerator do
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

    it "has a description mentioning pause/resume or circuit breaker" do
      expect(described_class.desc).to match(/pause|circuit breaker/i)
    end
  end

  describe "generated migration" do
    let(:basename) { "_add_pgbus_queue_states.rb" }

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

    it "creates the pgbus_queue_states table" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusQueueStates < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_queue_states")
      end
    end

    it "declares the pause and circuit breaker columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("t.string :queue_name, null: false")
        expect(content).to include("t.boolean :paused, null: false, default: false")
        expect(content).to include("t.integer :circuit_breaker_trip_count, default: 0")
        expect(content).to include("t.datetime :circuit_breaker_resume_at")
      end
    end

    it "adds a unique index on queue_name" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_index :pgbus_queue_states, :queue_name")
        expect(content).to include("unique: true")
        expect(content).to include('name: "idx_pgbus_queue_states_queue_name"')
      end
    end
  end
end
