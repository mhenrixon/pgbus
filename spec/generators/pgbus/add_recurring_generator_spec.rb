# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_recurring_generator"

RSpec.describe Pgbus::Generators::AddRecurringGenerator do
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

    it "has a description mentioning recurring jobs" do
      expect(described_class.desc).to match(/recurring/i)
    end
  end

  describe "generated migration" do
    let(:basename) { "_add_pgbus_recurring_tables.rb" }

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

    it "creates the pgbus_recurring_tasks table with a unique key index" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusRecurringTables < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_recurring_tasks")
        expect(content).to include("t.string :key, null: false")
        expect(content).to include("t.string :schedule, null: false")
        expect(content).to include("t.boolean :enabled, null: false, default: true")
        expect(content).to include("add_index :pgbus_recurring_tasks, :key")
        expect(content).to include('name: "idx_pgbus_recurring_tasks_key"')
      end
    end

    it "creates the pgbus_recurring_executions table with dedup and cleanup indexes" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("create_table :pgbus_recurring_executions")
        expect(content).to include("t.string :task_key, null: false")
        expect(content).to include("t.datetime :run_at, null: false")
        expect(content).to include("add_index :pgbus_recurring_executions, [:task_key, :run_at]")
        expect(content).to include('name: "idx_pgbus_recurring_executions_dedup"')
        expect(content).to include("add_index :pgbus_recurring_executions, :run_at")
        expect(content).to include('name: "idx_pgbus_recurring_executions_cleanup"')
      end
    end
  end

  describe "recurring config" do
    let(:basename) { "_add_pgbus_recurring_tables.rb" }

    it "also renders config/recurring.yml alongside the migration" do
      generate_migration(described_class, basename: basename) do |_path, _content, root|
        expect(File.exist?(File.join(root, "config", "recurring.yml"))).to be(true)
      end
    end
  end
end
