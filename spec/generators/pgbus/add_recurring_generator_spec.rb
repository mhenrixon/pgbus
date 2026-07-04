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

  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/add_recurring_tables.rb.erb", __dir__)
    end
    let(:content) { File.read(template_path) }

    it "exists" do
      expect(File.exist?(template_path)).to be(true)
    end

    it "defines a versioned migration class" do
      expect(content).to include(
        "class AddPgbusRecurringTables < ActiveRecord::Migration<%= migration_version %>"
      )
    end

    it "creates the pgbus_recurring_tasks table" do
      expect(content).to include("create_table :pgbus_recurring_tasks")
      expect(content).to include("t.string :key, null: false")
      expect(content).to include("t.string :schedule, null: false")
      expect(content).to include("t.boolean :enabled, null: false, default: true")
    end

    it "adds a unique index on the task key" do
      expect(content).to include("add_index :pgbus_recurring_tasks, :key")
      expect(content).to include("unique: true")
      expect(content).to include('name: "idx_pgbus_recurring_tasks_key"')
    end

    it "creates the pgbus_recurring_executions table" do
      expect(content).to include("create_table :pgbus_recurring_executions")
      expect(content).to include("t.string :task_key, null: false")
      expect(content).to include("t.datetime :run_at, null: false")
    end

    it "adds a unique dedup index on (task_key, run_at)" do
      expect(content).to include("add_index :pgbus_recurring_executions, [:task_key, :run_at]")
      expect(content).to include("unique: true")
      expect(content).to include('name: "idx_pgbus_recurring_executions_dedup"')
    end

    it "adds a cleanup index on run_at" do
      expect(content).to include("add_index :pgbus_recurring_executions, :run_at")
      expect(content).to include('name: "idx_pgbus_recurring_executions_cleanup"')
    end
  end

  describe "recurring config template" do
    let(:config_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/recurring.yml.erb", __dir__)
    end

    it "renders a config/recurring.yml template alongside the migration" do
      expect(File.exist?(config_path)).to be(true)
    end
  end
end
