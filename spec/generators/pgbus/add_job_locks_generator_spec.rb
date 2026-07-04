# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_job_locks_generator"

RSpec.describe Pgbus::Generators::AddJobLocksGenerator do
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

    it "has a description mentioning job locks" do
      expect(described_class.desc).to match(/job locks/i)
    end
  end

  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/add_job_locks.rb.erb", __dir__)
    end
    let(:content) { File.read(template_path) }

    it "exists" do
      expect(File.exist?(template_path)).to be(true)
    end

    it "defines a versioned migration class" do
      expect(content).to include("class AddPgbusJobLocks < ActiveRecord::Migration<%= migration_version %>")
    end

    it "creates the pgbus_job_locks table" do
      expect(content).to include("create_table :pgbus_job_locks")
    end

    it "declares the lock ownership columns" do
      expect(content).to include("t.string :lock_key, null: false")
      expect(content).to include("t.string :job_class, null: false")
      expect(content).to include('t.string :state, null: false, default: "queued"')
      expect(content).to include("t.integer :owner_pid")
      expect(content).to include("t.datetime :expires_at, null: false")
    end

    it "adds a unique index on lock_key" do
      expect(content).to include("add_index :pgbus_job_locks, :lock_key")
      expect(content).to include('name: "idx_pgbus_job_locks_key"')
      expect(content).to include("unique: true")
    end

    it "adds a reaper index on (state, owner_pid)" do
      expect(content).to include("add_index :pgbus_job_locks, [:state, :owner_pid]")
      expect(content).to include('name: "idx_pgbus_job_locks_reaper"')
    end

    it "adds an expiry index" do
      expect(content).to include("add_index :pgbus_job_locks, :expires_at")
      expect(content).to include('name: "idx_pgbus_job_locks_expires"')
    end
  end
end
