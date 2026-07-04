# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/migrate_job_locks_generator"

RSpec.describe Pgbus::Generators::MigrateJobLocksGenerator do
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

    it "has a description mentioning the uniqueness keys migration" do
      expect(described_class.desc).to match(/uniqueness/i)
    end
  end

  describe "migration template" do
    let(:template_path) do
      File.expand_path(
        "../../../lib/generators/pgbus/templates/migrate_job_locks_to_uniqueness_keys.rb.erb", __dir__
      )
    end
    let(:content) { File.read(template_path) }

    it "exists" do
      expect(File.exist?(template_path)).to be(true)
    end

    it "defines a versioned migration class" do
      expect(content).to include(
        "class MigratePgbusJobLocksToUniquenessKeys < ActiveRecord::Migration<%= migration_version %>"
      )
    end

    it "creates the new lightweight pgbus_uniqueness_keys table only if absent" do
      expect(content).to include("unless table_exists?(:pgbus_uniqueness_keys)")
      expect(content).to include("create_table :pgbus_uniqueness_keys, id: false")
      expect(content).to include("t.string :lock_key, null: false")
      expect(content).to include("t.bigint :msg_id, null: false")
    end

    it "adds a unique index on the uniqueness key" do
      expect(content).to include("add_index :pgbus_uniqueness_keys, :lock_key")
      expect(content).to include("unique: true")
      expect(content).to include('name: "idx_pgbus_uniqueness_keys_key"')
    end

    it "refuses to migrate while active locks remain in pgbus_job_locks" do
      expect(content).to include("if table_exists?(:pgbus_job_locks)")
      expect(content).to include("SELECT COUNT(*) FROM pgbus_job_locks")
      expect(content).to include("raise")
    end

    it "drops the old pgbus_job_locks table once drained" do
      expect(content).to include("drop_table :pgbus_job_locks")
    end

    it "is irreversible on down" do
      expect(content).to include("raise ActiveRecord::IrreversibleMigration")
    end
  end
end
