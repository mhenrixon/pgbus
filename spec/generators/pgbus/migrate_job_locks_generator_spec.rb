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

  describe "generated migration" do
    let(:basename) { "_migrate_pgbus_job_locks_to_uniqueness_keys.rb" }

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

    it "creates the lightweight pgbus_uniqueness_keys table only if absent" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class MigratePgbusJobLocksToUniquenessKeys < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("unless table_exists?(:pgbus_uniqueness_keys)")
        expect(content).to include("create_table :pgbus_uniqueness_keys, id: false")
        expect(content).to include("t.string :lock_key, null: false")
        expect(content).to include("t.bigint :msg_id, null: false")
        expect(content).to include("add_index :pgbus_uniqueness_keys, :lock_key")
        expect(content).to include('name: "idx_pgbus_uniqueness_keys_key"')
      end
    end

    it "refuses to migrate while active locks remain, then drops the old table" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("if table_exists?(:pgbus_job_locks)")
        expect(content).to include("SELECT COUNT(*) FROM pgbus_job_locks")
        expect(content).to include("raise")
        expect(content).to include("drop_table :pgbus_job_locks")
      end
    end

    it "is irreversible on down" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("raise ActiveRecord::IrreversibleMigration")
      end
    end
  end
end
