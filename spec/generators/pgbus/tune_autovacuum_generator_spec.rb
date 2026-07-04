# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/tune_autovacuum_generator"

RSpec.describe Pgbus::Generators::TuneAutovacuumGenerator do
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

    it "has a description mentioning autovacuum tuning" do
      expect(described_class.desc).to match(/autovacuum/i)
    end
  end

  describe "generated migration" do
    let(:basename) { "_tune_pgbus_autovacuum.rb" }

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

    it "applies queue and high-churn autovacuum settings via AutovacuumTuning" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class TunePgbusAutovacuum < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("execute Pgbus::AutovacuumTuning.sql_for_all_queues")
        expect(content).to include("execute Pgbus::AutovacuumTuning.sql_for_high_churn_tables")
      end
    end

    it "resets autovacuum settings on down" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("def down")
        expect(content).to include("RESET (autovacuum_vacuum_scale_factor")
        expect(content).to include("Pgbus::AutovacuumTuning::HIGH_CHURN_TABLES")
      end
    end
  end
end
