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

  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/tune_autovacuum.rb.erb", __dir__)
    end
    let(:content) { File.read(template_path) }

    it "exists" do
      expect(File.exist?(template_path)).to be(true)
    end

    it "defines a versioned migration class" do
      expect(content).to include("class TunePgbusAutovacuum < ActiveRecord::Migration<%= migration_version %>")
    end

    it "applies queue-wide autovacuum settings via AutovacuumTuning" do
      expect(content).to include("execute Pgbus::AutovacuumTuning.sql_for_all_queues")
    end

    it "tunes high-churn pgbus tables too" do
      expect(content).to include("execute Pgbus::AutovacuumTuning.sql_for_high_churn_tables")
    end

    it "resets autovacuum settings on down" do
      expect(content).to include("def down")
      expect(content).to include("RESET (autovacuum_vacuum_scale_factor")
      expect(content).to include("Pgbus::AutovacuumTuning::HIGH_CHURN_TABLES")
    end
  end
end
