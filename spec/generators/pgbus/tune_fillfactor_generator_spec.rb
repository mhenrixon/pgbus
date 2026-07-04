# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/tune_fillfactor_generator"

RSpec.describe Pgbus::Generators::TuneFillfactorGenerator do
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

    it "has a description mentioning fillfactor" do
      expect(described_class.desc).to match(/fillfactor/i)
    end
  end

  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/tune_fillfactor.rb.erb", __dir__)
    end
    let(:content) { File.read(template_path) }

    it "exists" do
      expect(File.exist?(template_path)).to be(true)
    end

    it "defines a versioned migration class" do
      expect(content).to include("class TunePgbusFillfactor < ActiveRecord::Migration<%= migration_version %>")
    end

    it "sets fillfactor across all queue tables via TableMaintenance" do
      expect(content).to include("execute Pgbus::TableMaintenance.fillfactor_sql_for_all_queues")
    end

    it "resets fillfactor on down" do
      expect(content).to include("def down")
      expect(content).to include("RESET (fillfactor)")
    end
  end
end
