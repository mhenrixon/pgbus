# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_presence_generator"

RSpec.describe Pgbus::Generators::AddPresenceGenerator do
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

    it "has a description mentioning presence" do
      expect(described_class.desc).to match(/presence/i)
    end
  end

  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/add_presence.rb.erb", __dir__)
    end
    let(:content) { File.read(template_path) }

    it "exists" do
      expect(File.exist?(template_path)).to be(true)
    end

    it "defines a versioned migration class" do
      expect(content).to include("class AddPgbusPresence < ActiveRecord::Migration<%= migration_version %>")
    end

    it "creates the pgbus_presence_members table without a default id" do
      expect(content).to include("create_table :pgbus_presence_members, id: false")
    end

    it "declares the membership columns" do
      expect(content).to include("t.string :stream_name, null: false")
      expect(content).to include("t.string :member_id, null: false")
      expect(content).to include("t.jsonb :metadata, null: false, default: {}")
      expect(content).to include("t.datetime :last_seen_at, null: false")
    end

    it "adds a unique composite primary-key index on (stream_name, member_id)" do
      expect(content).to include("%i[stream_name member_id]")
      expect(content).to include("unique: true")
      expect(content).to include('name: "idx_pgbus_presence_members_pk"')
    end

    it "adds a sweep index on (stream_name, last_seen_at)" do
      expect(content).to include("%i[stream_name last_seen_at]")
      expect(content).to include('name: "idx_pgbus_presence_members_sweep"')
    end
  end
end
