# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_outbox_generator"

RSpec.describe Pgbus::Generators::AddOutboxGenerator do
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

    it "has a description mentioning the outbox" do
      expect(described_class.desc).to match(/outbox/i)
    end
  end

  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/add_outbox.rb.erb", __dir__)
    end
    let(:content) { File.read(template_path) }

    it "exists" do
      expect(File.exist?(template_path)).to be(true)
    end

    it "defines a versioned migration class" do
      expect(content).to include("class AddPgbusOutbox < ActiveRecord::Migration<%= migration_version %>")
    end

    it "creates the pgbus_outbox_entries table" do
      expect(content).to include("create_table :pgbus_outbox_entries")
    end

    it "stores the jsonb payload as non-null" do
      expect(content).to include("t.jsonb :payload, null: false")
    end

    it "enforces exactly one of queue_name or routing_key via a check constraint" do
      expect(content).to include("add_check_constraint :pgbus_outbox_entries")
      expect(content).to include("(queue_name IS NOT NULL) <> (routing_key IS NOT NULL)")
      expect(content).to include('name: "chk_pgbus_outbox_destination"')
    end

    it "indexes unpublished rows partially" do
      expect(content).to include('where: "published_at IS NULL"')
      expect(content).to include('name: "idx_pgbus_outbox_unpublished"')
    end

    it "indexes published rows for cleanup" do
      expect(content).to include('where: "published_at IS NOT NULL"')
      expect(content).to include('name: "idx_pgbus_outbox_cleanup"')
    end
  end
end
