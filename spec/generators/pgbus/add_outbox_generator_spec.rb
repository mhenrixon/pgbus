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

  describe "generated migration" do
    it "writes the migration into db/migrate by default" do
      generate_migration(described_class, basename: "_add_pgbus_outbox.rb") do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "routes into db/pgbus_migrate when --database is set" do
      generate_migration(
        described_class,
        options: { database: "pgbus" },
        migrate_dir: "db/pgbus_migrate",
        basename: "_add_pgbus_outbox.rb"
      ) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "defines a versioned migration class" do
      generate_migration(described_class, basename: "_add_pgbus_outbox.rb") do |_path, content|
        expect(content).to match(/class AddPgbusOutbox < ActiveRecord::Migration\[\d+\.\d+\]/)
      end
    end

    it "creates the pgbus_outbox_entries table with a non-null jsonb payload" do
      generate_migration(described_class, basename: "_add_pgbus_outbox.rb") do |_path, content|
        expect(content).to include("create_table :pgbus_outbox_entries")
        expect(content).to include("t.jsonb :payload, null: false")
      end
    end

    it "enforces exactly one of queue_name or routing_key via a check constraint" do
      generate_migration(described_class, basename: "_add_pgbus_outbox.rb") do |_path, content|
        expect(content).to include("add_check_constraint :pgbus_outbox_entries")
        expect(content).to include("(queue_name IS NOT NULL) <> (routing_key IS NOT NULL)")
        expect(content).to include('name: "chk_pgbus_outbox_destination"')
      end
    end

    it "adds partial indexes for unpublished and published rows" do
      generate_migration(described_class, basename: "_add_pgbus_outbox.rb") do |_path, content|
        expect(content).to include('where: "published_at IS NULL"')
        expect(content).to include('name: "idx_pgbus_outbox_unpublished"')
        expect(content).to include('where: "published_at IS NOT NULL"')
        expect(content).to include('name: "idx_pgbus_outbox_cleanup"')
      end
    end
  end
end
