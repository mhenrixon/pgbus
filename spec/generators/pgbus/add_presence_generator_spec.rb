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

  describe "generated migration" do
    let(:basename) { "_add_pgbus_presence.rb" }

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

    it "creates the pgbus_presence_members table without a default id" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusPresence < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_presence_members, id: false")
      end
    end

    it "declares the membership columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("t.string :stream_name, null: false")
        expect(content).to include("t.string :member_id, null: false")
        expect(content).to include("t.jsonb :metadata, null: false, default: {}")
        expect(content).to include("t.datetime :last_seen_at, null: false")
      end
    end

    it "adds the unique composite primary-key index and the sweep index" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("%i[stream_name member_id]")
        expect(content).to include("unique: true")
        expect(content).to include('name: "idx_pgbus_presence_members_pk"')
        expect(content).to include("%i[stream_name last_seen_at]")
        expect(content).to include('name: "idx_pgbus_presence_members_sweep"')
      end
    end
  end
end
