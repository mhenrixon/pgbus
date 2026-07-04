# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_uniqueness_keys_generator"

RSpec.describe Pgbus::Generators::AddUniquenessKeysGenerator do
  it_behaves_like "a pgbus generator", /uniqueness/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_uniqueness_keys.rb" }

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

    it "defines a versioned migration class creating pgbus_uniqueness_keys" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusUniquenessKeys < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_uniqueness_keys")
      end
    end

    it "declares the lock key, queue, and message columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("t.string :lock_key, null: false")
        expect(content).to include("t.string :queue_name, null: false")
        expect(content).to include("t.bigint :msg_id, null: false")
      end
    end

    it "adds a unique index on lock_key" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_index :pgbus_uniqueness_keys, :lock_key")
        expect(content).to include("unique: true")
        expect(content).to include('name: "idx_pgbus_uniqueness_keys_key"')
      end
    end
  end
end
