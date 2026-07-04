# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_job_locks_generator"

RSpec.describe Pgbus::Generators::AddJobLocksGenerator do
  it_behaves_like "a pgbus generator", /job locks/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_job_locks.rb" }

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

    it "defines a versioned migration class creating pgbus_job_locks" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusJobLocks < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_job_locks")
      end
    end

    it "declares the lock ownership columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("t.string :lock_key, null: false")
        expect(content).to include("t.string :job_class, null: false")
        expect(content).to include('t.string :state, null: false, default: "queued"')
        expect(content).to include("t.integer :owner_pid")
        expect(content).to include("t.datetime :expires_at, null: false")
      end
    end

    it "adds the key, expiry, and reaper indexes" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_index :pgbus_job_locks, :lock_key")
        expect(content).to include('name: "idx_pgbus_job_locks_key"')
        expect(content).to include("add_index :pgbus_job_locks, :expires_at")
        expect(content).to include('name: "idx_pgbus_job_locks_expires"')
        expect(content).to include("add_index :pgbus_job_locks, [:state, :owner_pid]")
        expect(content).to include('name: "idx_pgbus_job_locks_reaper"')
      end
    end
  end

  describe "deprecation warning" do
    it "steers new installs to add_uniqueness_keys and existing migrations to migrate_job_locks" do
      generate_migration(described_class, basename: "_add_pgbus_job_locks.rb") do |_path, _content, tmpdir|
        generator = described_class.new([], {})
        generator.destination_root = tmpdir

        expect do
          generator.display_post_install
        end.to output(/DEPRECATED.*pgbus:add_uniqueness_keys.*pgbus:migrate_job_locks/m).to_stdout
      end
    end
  end
end
