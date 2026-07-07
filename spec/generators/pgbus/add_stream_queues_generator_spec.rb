# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_stream_queues_generator"

RSpec.describe Pgbus::Generators::AddStreamQueuesGenerator do
  it_behaves_like "a pgbus generator", /stream queue registry/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_stream_queues.rb" }

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

    # Issue #344: a bare invocation (no --database) in an app configured for a
    # separate pgbus database must route to db/pgbus_migrate, not silently to
    # db/migrate (the primary DB). The detector reads connects_to.
    it "routes into db/pgbus_migrate when connects_to is set even without --database" do
      detector = instance_double(Pgbus::Generators::DatabaseTargetDetector, detect: "pgbus")
      allow(Pgbus::Generators::DatabaseTargetDetector).to receive(:new).and_return(detector)

      Dir.mktmpdir do |tmpdir|
        FileUtils.mkdir_p(File.join(tmpdir, "db/pgbus_migrate"))
        generator = described_class.new([], {})
        generator.destination_root = tmpdir
        # No --database, so Rails' db_migrate_path can't resolve; the module
        # resolves the detected database's path itself. Stub that resolution.
        allow(generator).to receive(:resolve_detected_migrate_path).and_return("db/pgbus_migrate")

        silence_generator { generator.invoke_all }

        path = Dir[File.join(tmpdir, "db/pgbus_migrate", "*#{basename}")].first
        expect(path).not_to be_nil
        expect(Dir[File.join(tmpdir, "db/migrate", "*#{basename}")]).to be_empty
      end
    end

    it "creates the pgbus_stream_queues table" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusStreamQueues < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_stream_queues")
      end
    end

    it "declares the queue_name and created_at columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("t.string :queue_name, null: false")
        expect(content).to include("t.datetime :created_at, null: false")
      end
    end

    it "adds a unique index on queue_name" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_index :pgbus_stream_queues, :queue_name")
        expect(content).to include("unique: true")
        expect(content).to include('name: "idx_pgbus_stream_queues_queue_name"')
      end
    end
  end
end
