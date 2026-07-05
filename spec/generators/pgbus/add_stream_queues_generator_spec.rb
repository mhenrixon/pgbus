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
