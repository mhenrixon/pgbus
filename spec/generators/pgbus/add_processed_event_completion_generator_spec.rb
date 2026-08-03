# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_processed_event_completion_generator"

RSpec.describe Pgbus::Generators::AddProcessedEventCompletionGenerator do
  it_behaves_like "a pgbus generator", /completed_at|two-phase|idempoten/i

  describe "generated migration" do
    it "writes the migration into db/migrate by default" do
      generate_migration(described_class, basename: "_add_pgbus_processed_event_completion.rb") do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "routes into db/pgbus_migrate when --database is set" do
      generate_migration(
        described_class,
        options: { database: "pgbus" },
        migrate_dir: "db/pgbus_migrate",
        basename: "_add_pgbus_processed_event_completion.rb"
      ) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "defines a versioned migration class" do
      generate_migration(described_class, basename: "_add_pgbus_processed_event_completion.rb") do |_path, content|
        expect(content).to match(/class AddPgbusProcessedEventCompletion < ActiveRecord::Migration\[\d+\.\d+\]/)
      end
    end

    it "adds the nullable completed_at column" do
      generate_migration(described_class, basename: "_add_pgbus_processed_event_completion.rb") do |_path, content|
        expect(content).to include("add_column :pgbus_processed_events, :completed_at, :datetime")
      end
    end

    it "backfills legacy rows as completed so history is not retroactively re-run" do
      generate_migration(described_class, basename: "_add_pgbus_processed_event_completion.rb") do |_path, content|
        expect(content).to include("SET completed_at = processed_at")
        expect(content).to include("WHERE completed_at IS NULL")
      end
    end

    it "is reversible" do
      generate_migration(described_class, basename: "_add_pgbus_processed_event_completion.rb") do |_path, content|
        expect(content).to include("remove_column :pgbus_processed_events, :completed_at")
      end
    end
  end
end
