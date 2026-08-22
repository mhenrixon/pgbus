# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_batch_callback_jobs_generator"

RSpec.describe Pgbus::Generators::AddBatchCallbackJobsGenerator do
  it_behaves_like "a pgbus generator", /callback/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_batch_callback_jobs.rb" }

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

    it "adds the three jsonb callback columns idempotently" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusBatchCallbackJobs < ActiveRecord::Migration\[\d+\.\d+\]/)
        %w[on_finish_job on_success_job on_failure_job].each do |column|
          expect(content).to include("add_column :pgbus_batches, :#{column}, :jsonb unless column_exists?")
          expect(content).to include("remove_column :pgbus_batches, :#{column} if column_exists?")
        end
      end
    end
  end
end
