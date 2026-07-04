# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/tune_fillfactor_generator"

RSpec.describe Pgbus::Generators::TuneFillfactorGenerator do
  it_behaves_like "a pgbus generator", /fillfactor/i

  describe "generated migration" do
    let(:basename) { "_tune_pgbus_fillfactor.rb" }

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

    it "sets fillfactor across all queue tables via TableMaintenance" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class TunePgbusFillfactor < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("execute Pgbus::TableMaintenance.fillfactor_sql_for_all_queues")
      end
    end

    it "resets fillfactor on down" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("def down")
        expect(content).to include("RESET (fillfactor)")
      end
    end
  end
end
