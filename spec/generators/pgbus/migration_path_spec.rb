# frozen_string_literal: true

require "spec_helper"
require "generators/pgbus/migration_path"

RSpec.describe Pgbus::Generators::MigrationPath do
  # pgbus_migrate_path / separate_database? are private in the mixin.
  subject(:migrate_path) { host.send(:pgbus_migrate_path) }

  let(:host) { host_class.new(options) }

  # A minimal host that mixes in the module the way every generator does.
  # `options` and `db_migrate_path` normally come from Thor/Rails; here they
  # are stubbed so the branching logic can be tested in isolation without a
  # real Rails generator or database.yml.
  let(:host_class) do
    Class.new do
      include Pgbus::Generators::MigrationPath

      attr_reader :options

      def initialize(options)
        @options = options
      end

      # Stands in for ActiveRecord::Generators::Migration#db_migrate_path,
      # which reads migrations_paths from database.yml at runtime.
      def db_migrate_path
        "db/pgbus_migrate"
      end
    end
  end

  context "when no --database option is set" do
    let(:options) { {} }

    it "returns the single-database default path" do
      expect(migrate_path).to eq("db/migrate")
    end

    it "reports separate_database? as false" do
      expect(host.send(:separate_database?)).to be(false)
    end

    it "does not delegate to db_migrate_path" do
      allow(host).to receive(:db_migrate_path)
      migrate_path
      expect(host).not_to have_received(:db_migrate_path)
    end
  end

  context "when --database is nil" do
    let(:options) { { database: nil } }

    it "treats a nil database as single-database mode" do
      expect(migrate_path).to eq("db/migrate")
    end

    it "reports separate_database? as false" do
      expect(host.send(:separate_database?)).to be(false)
    end
  end

  context "when --database is a blank string" do
    let(:options) { { database: "" } }

    it "treats a blank database as single-database mode" do
      expect(migrate_path).to eq("db/migrate")
    end
  end

  context "when --database=pgbus is set" do
    let(:options) { { database: "pgbus" } }

    it "delegates to Rails' db_migrate_path" do
      expect(migrate_path).to eq("db/pgbus_migrate")
    end

    it "reports separate_database? as true" do
      expect(host.send(:separate_database?)).to be(true)
    end

    it "calls db_migrate_path exactly once" do
      allow(host).to receive(:db_migrate_path).and_return("db/pgbus_migrate")
      migrate_path
      expect(host).to have_received(:db_migrate_path).once
    end
  end
end
