# frozen_string_literal: true

require "spec_helper"
require "generators/pgbus/migration_path"

RSpec.describe Pgbus::Generators::MigrationPath do
  # pgbus_migrate_path / separate_database? are private in the mixin.
  subject(:migrate_path) { host.send(:pgbus_migrate_path) }

  let(:host) { host_class.new(options) }

  # A minimal host that mixes in the module the way every generator does.
  # `options`, `db_migrate_path`, and `destination_root` normally come from
  # Thor/Rails; here they are stubbed so the branching logic can be tested in
  # isolation without a real Rails generator or database.yml.
  let(:host_class) do
    Class.new do
      include Pgbus::Generators::MigrationPath

      attr_reader :options
      attr_accessor :destination_root

      # Records what the mixin would print via the generator's own output.
      attr_reader :said

      def initialize(options)
        @options = options
        @destination_root = "/nonexistent"
        @said = []
      end

      # Stands in for ActiveRecord::Generators::Migration#db_migrate_path,
      # which reads migrations_paths from database.yml at runtime.
      def db_migrate_path
        "db/pgbus_migrate"
      end

      # Stands in for Thor::Shell#say, which real generators provide.
      def say(message, *_args)
        @said << message
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

  # Issue #344: a bare invocation (no --database) in an app configured for a
  # separate pgbus database must NOT land in db/migrate against the wrong DB.
  # The detector supplies the database name so pgbus_migrate_path routes to the
  # separate-database path and the post-install output names the right db:migrate task.
  context "when --database is absent but connects_to is configured (issue #344)" do
    let(:options) { {} }

    before do
      detector = instance_double(Pgbus::Generators::DatabaseTargetDetector, detect: "pgbus")
      allow(Pgbus::Generators::DatabaseTargetDetector)
        .to receive(:new).and_return(detector)
    end

    it "reports separate_database? as true from the detected database" do
      expect(host.send(:separate_database?)).to be(true)
    end

    it "exposes the detected database via effective_database_name" do
      expect(host.send(:effective_database_name)).to eq("pgbus")
    end

    it "routes to the separate-database migrate path" do
      # With no --database, Rails' db_migrate_path can't resolve the path
      # (it reads options[:database]); the module resolves it for the
      # detected database instead, falling back to the db/pgbus_migrate
      # convention when the DB isn't in configurations.
      allow(host).to receive(:resolve_detected_migrate_path).and_return("db/pgbus_migrate")
      expect(migrate_path).to eq("db/pgbus_migrate")
    end

    it "detects only once, memoizing the result" do
      host.send(:effective_database_name)
      host.send(:effective_database_name)
      expect(Pgbus::Generators::DatabaseTargetDetector).to have_received(:new).once
    end
  end

  context "when --database is given AND connects_to is configured" do
    let(:options) { { database: "explicit_db" } }

    it "the explicit --database wins over detection" do
      allow(Pgbus::Generators::DatabaseTargetDetector).to receive(:new)
      expect(host.send(:effective_database_name)).to eq("explicit_db")
      expect(Pgbus::Generators::DatabaseTargetDetector).not_to have_received(:new)
    end
  end

  # The post-install "Next steps" line appends the database name to db:migrate
  # only for a separate database; issue #344 requires it to reflect the
  # detected database too, not just an explicit --database.
  describe "#migrate_command_suffix" do
    context "with no separate database" do
      let(:options) { {} }

      it "is empty so the output reads plain `rails db:migrate`" do
        expect(host.send(:migrate_command_suffix)).to eq("")
      end
    end

    context "with an explicit --database" do
      let(:options) { { database: "pgbus" } }

      it "names the db:migrate task" do
        expect(host.send(:migrate_command_suffix)).to eq(":pgbus")
      end
    end

    context "with a detected database (issue #344)" do
      let(:options) { {} }

      before do
        detector = instance_double(Pgbus::Generators::DatabaseTargetDetector, detect: "pgbus")
        allow(Pgbus::Generators::DatabaseTargetDetector).to receive(:new).and_return(detector)
      end

      it "names the db:migrate task from the detected database" do
        expect(host.send(:migrate_command_suffix)).to eq(":pgbus")
      end
    end
  end

  # A missing migrations_paths for the detected DB is normal (fall back to the
  # convention silently); a genuine failure must surface via the generator's own
  # output rather than vanish — not Pgbus.logger, which is inappropriate at
  # generate time. Grounds the rescue in migrations_path_for.
  describe "#migrations_path_for" do
    let(:options) { {} }

    before do
      # The unit spec loads Rails as a bare module without an env; the mixin
      # guards on Rails.respond_to?(:env), so give it one for these cases.
      allow(Rails).to receive(:respond_to?).and_call_original
      allow(Rails).to receive(:respond_to?).with(:env).and_return(true)
      allow(Rails).to receive(:env).and_return("test")
    end

    it "returns the configured migrations_path when the DB is in configurations" do
      config = double("db_config", migrations_paths: ["db/pgbus_migrate"])
      allow(ActiveRecord::Base.configurations)
        .to receive(:configs_for).with(env_name: "test", name: "pgbus").and_return(config)

      expect(host.send(:migrations_path_for, "pgbus")).to eq("db/pgbus_migrate")
      expect(host.said).to be_empty
    end

    it "returns nil quietly when the DB is not in configurations (normal fallback case)" do
      allow(ActiveRecord::Base.configurations)
        .to receive(:configs_for).with(env_name: "test", name: "pgbus").and_return(nil)

      expect(host.send(:migrations_path_for, "pgbus")).to be_nil
      expect(host.said).to be_empty
    end

    it "surfaces an unexpected lookup failure via the generator output and returns nil" do
      allow(ActiveRecord::Base.configurations)
        .to receive(:configs_for).and_raise(StandardError, "malformed database.yml")

      expect(host.send(:migrations_path_for, "pgbus")).to be_nil
      expect(host.said.join).to include("could not resolve migrations_paths for \"pgbus\"")
      expect(host.said.join).to include("malformed database.yml")
    end
  end
end
