# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"
require "rails/generators"
require "generators/pgbus/update_generator"

RSpec.describe Pgbus::Generators::UpdateGenerator do
  describe "class" do
    it "is a Rails::Generators::Base subclass" do
      expect(described_class.ancestors).to include(Rails::Generators::Base)
    end

    it "has a description" do
      expect(described_class.desc).to include("Upgrade pgbus")
      expect(described_class.desc).to include("migrations")
    end

    it "exposes --skip-migrations, --database, --dry-run, and --quiet options" do
      %i[skip_migrations database dry_run quiet].each do |opt|
        expect(described_class.class_options[opt]).not_to be_nil, "missing #{opt} option"
      end
    end

    it "no longer exposes the removed YAML-conversion options" do
      %i[source destination skip_config].each do |opt|
        expect(described_class.class_options[opt]).to be_nil, "unexpected #{opt} option still present"
      end
    end
  end

  describe "migration detection orchestration" do
    # These tests exercise the new orchestration layer added to
    # UpdateGenerator: given a detector result, verify the generator
    # translates it into the right invoke calls with the right args.
    #
    # MigrationDetector and DatabaseTargetDetector are covered by their
    # own unit specs; here we only care that the UpdateGenerator wires
    # them together correctly.

    let(:mock_connection) { double("ActiveRecord::Connection") }
    let(:tmpdir) { Dir.mktmpdir }

    before do
      FileUtils.mkdir_p(File.join(tmpdir, "config", "initializers"))
      FileUtils.mkdir_p(File.join(tmpdir, "db", "migrate"))

      # Replace AR::Base with a minimal stub so the generator's
      # "ActiveRecord::Base.connection" path fires without loading the
      # real ActiveRecord. The no-op abstract_class= lets Pgbus::BusRecord
      # autoload cleanly if connects_to is set in a test.
      fake_ar_base = Class.new do
        def self.abstract_class=(*); end
      end
      stub_const("ActiveRecord::Base", fake_ar_base)
      allow(ActiveRecord::Base).to receive(:connection).and_return(mock_connection)
      allow(Pgbus.configuration).to receive(:connects_to).and_return(nil)
    end

    after { FileUtils.rm_rf(tmpdir) }

    def build_generator(opts = {})
      defaults = { skip_migrations: false, dry_run: true, quiet: true }
      gen = described_class.new([], defaults.merge(opts))
      gen.destination_root = tmpdir
      gen
    end

    it "prints the fresh-install redirect when the detector reports FRESH_INSTALL" do
      allow(Pgbus::Generators::MigrationDetector).to receive(:new)
        .and_return(instance_double(Pgbus::Generators::MigrationDetector, missing_migrations: [Pgbus::Generators::MigrationDetector::FRESH_INSTALL]))

      generator = build_generator(quiet: false)
      allow(generator).to receive(:invoke)

      expect { generator.detect_and_install_missing_migrations }.to output(/fresh install/i).to_stdout
      expect(generator).not_to have_received(:invoke)
    end

    it "logs 'up to date' and does nothing when the detector returns an empty list" do
      allow(Pgbus::Generators::MigrationDetector).to receive(:new)
        .and_return(instance_double(Pgbus::Generators::MigrationDetector, missing_migrations: []))

      generator = build_generator(quiet: false)
      allow(generator).to receive(:invoke)

      expect { generator.detect_and_install_missing_migrations }.to output(/up to date/i).to_stdout
      expect(generator).not_to have_received(:invoke)
    end

    it "invokes each missing migration's generator in order" do
      allow(Pgbus::Generators::MigrationDetector).to receive(:new)
        .and_return(instance_double(Pgbus::Generators::MigrationDetector, missing_migrations: %i[add_presence add_stream_stats]))

      generator = build_generator(dry_run: false)
      allow(generator).to receive(:invoke)

      generator.detect_and_install_missing_migrations

      expect(generator).to have_received(:invoke).with("pgbus:add_presence", []).ordered
      expect(generator).to have_received(:invoke).with("pgbus:add_stream_stats", []).ordered
    end

    it "skips invocation on dry_run and prints 'would invoke'" do
      allow(Pgbus::Generators::MigrationDetector).to receive(:new)
        .and_return(instance_double(Pgbus::Generators::MigrationDetector, missing_migrations: %i[add_presence]))

      generator = build_generator(dry_run: true, quiet: false)
      allow(generator).to receive(:invoke)

      expect do
        generator.detect_and_install_missing_migrations
      end.to output(/would invoke pgbus:add_presence/).to_stdout
      expect(generator).not_to have_received(:invoke)
    end

    it "passes --database through when explicitly set via options" do
      allow(Pgbus::Generators::MigrationDetector).to receive(:new)
        .and_return(instance_double(Pgbus::Generators::MigrationDetector, missing_migrations: %i[add_presence]))

      generator = build_generator(dry_run: false, database: "queue_db")
      allow(generator).to receive(:invoke)

      generator.detect_and_install_missing_migrations

      expect(generator).to have_received(:invoke).with("pgbus:add_presence", ["--database=queue_db"])
    end

    it "auto-detects --database from Pgbus.configuration.connects_to" do
      allow(Pgbus.configuration).to receive(:connects_to).and_return(database: { writing: :pgbus })
      allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection)
      allow(Pgbus::Generators::MigrationDetector).to receive(:new)
        .and_return(instance_double(Pgbus::Generators::MigrationDetector, missing_migrations: %i[add_presence]))

      generator = build_generator(dry_run: false)
      allow(generator).to receive(:invoke)

      generator.detect_and_install_missing_migrations

      expect(generator).to have_received(:invoke).with("pgbus:add_presence", ["--database=pgbus"])
    end

    it "logs a skip message and does not invoke anything when ActiveRecord is not loaded" do
      hide_const("ActiveRecord::Base")

      generator = build_generator(quiet: false)
      allow(generator).to receive(:invoke)

      expect do
        generator.detect_and_install_missing_migrations
      end.to output(/ActiveRecord not loaded/).to_stdout
      expect(generator).not_to have_received(:invoke)
    end
  end
end
