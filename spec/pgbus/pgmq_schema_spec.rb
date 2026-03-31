# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::PgmqSchema do
  describe ".latest_version" do
    it "returns the latest vendored PGMQ version" do
      expect(described_class.latest_version).to eq("1.11.0")
    end
  end

  describe ".sql_path" do
    it "returns the path to the vendored SQL file for a version" do
      path = described_class.sql_path("1.11.0")
      expect(path).to end_with("pgmq_schema/pgmq_v1.11.0.sql")
      expect(File.exist?(path)).to be true
    end

    it "raises for unknown versions" do
      expect { described_class.sql_path("0.0.0") }
        .to raise_error(Pgbus::PgmqSchema::VersionNotFoundError, /0\.0\.0/)
    end
  end

  describe ".available_versions" do
    it "returns sorted list of vendored versions" do
      versions = described_class.available_versions
      expect(versions).to include("1.11.0")
      expect(versions).to eq(versions.sort_by { |v| Gem::Version.new(v) })
    end
  end

  describe ".sql_for_version" do
    it "returns the SQL content for a version" do
      sql = described_class.sql_for_version("1.11.0")
      expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
      expect(sql).to include("CREATE FUNCTION pgmq.create(")
    end
  end

  describe ".install_sql" do
    it "returns SQL that creates pgmq schema without extension dependency" do
      sql = described_class.install_sql
      expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
      expect(sql).not_to include("CREATE EXTENSION")
    end
  end

  describe ".version_tracking_sql" do
    it "returns SQL to create the version tracking table" do
      sql = described_class.version_tracking_sql
      expect(sql).to include("pgbus_pgmq_schema_versions")
      expect(sql).to include("CREATE TABLE")
    end

    it "includes an insert for the installed version" do
      sql = described_class.version_tracking_sql("1.11.0")
      expect(sql).to include("1.11.0")
    end
  end
end
