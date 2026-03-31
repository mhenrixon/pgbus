# frozen_string_literal: true

module Pgbus
  # Manages embedded PGMQ SQL schema for extension-free installations.
  #
  # Supports three modes:
  #   :auto      - Try extension first, fall back to embedded SQL
  #   :extension - Require the pgmq PostgreSQL extension
  #   :embedded  - Use vendored SQL (no extension needed)
  #
  # Vendored SQL files live in lib/pgbus/pgmq_schema/pgmq_v{VERSION}.sql
  # and are exact copies of the upstream pgmq-extension/sql/pgmq.sql at each release.
  module PgmqSchema
    class VersionNotFoundError < StandardError; end

    SCHEMA_DIR = File.expand_path("pgmq_schema", __dir__).freeze

    class << self
      # Returns the latest vendored PGMQ version string.
      def latest_version
        available_versions.last
      end

      # Returns sorted list of all vendored PGMQ versions.
      def available_versions
        Dir.glob(File.join(SCHEMA_DIR, "pgmq_v*.sql"))
           .map { |f| File.basename(f).match(/pgmq_v(.+)\.sql/)[1] }
           .sort_by { |v| Gem::Version.new(v) }
      end

      # Returns the filesystem path to the vendored SQL file for a given version.
      #
      # @param version [String] e.g. "1.11.0"
      # @return [String] absolute path
      # @raise [VersionNotFoundError] if no SQL file exists for that version
      def sql_path(version)
        path = File.join(SCHEMA_DIR, "pgmq_v#{version}.sql")
        raise VersionNotFoundError, "No vendored PGMQ SQL for version #{version}" unless File.exist?(path)

        path
      end

      # Returns the raw SQL content for a given version.
      def sql_for_version(version)
        File.read(sql_path(version))
      end

      # Returns the SQL to install PGMQ schema without the extension.
      # Strips the extension-only pg_dump config blocks since they're
      # irrelevant when not installed as an extension.
      #
      # @param version [String] defaults to latest
      # @return [String] SQL
      def install_sql(version = latest_version)
        sql = sql_for_version(version)
        strip_extension_only_blocks(sql)
      end

      # Returns SQL to create the version tracking table and record an installation.
      #
      # @param version [String] defaults to latest
      # @return [String] SQL
      def version_tracking_sql(version = latest_version)
        <<~SQL
          CREATE TABLE IF NOT EXISTS pgbus_pgmq_schema_versions (
            id SERIAL PRIMARY KEY,
            version VARCHAR NOT NULL,
            installed_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            install_method VARCHAR NOT NULL DEFAULT 'embedded'
          );

          INSERT INTO pgbus_pgmq_schema_versions (version, install_method)
          VALUES ('#{version}', 'embedded');
        SQL
      end

      # Returns SQL to record an extension-based installation in the version tracking table.
      #
      # @param version [String]
      # @return [String] SQL
      def version_tracking_extension_sql(version = latest_version)
        <<~SQL
          CREATE TABLE IF NOT EXISTS pgbus_pgmq_schema_versions (
            id SERIAL PRIMARY KEY,
            version VARCHAR NOT NULL,
            installed_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            install_method VARCHAR NOT NULL DEFAULT 'embedded'
          );

          INSERT INTO pgbus_pgmq_schema_versions (version, install_method)
          VALUES ('#{version}', 'extension');
        SQL
      end

      # SQL to drop all pgmq functions/types (for clean upgrade).
      # Uses CASCADE so dependent objects are also dropped.
      def drop_pgmq_functions_sql
        <<~SQL
          DO $$
          DECLARE
            r RECORD;
          BEGIN
            -- Drop all functions in pgmq schema
            FOR r IN
              SELECT pg_catalog.pg_get_functiondef(p.oid) AS funcdef,
                     n.nspname || '.' || p.proname || '(' ||
                       pg_catalog.pg_get_function_identity_arguments(p.oid) || ')' AS func_sig
              FROM pg_catalog.pg_proc p
              JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
              WHERE n.nspname = 'pgmq'
            LOOP
              EXECUTE 'DROP FUNCTION IF EXISTS ' || r.func_sig || ' CASCADE';
            END LOOP;

            -- Drop custom types in pgmq schema
            FOR r IN
              SELECT n.nspname || '.' || t.typname AS type_name
              FROM pg_catalog.pg_type t
              JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
              WHERE n.nspname = 'pgmq'
                AND t.typtype = 'c'
                AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_class c
                  WHERE c.reltype = t.oid
                )
            LOOP
              EXECUTE 'DROP TYPE IF EXISTS ' || r.type_name || ' CASCADE';
            END LOOP;
          END $$;
        SQL
      end

      private

      # Strips extension-specific blocks (pg_extension_config_dump, pg_depend checks)
      # that only work when pgmq is installed as an extension.
      def strip_extension_only_blocks(sql)
        # Remove the DO block that conditionally creates schema only when extension is missing.
        # Replace with unconditional schema creation.
        sql = sql.sub(
          /DO\s*\$\$\s*BEGIN\s*IF\s*\(SELECT\s+NOT\s+EXISTS.*?END\s*\$\$;/m,
          "CREATE SCHEMA IF NOT EXISTS pgmq;"
        )

        # Remove pg_extension_config_dump blocks
        sql = sql.gsub(
          /DO\s*\$\$\s*BEGIN\s*IF\s+EXISTS\(SELECT\s+1\s+FROM\s+pg_extension.*?END\s*\$\$;/m,
          ""
        )

        # Remove _belongs_to_pgmq function (checks pg_depend on extension)
        sql.gsub(
          /CREATE FUNCTION pgmq\._belongs_to_pgmq.*?LANGUAGE plpgsql;/m,
          ""
        )
      end
    end
  end
end
