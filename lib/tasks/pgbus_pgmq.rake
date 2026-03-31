# frozen_string_literal: true

namespace :pgbus do
  namespace :pgmq do
    desc "Show current PGMQ schema status (installed version, available updates)"
    task status: :environment do
      require "pgbus/pgmq_schema"

      puts "PGMQ Schema Status"
      puts "=" * 40

      latest = Pgbus::PgmqSchema.latest_version
      puts "Vendored version:  #{latest}"

      if ActiveRecord::Base.connection.table_exists?("pgbus_pgmq_schema_versions")
        row = ActiveRecord::Base.connection.select_one(
          "SELECT version, install_method, installed_at FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
        )
        if row
          puts "Installed version: #{row["version"]}"
          puts "Install method:    #{row["install_method"]}"
          puts "Installed at:      #{row["installed_at"]}"

          puts ""
          if Gem::Version.new(row["version"]) < Gem::Version.new(latest)
            puts "Update available! Run: rails generate pgbus:upgrade_pgmq"
          else
            puts "Schema is up to date."
          end
        else
          puts "Installed version: unknown (no records in tracking table)"
        end
      else
        # Check if pgmq schema exists at all
        schema_exists = ActiveRecord::Base.connection.select_value(
          "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'"
        )
        if schema_exists
          puts "Installed version: unknown (installed before version tracking)"
          puts ""
          puts "Run: rails generate pgbus:upgrade_pgmq"
          puts "This will add version tracking and ensure latest functions."
        else
          puts "PGMQ is not installed."
          puts ""
          puts "Run: rails generate pgbus:install"
        end
      end
    end

    desc "Show available vendored PGMQ versions"
    task versions: :environment do
      require "pgbus/pgmq_schema"

      puts "Available vendored PGMQ versions:"
      Pgbus::PgmqSchema.available_versions.each do |v|
        marker = v == Pgbus::PgmqSchema.latest_version ? " (latest)" : ""
        puts "  #{v}#{marker}"
      end
    end
  end
end
