# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class TuneAutovacuumGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Tune autovacuum settings on PGMQ queue and archive tables for optimal queue health"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "tune_autovacuum.rb.erb",
                           File.join(pgbus_migrate_path, "tune_pgbus_autovacuum.rb")
      end

      def display_post_install
        say ""
        say "Pgbus autovacuum tuning migration created!", :green
        say ""
        say "This migration applies aggressive autovacuum settings to all existing"
        say "PGMQ queue and archive tables. New queues created at runtime will"
        say "automatically receive these settings."
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{":#{options[:database]}" if separate_database?}"
        say "  2. Restart pgbus: bin/pgbus start"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
