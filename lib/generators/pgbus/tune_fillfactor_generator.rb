# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class TuneFillfactorGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Set fillfactor on PGMQ queue tables to reduce page density and bloat"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "tune_fillfactor.rb.erb",
                           File.join(pgbus_migrate_path, "tune_pgbus_fillfactor.rb")
      end

      def display_post_install
        say ""
        say "Pgbus fillfactor tuning migration created!", :green
        say ""
        say "This migration sets fillfactor=#{Pgbus::TableMaintenance::FILLFACTOR} on all existing"
        say "PGMQ queue tables. This reserves #{100 - Pgbus::TableMaintenance::FILLFACTOR}% of each page to"
        say "reduce page density during PGMQ's heavy read UPDATE churn."
        say ""
        say "New queues created at runtime will automatically receive"
        say "this setting."
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
