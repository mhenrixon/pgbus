# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddJobLocksGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add job locks table for uniqueness guarantees"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_job_locks.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_job_locks.rb")
      end

      def display_post_install
        say ""
        say "DEPRECATED: pgbus:add_job_locks creates the legacy pgbus_job_locks table.", :yellow
        say "  The pgbus runtime no longer reads it -- ensures_uniqueness uses pgbus_uniqueness_keys.", :yellow
        say "  (pgbus:migrate_job_locks still reads it to migrate legacy rows across.)", :yellow
        say "  New installs: run `rails generate pgbus:add_uniqueness_keys` instead.", :yellow
        say "  Existing installs mid-migration: keep this generator alongside " \
            "`rails generate pgbus:migrate_job_locks`.", :yellow
        say ""
        say "Pgbus job locks table installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{":#{options[:database]}" if separate_database?}"
        say "  2. Add `ensures_uniqueness` to your job classes (DSL is auto-included)"
        say "  3. Restart pgbus: bin/pgbus start"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
