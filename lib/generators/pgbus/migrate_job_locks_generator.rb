# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class MigrateJobLocksGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Migrate pgbus_job_locks to lightweight pgbus_uniqueness_keys table"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        if separate_database?
          migration_template "migrate_job_locks_to_uniqueness_keys.rb.erb",
                             "db/pgbus_migrate/migrate_pgbus_job_locks_to_uniqueness_keys.rb"
        else
          migration_template "migrate_job_locks_to_uniqueness_keys.rb.erb",
                             "db/migrate/migrate_pgbus_job_locks_to_uniqueness_keys.rb"
        end
      end

      def display_post_install
        say ""
        say "Pgbus uniqueness keys migration created!", :green
        say ""
        say "This migration will:"
        say "  1. Create the new pgbus_uniqueness_keys table (3 columns, 1 index)"
        say "  2. Migrate existing locks from pgbus_job_locks"
        say "  3. Drop the old pgbus_job_locks table (8 columns, 3 indexes)"
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

      def separate_database?
        options[:database].present?
      end
    end
  end
end
