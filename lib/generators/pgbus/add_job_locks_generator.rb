# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class AddJobLocksGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Add job locks table for uniqueness guarantees"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        if separate_database?
          migration_template "add_job_locks.rb.erb",
                             "db/pgbus_migrate/add_pgbus_job_locks.rb"
        else
          migration_template "add_job_locks.rb.erb",
                             "db/migrate/add_pgbus_job_locks.rb"
        end
      end

      def display_post_install
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

      def separate_database?
        options[:database].present?
      end
    end
  end
end
