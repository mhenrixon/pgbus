# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class AddRecurringGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Add recurring jobs tables to an existing Pgbus installation"

      def create_migration
        migration_template "add_recurring_tables.rb.erb",
                           "db/migrate/add_pgbus_recurring_tables.rb"
      end

      def create_recurring_config
        template "recurring.yml.erb", "config/recurring.yml"
      end

      def display_post_install
        say ""
        say "Pgbus recurring jobs installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate"
        say "  2. Edit config/recurring.yml to define your recurring tasks"
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
