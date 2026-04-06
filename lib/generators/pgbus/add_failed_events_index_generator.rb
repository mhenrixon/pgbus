# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class AddFailedEventsIndexGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Add unique index on pgbus_failed_events (queue_name, msg_id) for failure tracking upserts"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        if separate_database?
          migration_template "add_failed_events_unique_index.rb.erb",
                             "db/pgbus_migrate/add_pgbus_failed_events_unique_index.rb"
        else
          migration_template "add_failed_events_unique_index.rb.erb",
                             "db/migrate/add_pgbus_failed_events_unique_index.rb"
        end
      end

      def display_post_install
        say ""
        say "Pgbus failed events unique index added!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{":#{options[:database]}" if separate_database?}"
        say "  2. Failed jobs will now be tracked in the dashboard"
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
