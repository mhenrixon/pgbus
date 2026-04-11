# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddFailedEventsIndexGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add unique index on pgbus_failed_events (queue_name, msg_id) for failure tracking upserts"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_failed_events_unique_index.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_failed_events_unique_index.rb")
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
    end
  end
end
