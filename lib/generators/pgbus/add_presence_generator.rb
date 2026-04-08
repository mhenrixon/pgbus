# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class AddPresenceGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Add presence_members table for Pgbus::Streams presence tracking"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        if separate_database?
          migration_template "add_presence.rb.erb",
                             "db/pgbus_migrate/add_pgbus_presence.rb"
        else
          migration_template "add_presence.rb.erb",
                             "db/migrate/add_pgbus_presence.rb"
        end
      end

      def display_post_install
        say ""
        say "Pgbus presence installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{":#{options[:database]}" if separate_database?}"
        say "  2. Use in your code:"
        say "       Pgbus.stream(@room).presence.join(member_id: current_user.id.to_s)"
        say "       Pgbus.stream(@room).presence.members"
        say "  3. Run a periodic sweeper to expire idle members:"
        say "       Pgbus.stream(@room).presence.sweep!(older_than: 60.seconds.ago)"
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
