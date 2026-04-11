# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddStreamStatsGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add stream stats table for Insights dashboard (opt-in, disabled by default)"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_stream_stats.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_stream_stats.rb")
      end

      def display_post_install
        say ""
        say "Pgbus stream stats table installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{":#{options[:database]}" if separate_database?}"
        say "  2. Opt in by setting `config.streams_stats_enabled = true` in your"
        say "     pgbus initializer (disabled by default — stream event volume can"
        say "     be high, so stats recording is off unless you ask for it)."
        say "  3. View stream insights at /pgbus/insights"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
