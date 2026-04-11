# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddJobStatsLatencyGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add enqueue latency and retry count columns to pgbus_job_stats"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_job_stats_latency.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_job_stats_latency.rb")
      end

      def display_post_install
        say ""
        say "Pgbus job stats latency columns installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{":#{options[:database]}" if separate_database?}"
        say "  2. Queue latency and retry metrics are now tracked automatically"
        say "  3. View latency insights at /pgbus/insights"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
