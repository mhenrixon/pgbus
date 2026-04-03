# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class AddJobStatsLatencyGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Add enqueue latency and retry count columns to pgbus_job_stats"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        if separate_database?
          migration_template "add_job_stats_latency.rb.erb",
                             "db/pgbus_migrate/add_pgbus_job_stats_latency.rb"
        else
          migration_template "add_job_stats_latency.rb.erb",
                             "db/migrate/add_pgbus_job_stats_latency.rb"
        end
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

      def separate_database?
        options[:database].present?
      end
    end
  end
end
