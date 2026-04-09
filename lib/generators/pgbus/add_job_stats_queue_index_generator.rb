# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class AddJobStatsQueueIndexGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Add composite index on pgbus_job_stats (queue_name, created_at) for Insights latency-by-queue aggregation"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        if separate_database?
          migration_template "add_job_stats_queue_index.rb.erb",
                             "db/pgbus_migrate/add_pgbus_job_stats_queue_index.rb"
        else
          migration_template "add_job_stats_queue_index.rb.erb",
                             "db/migrate/add_pgbus_job_stats_queue_index.rb"
        end
      end

      def display_post_install
        say ""
        say "Pgbus job stats queue index added!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{":#{options[:database]}" if separate_database?}"
        say "  2. The Insights 'latency by queue' aggregation will now use the index"
        say "     instead of sequentially scanning pgbus_job_stats. Install this on"
        say "     heavy-traffic deployments with a large job stats retention window."
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
