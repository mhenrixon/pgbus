# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddJobStatsQueueIndexGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add composite index on pgbus_job_stats (queue_name, created_at) for Insights latency-by-queue aggregation"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_job_stats_queue_index.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_job_stats_queue_index.rb")
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
    end
  end
end
