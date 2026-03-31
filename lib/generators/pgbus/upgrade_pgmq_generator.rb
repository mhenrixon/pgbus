# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class UpgradePgmqGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Upgrade PGMQ schema to the latest vendored version"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration
        if separate_database?
          migration_template "upgrade_pgmq.rb.erb",
                             "db/pgbus_migrate/upgrade_pgmq_to_v#{target_version_slug}.rb"
        else
          migration_template "upgrade_pgmq.rb.erb",
                             "db/migrate/upgrade_pgmq_to_v#{target_version_slug}.rb"
        end
      end

      def display_post_upgrade
        say ""
        say "PGMQ upgrade migration created!", :green
        say "  Target version: #{target_version}", :yellow
        say ""
        say "Next steps:"
        say "  1. Review the migration in db/#{separate_database? ? "pgbus_migrate" : "migrate"}/"
        say "  2. Run: rails db:migrate#{":#{options[:database]}" if separate_database?}"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end

      def target_version
        Pgbus::PgmqSchema.latest_version
      end

      def target_version_slug
        target_version.tr(".", "_")
      end

      def separate_database?
        options[:database].present?
      end
    end
  end
end
