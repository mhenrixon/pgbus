# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class InstallGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Install Pgbus: create migration, config file, and binstub"

      class_option :pgmq_schema_mode,
                   type: :string,
                   default: "auto",
                   desc: "PGMQ install mode: auto (try extension, fallback embedded), " \
                         "extension (require ext), embedded (no ext)"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus). " \
                         "Migrations go to db/pgbus_migrate/ and schema to db/pgbus_schema.rb"

      def create_migration
        if separate_database?
          migration_template "migration.rb.erb",
                             "db/pgbus_migrate/create_pgbus_tables.rb"
        else
          migration_template "migration.rb.erb",
                             "db/migrate/create_pgbus_tables.rb"
        end
      end

      def create_config_file
        template "pgbus.yml.erb", "config/pgbus.yml"
      end

      def create_binstub
        template "pgbus_binstub.erb", "bin/pgbus"
        chmod "bin/pgbus", 0o755
      end

      def configure_active_job
        application_config = "config.active_job.queue_adapter = :pgbus"

        return unless File.exist?(File.join(destination_root, "config", "application.rb"))

        inject_into_file "config/application.rb",
                         "\n    #{application_config}\n",
                         after: "class Application < Rails::Application\n"
      end

      def configure_separate_database
        return unless separate_database?

        db_config = <<~YAML

          # Pgbus separate database (added by pgbus:install --database=#{database_name})
          # Uncomment and configure for your environment:
          # #{database_name}:
          #   primary:
          #     <<: *default
          #     database: #{Rails.application.class.module_parent_name.underscore}_#{database_name}_<%= Rails.env %>
          #     migrations_paths: db/pgbus_migrate
        YAML

        say ""
        say "Separate database mode enabled!", :yellow
        say "Add the following to your config/database.yml:", :yellow
        say db_config, :cyan
        say ""
        say "Then add to config/application.rb or an initializer:", :yellow
        say "  Pgbus.configure { |c| c.connects_to = { database: { writing: :pgbus } } }", :cyan
        say ""
      end

      def display_post_install
        say ""
        say "Pgbus installed successfully!", :green
        say ""
        say "PGMQ schema mode: #{pgmq_schema_mode}", :yellow
        case pgmq_schema_mode
        when "auto"
          say "  The migration will try the pgmq extension first."
          say "  If unavailable, it falls back to embedded SQL (no extension needed)."
        when "extension"
          say "  The migration requires the pgmq PostgreSQL extension."
          say "  Install it: CREATE EXTENSION pgmq;"
        when "embedded"
          say "  The migration uses embedded SQL — no pgmq extension needed."
          say "  PGMQ #{Pgbus::PgmqSchema.latest_version} schema will be created directly."
        end

        if separate_database?
          say ""
          say "Migrations path: db/pgbus_migrate/", :yellow
          say "Schema dump: db/pgbus_schema.rb", :yellow
        end

        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{":#{database_name}" if separate_database?}"
        say "  2. Edit config/pgbus.yml to configure workers"
        say "  3. Start processing: bin/pgbus start"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end

      def pgmq_schema_mode
        options[:pgmq_schema_mode]
      end

      def database_name
        options[:database]
      end

      def separate_database?
        database_name.present?
      end
    end
  end
end
