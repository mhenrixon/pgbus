# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module Pgbus
  module Generators
    class InstallGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration

      source_root File.expand_path("templates", __dir__)

      desc "Install Pgbus: create migration, config file, and binstub"

      def create_migration
        migration_template "migration.rb.erb", "db/migrate/install_pgbus.rb"
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

        if File.exist?(File.join(destination_root, "config", "application.rb"))
          inject_into_file "config/application.rb",
            "\n    #{application_config}\n",
            after: "class Application < Rails::Application\n"
        end
      end

      def display_post_install
        say ""
        say "Pgbus installed successfully!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate"
        say "  2. Edit config/pgbus.yml to configure workers"
        say "  3. Start processing: bin/pgbus start"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
