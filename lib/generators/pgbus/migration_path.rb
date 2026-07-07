# frozen_string_literal: true

require "pgbus/generators/database_target_detector"

module Pgbus
  module Generators
    # Shared migration path logic for all pgbus generators.
    #
    # A separate pgbus database can be selected two ways:
    #
    #   1. --database=pgbus explicitly on the generator invocation, or
    #   2. the host app has already configured Pgbus with a dedicated database
    #      (config.connects_to = { database: { writing: :pgbus } }).
    #
    # When either applies, migrations go to the separate-database path (e.g.
    # db/pgbus_migrate) and the post-install output names the db:migrate:<name>
    # task. When neither applies, everything falls back to db/migrate.
    #
    # Auto-detecting from connects_to (case 2) fixes issue #344: a bare
    # `rails g pgbus:add_*` in an app configured for a separate database used to
    # silently write to db/migrate and run against the WRONG database.
    module MigrationPath
      private

      def pgbus_migrate_path
        return "db/migrate" unless separate_database?

        # --database was passed: Rails' db_migrate_path reads options[:database]
        # and resolves migrations_paths from database.yml. When the database was
        # instead auto-detected from connects_to, options[:database] is nil, so
        # db_migrate_path can't see it — resolve the path for the detected
        # database ourselves.
        if options[:database].present?
          db_migrate_path
        else
          resolve_detected_migrate_path
        end
      end

      def separate_database?
        effective_database_name.present?
      end

      # The database name that migrations should target: an explicit --database
      # wins; otherwise auto-detect from the app's connects_to configuration.
      def effective_database_name
        return @effective_database_name if defined?(@effective_database_name)

        @effective_database_name =
          if options[:database].present?
            options[:database]
          else
            DatabaseTargetDetector.new(destination_root: destination_root).detect
          end
      end

      # The `:name` suffix appended to `rails db:migrate` in post-install
      # output. Empty string for single-database mode so the line reads a plain
      # `rails db:migrate`.
      def migrate_command_suffix
        separate_database? ? ":#{effective_database_name}" : ""
      end

      # Migrations path for an auto-detected separate database. Rails' own
      # db_migrate_path is keyed on options[:database], which is nil here, so
      # look up migrations_paths for the detected database directly and fall
      # back to the db/pgbus_migrate convention if it isn't in database.yml.
      def resolve_detected_migrate_path
        migrations_path_for(effective_database_name) || "db/pgbus_migrate"
      end

      def migrations_path_for(database_name)
        return nil unless defined?(::ActiveRecord::Base) && defined?(::Rails) && ::Rails.respond_to?(:env)

        config = ::ActiveRecord::Base.configurations.configs_for(
          env_name: ::Rails.env,
          name: database_name
        )
        Array(config&.migrations_paths).first
      rescue StandardError
        nil
      end
    end
  end
end
