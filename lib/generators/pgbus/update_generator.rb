# frozen_string_literal: true

require "rails/generators"
require "pgbus/generators/migration_detector"
require "pgbus/generators/database_target_detector"

module Pgbus
  module Generators
    # Upgrade command: inspect the live database and add any missing pgbus
    # migrations to db/migrate (or db/pgbus_migrate if a separate database is
    # configured). Invokes each matching sub-generator in-process via Thor's
    # invoke, so this mirrors what the user would get running each generator
    # by hand.
    #
    # Usage:
    #
    #   bin/rails generate pgbus:update
    #   bin/rails generate pgbus:update --dry-run
    #   bin/rails generate pgbus:update --skip-migrations
    #   bin/rails generate pgbus:update --database=pgbus
    #   bin/rails generate pgbus:update --quiet
    class UpdateGenerator < Rails::Generators::Base
      desc "Upgrade pgbus: add any missing migrations"

      class_option :skip_migrations,
                   type: :boolean,
                   default: false,
                   desc: "Skip the migration detection step"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (default: auto-detect " \
                         "from Pgbus.configuration.connects_to or config/initializers/pgbus.rb)"

      class_option :dry_run,
                   type: :boolean,
                   default: false,
                   desc: "Print what would be done without creating any files"

      class_option :quiet,
                   type: :boolean,
                   default: false,
                   desc: "Suppress verbose per-step output"

      def detect_and_install_missing_migrations
        return if options[:skip_migrations]

        unless active_record_available?
          log "ActiveRecord not loaded — skipping migration detection. Run this generator from a Rails app."
          return
        end

        connection = resolve_connection
        unless connection
          log "No ActiveRecord connection available — skipping migration detection."
          return
        end

        detector = MigrationDetector.new(connection)
        missing = detector.missing_migrations

        if missing.empty?
          log "Database schema is up to date — no migrations needed."
          return
        end

        if missing == [MigrationDetector::FRESH_INSTALL]
          say ""
          say "Database looks empty of pgbus tables — this is a fresh install.", :yellow
          say "Run `rails generate pgbus:install` instead of `pgbus:update`.", :yellow
          say ""
          return
        end

        database_name = options[:database] || detected_database_name
        log "Auto-detected separate database: #{database_name}" if options[:database].nil? && database_name

        log "Found #{missing.size} missing migration(s):"
        missing.each do |key|
          description = MigrationDetector::DESCRIPTIONS[key] || key.to_s
          log "  - #{key}: #{description}"
        end

        # Two loops on purpose: print the full plan first so operators
        # see what's coming, then execute. Combining would interleave
        # "  - add_presence: foo" with "Invoking pgbus:add_presence..."
        # which hides the shape of the upgrade from the reader.
        missing.each do |key| # rubocop:disable Style/CombinableLoops
          generator = MigrationDetector::GENERATOR_MAP[key]
          unless generator
            say "  !  no generator mapped for #{key}, skipping", :red
            next
          end

          if options[:dry_run]
            log_change "[dry-run] would invoke #{generator}#{" --database=#{database_name}" if database_name}"
            next
          end

          invoke_args = []
          invoke_args << "--database=#{database_name}" if database_name
          log "Invoking #{generator}#{" --database=#{database_name}" if database_name}..."
          invoke generator, invoke_args
        end
      end

      def display_post_install
        return if options[:quiet]

        say ""
        say "Pgbus update complete.", :green
        say ""
        if options[:dry_run]
          say "Dry-run: no files were created.", :yellow
        else
          say "Next steps:"
          say "  1. Review the generated migration files in db/migrate (or db/pgbus_migrate)"
          say "  2. Run: rails db:migrate#{":#{effective_database_name}" if effective_database_name}"
          say "  3. Restart pgbus: bin/pgbus start"
        end
        say ""
      end

      private

      def active_record_available?
        defined?(::ActiveRecord::Base) && ::ActiveRecord::Base.respond_to?(:connection)
      end

      # Resolve the AR connection to inspect. If pgbus is configured to
      # use a separate database (via connects_to), use BusRecord's
      # connection so the detector probes the right schema.
      def resolve_connection
        if defined?(Pgbus) && Pgbus.respond_to?(:configuration) && Pgbus.configuration.connects_to
          Pgbus::BusRecord.connection
        else
          ::ActiveRecord::Base.connection
        end
      rescue StandardError => e
        say "  !  could not resolve AR connection: #{e.class}: #{e.message}", :red
        nil
      end

      def detected_database_name
        @detected_database_name ||= DatabaseTargetDetector.new(
          destination_root: destination_root
        ).detect
      end

      def effective_database_name
        options[:database] || detected_database_name
      end

      def log(message)
        return if options[:quiet]

        say message
      end

      def log_change(message)
        return if options[:quiet]

        say message, :yellow
      end
    end
  end
end
