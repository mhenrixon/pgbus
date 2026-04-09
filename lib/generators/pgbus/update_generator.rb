# frozen_string_literal: true

require "rails/generators"
require "pgbus/generators/config_converter"
require "pgbus/generators/migration_detector"
require "pgbus/generators/database_target_detector"

module Pgbus
  module Generators
    # Upgrade command with two independent jobs:
    #
    #   1. Config conversion: if config/pgbus.yml exists, convert it to
    #      config/initializers/pgbus.rb using the modern Ruby DSL. Skip
    #      silently if the initializer already exists or the YAML is
    #      absent — safe to re-run.
    #
    #   2. Migration detection: inspect the live database and add any
    #      missing pgbus migrations to db/migrate (or db/pgbus_migrate
    #      if a separate database is configured). Invokes each matching
    #      sub-generator in-process via Thor's invoke, so this mirrors
    #      what the user would get running each generator by hand.
    #
    # Usage:
    #
    #   bin/rails generate pgbus:update
    #   bin/rails generate pgbus:update --dry-run
    #   bin/rails generate pgbus:update --skip-config
    #   bin/rails generate pgbus:update --skip-migrations
    #   bin/rails generate pgbus:update --database=pgbus
    #   bin/rails generate pgbus:update --quiet
    class UpdateGenerator < Rails::Generators::Base
      desc "Upgrade pgbus: convert YAML config + add any missing migrations"

      class_option :source,
                   type: :string,
                   default: "config/pgbus.yml",
                   desc: "Path to an existing YAML config to convert (default: config/pgbus.yml)"

      class_option :destination,
                   type: :string,
                   default: "config/initializers/pgbus.rb",
                   desc: "Path to the generated initializer (default: config/initializers/pgbus.rb)"

      class_option :skip_config,
                   type: :boolean,
                   default: false,
                   desc: "Skip the YAML → Ruby initializer conversion step"

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

      def convert_yaml_if_present
        return if options[:skip_config]

        source_path = File.expand_path(options[:source], destination_root)
        destination_path = File.expand_path(options[:destination], destination_root)

        unless File.exist?(source_path)
          log "YAML config not found at #{options[:source]}; skipping config conversion."
          return
        end

        if File.exist?(destination_path)
          log "Initializer already exists at #{options[:destination]}; skipping config conversion."
          return
        end

        ruby_source = load_and_convert(source_path)
        if options[:dry_run]
          log_change "[dry-run] would create #{options[:destination]}"
        else
          create_file destination_path, ruby_source
        end
      end

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

      # Wrap converter and YAML errors as Thor::Error so the generator
      # surfaces them with the standard "in red, no backtrace, exit 1"
      # behavior. Catches:
      #   - ConfigConverter::Error (validation, missing file race)
      #   - Psych::Exception (malformed YAML, disallowed types)
      #   - Errno::ENOENT / Errno::EACCES (file disappeared / not readable)
      def load_and_convert(source_path)
        ConfigConverter.from_yaml(source_path)
      rescue ConfigConverter::Error, Psych::Exception, Errno::ENOENT, Errno::EACCES => e
        raise Thor::Error, "Failed to convert #{options[:source]}: #{e.message}"
      end

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
