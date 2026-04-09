# frozen_string_literal: true

module Pgbus
  module Generators
    # Detects whether the host application has configured pgbus to use
    # a separate database via connects_to, and returns the database name
    # that --database flags should be set to for sub-generators.
    #
    # Detection sources, in priority order:
    #
    #   1. Pgbus.configuration.connects_to (runtime — authoritative if
    #      the initializer has already booted)
    #   2. config/initializers/pgbus.rb (text scan)
    #   3. config/application.rb (text scan — fallback for apps that
    #      call connects_to in application config instead of the pgbus
    #      initializer)
    #
    # Returns a String (the database name, e.g. "pgbus") or nil if no
    # separate database is configured.
    class DatabaseTargetDetector
      # Matches:
      #   c.connects_to = { database: { writing: :pgbus } }
      #   c.connects_to(database: { writing: :queue_db })
      #   Pgbus.configuration.connects_to = { database: { writing: :pgbus } }
      #
      # Rejects:
      #   c.connects_to = { role: :writing }        (different API shape)
      #   connects_to :something                    (no database: key)
      #
      # The [^;\n]*? and [^}]*? laziness keeps the scan within a
      # reasonable window so a stray "writing:" later in the file
      # can't cross-contaminate.
      CONNECTS_TO_PATTERN = /connects_to\b[^;\n]*?database\s*:\s*\{[^}]*?writing\s*:\s*:?(?<name>[a-zA-Z_][a-zA-Z0-9_]*)/m

      def initialize(destination_root:)
        @destination_root = destination_root
      end

      # Returns the database name string if a separate DB is configured,
      # nil otherwise. Checks runtime config first, then falls back to
      # static file scanning.
      def detect
        runtime_database_name || scan_initializer || scan_application_config
      end

      private

      attr_reader :destination_root

      # Runtime path: Pgbus.configuration.connects_to. Only works if the
      # host app has loaded pgbus AND booted the initializer. The update
      # generator runs after Rails app boot so this is usually available.
      def runtime_database_name
        return nil unless defined?(Pgbus) && Pgbus.respond_to?(:configuration)

        extract_database_name(Pgbus.configuration.connects_to)
      rescue StandardError
        nil
      end

      def scan_initializer
        scan_file(File.join(destination_root, "config", "initializers", "pgbus.rb"))
      end

      def scan_application_config
        scan_file(File.join(destination_root, "config", "application.rb"))
      end

      def scan_file(path)
        return nil unless File.exist?(path)

        content = File.read(path)
        match = content.match(CONNECTS_TO_PATTERN)
        return nil unless match

        match[:name]
      rescue StandardError
        nil
      end

      # Parses `{ database: { writing: :name } }` or the String variant
      # into the database name. Returns nil for anything we don't
      # recognize.
      def extract_database_name(connects_to)
        return nil unless connects_to.is_a?(Hash)

        db = connects_to[:database] || connects_to["database"]
        return nil unless db.is_a?(Hash)

        (db[:writing] || db["writing"])&.to_s
      end
    end
  end
end
