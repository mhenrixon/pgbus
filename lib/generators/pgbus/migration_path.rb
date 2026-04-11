# frozen_string_literal: true

module Pgbus
  module Generators
    # Shared migration path logic for all pgbus generators.
    #
    # When --database is passed, uses Rails' built-in db_migrate_path which
    # reads migrations_paths from database.yml. This is the standard Rails
    # multi-database mechanism and respects custom paths like db/pgbus_migrate/.
    #
    # Falls back to db/migrate/ when no --database is set (single-database mode).
    module MigrationPath
      private

      def pgbus_migrate_path
        if separate_database?
          db_migrate_path
        else
          "db/migrate"
        end
      end

      def separate_database?
        options[:database].present?
      end
    end
  end
end
