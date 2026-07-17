# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddStreamQueuesGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add stream queue registry so maintenance and wildcard workers can " \
           "tell stream queues apart from job queues (issues #308, #309)"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_stream_queues.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_stream_queues.rb")
      end

      def display_post_install
        say ""
        say "Pgbus stream queue registry installed!", :green
        say ""
        say "Without this table, stream queues are indistinguishable from job"
        say "queues: per-stream archive retention and the orphan sweep skip them,"
        say "and wildcard ('*') workers can claim durable broadcasts. Active"
        say "streams register themselves on their next broadcast after migrating."
        say "Dormant durable streams (completed checkout flows, ended chats) never"
        say "broadcast again, but doctor / orphan sweep / wildcard workers already"
        say "see them via the archive-index fingerprint (known_names). Backfill"
        say "persists those names in the registry and clears the doctor DEGRADED"
        say "hint (issue #366)."
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Backfill dormant streams: rake pgbus:streams:backfill_registry"
        say "  3. Restart pgbus: bin/pgbus start"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
