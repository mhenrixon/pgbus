# frozen_string_literal: true

module Pgbus
  module Generators
    # Inspects a live ActiveRecord connection and determines which of
    # pgbus's migration generators need to run to bring the schema up
    # to date.
    #
    # Usage:
    #
    #   detector = Pgbus::Generators::MigrationDetector.new(connection)
    #   detector.missing_migrations
    #   # => [:add_uniqueness_keys, :add_job_stats_queue_index, ...]
    #
    # The returned symbols correspond to generator names that the
    # pgbus:update generator invokes via Thor composition. Each symbol
    # maps to exactly one generator (see GENERATOR_MAP).
    #
    # Detection rules:
    #
    # 1. Fresh install (no core tables) → returns [:fresh_install] as a
    #    sentinel. The caller should tell the user to run pgbus:install
    #    instead of stacking 8 migrations.
    #
    # 2. Core tables missing → queued unconditionally. These are features
    #    pgbus assumes are present.
    #
    # 3. Opt-in feature tables missing → queued unconditionally. The user
    #    asked for update-to-latest, so we add the migration files but
    #    don't enable the feature in config. They'll opt in separately.
    #
    # 4. Columns missing on existing tables → queued. These are in-place
    #    schema upgrades (e.g. add_job_stats_latency adds
    #    enqueue_latency_ms + retry_count).
    #
    # 5. Indexes missing on existing tables → queued. Additive, safe.
    #
    # 6. Modern replacements for legacy tables (e.g. pgbus_job_locks →
    #    pgbus_uniqueness_keys) → queue the migration path only if the
    #    legacy table still exists. Otherwise queue the fresh install.
    class MigrationDetector
      # Sentinel returned when the database looks empty of pgbus tables.
      # The caller (pgbus:update generator) should redirect the user to
      # pgbus:install rather than trying to stack the full schema as
      # individual add_* migrations.
      FRESH_INSTALL = :fresh_install

      # The set of tables that the base pgbus:install migration creates.
      # If NONE of these exist, we treat the DB as a fresh install.
      CORE_INSTALL_TABLES = %w[
        pgbus_processed_events
        pgbus_processes
        pgbus_failed_events
        pgbus_semaphores
        pgbus_blocked_executions
        pgbus_batches
      ].freeze

      # generator_key → Rails generator name. Passed to Thor's invoke.
      #
      # Note: uniqueness_keys uses the migrate_job_locks generator for
      # both the fresh-install and upgrade-from-job_locks paths. The
      # template is idempotent: `unless table_exists?(:pgbus_uniqueness_keys)`
      # creates it, and `if table_exists?(:pgbus_job_locks)` drops the
      # legacy table. One generator covers both cases.
      GENERATOR_MAP = {
        uniqueness_keys: "pgbus:migrate_job_locks",
        add_job_stats: "pgbus:add_job_stats",
        add_job_stats_latency: "pgbus:add_job_stats_latency",
        add_job_stats_queue_index: "pgbus:add_job_stats_queue_index",
        add_stream_stats: "pgbus:add_stream_stats",
        add_presence: "pgbus:add_presence",
        add_queue_states: "pgbus:add_queue_states",
        add_outbox: "pgbus:add_outbox",
        add_recurring: "pgbus:add_recurring",
        add_failed_events_index: "pgbus:add_failed_events_index",
        tune_autovacuum: "pgbus:tune_autovacuum",
        tune_fillfactor: "pgbus:tune_fillfactor"
      }.freeze

      # Human-friendly description of each migration for the generator
      # output. Keeps the update generator's run log readable.
      DESCRIPTIONS = {
        uniqueness_keys: "uniqueness keys table (job deduplication, also upgrades legacy job_locks if present)",
        add_job_stats: "job stats table (Insights dashboard)",
        add_job_stats_latency: "job stats latency columns (enqueue_latency_ms, retry_count)",
        add_job_stats_queue_index: "job stats (queue_name, created_at) index",
        add_stream_stats: "stream stats table (opt-in real-time Insights)",
        add_presence: "presence members table (Turbo Streams presence)",
        add_queue_states: "queue states table (pause/resume)",
        add_outbox: "outbox entries table (transactional outbox)",
        add_recurring: "recurring tasks + executions tables",
        add_failed_events_index: "unique index on pgbus_failed_events (queue_name, msg_id)",
        tune_autovacuum: "autovacuum tuning for PGMQ queue and archive tables",
        tune_fillfactor: "fillfactor=70 on PGMQ queue tables (reduces page density during update churn)"
      }.freeze

      def initialize(connection)
        @connection = connection
      end

      # Returns an Array of generator keys (symbols) in the order they
      # should run. Dependencies are resolved implicitly via order: the
      # base table creation for a feature always comes before the
      # column/index add-ons.
      def missing_migrations
        return [FRESH_INSTALL] if fresh_install?

        [
          *uniqueness_key_migrations,
          *job_stats_migrations,
          *stream_stats_migrations,
          *presence_migrations,
          *queue_states_migrations,
          *outbox_migrations,
          *recurring_migrations,
          *failed_events_index_migrations,
          *autovacuum_migrations,
          *fillfactor_migrations
        ]
      end

      private

      attr_reader :connection

      def fresh_install?
        CORE_INSTALL_TABLES.none? { |t| table_exists?(t) }
      end

      # Legacy pgbus_job_locks → modern pgbus_uniqueness_keys.
      # The migrate_job_locks template is idempotent: it creates
      # uniqueness_keys if missing and drops job_locks if present.
      # One symbol covers both cases; see GENERATOR_MAP.
      def uniqueness_key_migrations
        return [] if table_exists?("pgbus_uniqueness_keys")

        [:uniqueness_keys]
      end

      def job_stats_migrations
        migrations = []

        unless table_exists?("pgbus_job_stats")
          migrations << :add_job_stats
          # The latency columns and queue index are add-ons to job_stats.
          # If we're creating the base table now, the add_job_stats
          # template doesn't include them — add the upgrade migrations
          # so a fresh install lands on the current schema.
          migrations << :add_job_stats_latency
          migrations << :add_job_stats_queue_index
          return migrations
        end

        # Base table exists — check each add-on independently.
        cols = column_names("pgbus_job_stats")
        migrations << :add_job_stats_latency unless cols.include?("enqueue_latency_ms") && cols.include?("retry_count")

        migrations << :add_job_stats_queue_index unless index_exists?("pgbus_job_stats", "idx_pgbus_job_stats_queue_time")

        migrations
      end

      def stream_stats_migrations
        table_exists?("pgbus_stream_stats") ? [] : [:add_stream_stats]
      end

      def presence_migrations
        table_exists?("pgbus_presence_members") ? [] : [:add_presence]
      end

      def queue_states_migrations
        table_exists?("pgbus_queue_states") ? [] : [:add_queue_states]
      end

      def outbox_migrations
        table_exists?("pgbus_outbox_entries") ? [] : [:add_outbox]
      end

      def recurring_migrations
        # pgbus_recurring_tasks + pgbus_recurring_executions are the two
        # tables the recurring generator creates. If BOTH exist, nothing
        # to do. If either is missing, we need the generator (which is
        # idempotent via if_not_exists on both tables).
        return [] if table_exists?("pgbus_recurring_tasks") && table_exists?("pgbus_recurring_executions")

        [:add_recurring]
      end

      def failed_events_index_migrations
        # pgbus_failed_events is created by pgbus:install, but the
        # unique (queue_name, msg_id) index was added later via its own
        # generator. If the table exists without the unique index,
        # FailedEventRecorder's upsert silently swallows ON CONFLICT
        # errors — so this is a real bug waiting to bite.
        return [] unless table_exists?("pgbus_failed_events")
        return [] if index_exists?("pgbus_failed_events", "idx_pgbus_failed_events_queue_msg")

        [:add_failed_events_index]
      end

      # Autovacuum tuning: check if any PGMQ queue table already has
      # custom autovacuum settings applied. If not, queue the migration.
      def autovacuum_migrations
        return [] unless pgmq_schema_exists?
        return [] if autovacuum_already_tuned?

        [:tune_autovacuum]
      end

      # Fillfactor tuning: check if any PGMQ queue table already has
      # fillfactor applied. If not, queue the migration.
      def fillfactor_migrations
        return [] unless pgmq_schema_exists?
        return [] if fillfactor_already_tuned?

        [:tune_fillfactor]
      end

      # --- schema probes -------------------------------------------------

      def table_exists?(name)
        connection.table_exists?(name)
      rescue StandardError
        false
      end

      def column_names(table)
        connection.columns(table).map(&:name)
      rescue StandardError
        []
      end

      def index_exists?(table, index_name)
        connection.indexes(table).any? { |idx| idx.name == index_name }
      rescue StandardError
        false
      end

      def pgmq_schema_exists?
        result = connection.select_value("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'")
        result.present?
      rescue StandardError
        false
      end

      def autovacuum_already_tuned?
        queue_name = connection.select_value("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
        return true unless queue_name # no queues = nothing to tune, skip

        result = connection.select_value(<<~SQL)
          SELECT reloptions::text LIKE '%autovacuum_vacuum_scale_factor%'
          FROM pg_class
          WHERE relname = 'q_#{queue_name}'
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgmq')
        SQL

        [true, "t"].include?(result)
      rescue StandardError
        true # if we can't tell, assume already tuned (safe default)
      end

      def fillfactor_already_tuned?
        queue_name = connection.select_value("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
        return true unless queue_name # no queues = nothing to tune, skip

        result = connection.select_value(<<~SQL)
          SELECT reloptions::text LIKE '%fillfactor%'
          FROM pg_class
          WHERE relname = 'q_#{queue_name}'
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgmq')
        SQL

        [true, "t"].include?(result)
      rescue StandardError
        true # if we can't tell, assume already tuned (safe default)
      end
    end
  end
end
