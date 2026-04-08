# frozen_string_literal: true

require "active_record"
require "tempfile"
require "uri"
require "pgbus"

# Integration tests require a real PostgreSQL database with PGMQ installed.
# Set PGBUS_DATABASE_URL to connect:
#   export PGBUS_DATABASE_URL=postgres://pgbus:pgbus@localhost:5432/pgbus_test
PGBUS_DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL", nil)

def bootstrap_integration_tables(conn)
  unless conn.table_exists?("pgbus_uniqueness_keys")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_uniqueness_keys (
        lock_key VARCHAR NOT NULL,
        queue_name VARCHAR NOT NULL,
        msg_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_uniqueness_keys_key ON pgbus_uniqueness_keys (lock_key);
    SQL
  end

  unless conn.table_exists?("pgbus_presence_members")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_presence_members (
        stream_name VARCHAR NOT NULL,
        member_id VARCHAR NOT NULL,
        metadata JSONB NOT NULL DEFAULT '{}',
        joined_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_presence_members_pk
        ON pgbus_presence_members (stream_name, member_id);
      CREATE INDEX idx_pgbus_presence_members_sweep
        ON pgbus_presence_members (stream_name, last_seen_at);
    SQL
  end

  unless conn.table_exists?("pgbus_failed_events")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_failed_events (
        id BIGSERIAL PRIMARY KEY,
        queue_name VARCHAR NOT NULL,
        msg_id BIGINT,
        payload JSONB,
        headers JSONB,
        error_class VARCHAR,
        error_message TEXT,
        backtrace TEXT,
        retry_count INTEGER DEFAULT 0,
        failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    SQL
  end

  # Always ensure the unique index exists, even if the table was created
  # by an older bootstrap (e.g. CI workflow's "Set up pgbus tables" step
  # that predates the upsert). FailedEventRecorder.record! does
  # ON CONFLICT (queue_name, msg_id) UPDATE — without this index it raises
  # "no unique or exclusion constraint matching the ON CONFLICT specification"
  # and silently swallows the error, leaving failed_events.first nil in tests.
  conn.execute(<<~SQL)
    CREATE UNIQUE INDEX IF NOT EXISTS idx_pgbus_failed_events_queue_msg
      ON pgbus_failed_events (queue_name, msg_id);
  SQL
rescue StandardError => e
  warn "[pgbus integration] Bootstrap warning: #{e.message}"
end

RSpec.configure do |config|
  config.example_status_persistence_file_path = ".rspec_integration_status"
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.order = :random
  Kernel.srand config.seed

  unless PGBUS_DATABASE_URL
    config.before(:each, :integration) { skip "PGBUS_DATABASE_URL not set" }
    next
  end

  # Connect ActiveRecord with a pool large enough for concurrent tests.
  # Merge pool via URI parsing to avoid breaking URLs with existing query params.
  parsed = URI.parse(PGBUS_DATABASE_URL)
  url_params = URI.decode_www_form(parsed.query || "").to_h
  url_params["pool"] = "20"
  parsed.query = URI.encode_www_form(url_params)
  ActiveRecord::Base.establish_connection(parsed.to_s)

  # Configure pgbus with the real database
  Pgbus.configure do |c|
    c.database_url = PGBUS_DATABASE_URL
    c.queue_prefix = "pgbus_int"
    c.default_queue = "default"
    c.logger = Logger.new(IO::NULL)
    c.pgmq_schema_mode = :embedded
    c.listen_notify = false
  end

  # Bootstrap tables that may not exist in the CI database
  bootstrap_integration_tables(ActiveRecord::Base.connection)

  # Purge all integration queues BEFORE each test so no stale messages leak
  config.before do
    Pgbus.reset_client!
    purge_integration_queues
    cleanup_tables
  end

  config.after(:suite) do
    Pgbus.reset!
  end
end

# Purge all pgbus_* queues via raw SQL to avoid prefix double-application.
# Covers both pgbus_int_* (integration) and pgbus_test_* (unit test leakage).
def purge_integration_queues
  conn = ActiveRecord::Base.connection
  queue_names = conn.select_values("SELECT queue_name FROM pgmq.meta WHERE queue_name LIKE 'pgbus_%'")
  queue_names.each do |full_name|
    conn.execute("DELETE FROM pgmq.q_#{full_name}")
  rescue StandardError
    nil
  end
rescue StandardError => e
  warn "[pgbus integration] Queue purge warning: #{e.message}"
end

def cleanup_tables
  conn = ActiveRecord::Base.connection
  tables = %w[
    pgbus_semaphores pgbus_blocked_executions pgbus_uniqueness_keys
    pgbus_processes pgbus_recurring_executions pgbus_failed_events
    pgbus_presence_members
  ]
  tables.each do |table|
    conn.execute("DELETE FROM #{table}")
  rescue StandardError
    nil
  end
rescue StandardError => e
  warn "[pgbus integration] Table cleanup warning: #{e.message}"
end

# Build a dedicated PG::Connection for tests that need to LISTEN on the
# same database the integration suite is using. Parses every credential
# from PGBUS_DATABASE_URL — including the password, which is required
# in CI where Postgres has authentication enabled. Previously each
# streams integration spec defined its own copy of this helper, and
# every copy forgot the password, so all PG.connect calls in CI failed
# with `fe_sendauth: no password supplied`.
def build_pg_listen_connection
  require "pg"
  uri = URI.parse(PGBUS_DATABASE_URL)
  PG.connect(
    host: uri.host || "localhost",
    port: (uri.port || 5432).to_i,
    dbname: uri.path.delete_prefix("/"),
    user: uri.user || ENV.fetch("USER"),
    password: uri.password
  )
end
