# frozen_string_literal: true

require "active_record"
require "tempfile"
require "pgbus"

# Integration tests require a real PostgreSQL database with PGMQ installed.
# Set PGBUS_DATABASE_URL to connect:
#   export PGBUS_DATABASE_URL=postgres://pgbus:pgbus@localhost:5432/pgbus_test
PGBUS_DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL", nil)

RSpec.configure do |config|
  config.example_status_persistence_file_path = ".rspec_integration_status"
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.order = :random
  Kernel.srand config.seed

  unless PGBUS_DATABASE_URL
    config.before { skip "PGBUS_DATABASE_URL not set" }
    next
  end

  # Connect ActiveRecord with a pool large enough for concurrent tests
  ActiveRecord::Base.establish_connection("#{PGBUS_DATABASE_URL}?pool=20")

  # Configure pgbus with the real database
  Pgbus.configure do |c|
    c.database_url = PGBUS_DATABASE_URL
    c.queue_prefix = "pgbus_int"
    c.default_queue = "default"
    c.logger = Logger.new(IO::NULL)
    c.pgmq_schema_mode = :embedded
    c.listen_notify = false
  end

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

# Purge all pgbus_int_* queues via raw SQL to avoid prefix double-application
def purge_integration_queues
  conn = ActiveRecord::Base.connection
  queue_names = conn.select_values("SELECT queue_name FROM pgmq.meta WHERE queue_name LIKE 'pgbus_int_%'")
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
  %w[pgbus_semaphores pgbus_blocked_executions pgbus_job_locks pgbus_processes pgbus_recurring_executions].each do |table|
    conn.execute("DELETE FROM #{table}")
  end
rescue StandardError => e
  warn "[pgbus integration] Table cleanup warning: #{e.message}"
end
