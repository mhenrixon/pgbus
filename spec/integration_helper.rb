# frozen_string_literal: true

require "active_record"
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

  # Connect ActiveRecord for model access
  ActiveRecord::Base.establish_connection(PGBUS_DATABASE_URL)

  # Configure pgbus with the real database
  Pgbus.configure do |c|
    c.database_url = PGBUS_DATABASE_URL
    c.queue_prefix = "pgbus_int"
    c.default_queue = "default"
    c.logger = Logger.new(IO::NULL)
    c.pgmq_schema_mode = :embedded
  end

  # Clean up queues and tables between tests
  config.around do |example|
    Pgbus.reset_client!
    example.run
  ensure
    cleanup_queues
    cleanup_tables
  end

  config.after(:suite) do
    Pgbus.reset!
  end
end

def cleanup_queues
  client = Pgbus.client
  queues = client.list_queues
  queues.each do |q|
    next unless q.to_s.start_with?("pgbus_int_")

    client.purge_queue(q.to_s.delete_prefix("pgbus_int_"))
  end
rescue StandardError => e
  warn "[pgbus integration] Queue cleanup warning: #{e.message}"
end

def cleanup_tables
  conn = ActiveRecord::Base.connection
  %w[pgbus_semaphores pgbus_blocked_executions pgbus_job_locks].each do |table|
    conn.execute("DELETE FROM #{table}")
  end
rescue StandardError => e
  warn "[pgbus integration] Table cleanup warning: #{e.message}"
end
