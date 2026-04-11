# frozen_string_literal: true

namespace :pgbus do
  desc "Apply autovacuum tuning to PGMQ queue/archive tables and high-churn pgbus tables"
  task tune_autovacuum: :environment do
    require "pgbus/autovacuum_tuning"

    conn = Pgbus.configuration.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection

    # Only run if pgmq schema exists (tables may not be created yet during
    # initial setup — the install migration handles tuning itself).
    pgmq_exists = conn.select_value(
      "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'"
    )

    unless pgmq_exists
      puts "[pgbus] PGMQ schema not found — skipping autovacuum tuning."
      next
    end

    puts "[pgbus] Applying autovacuum tuning to PGMQ queue/archive tables..."
    conn.execute(Pgbus::AutovacuumTuning.sql_for_all_queues)

    puts "[pgbus] Applying autovacuum tuning to high-churn pgbus tables..."
    conn.execute(Pgbus::AutovacuumTuning.sql_for_high_churn_tables)

    puts "[pgbus] Autovacuum tuning complete."
  end
end

# Reapply autovacuum settings after schema:load since Ruby-format schema.rb
# does not preserve ALTER TABLE ... SET (reloptions). This is a no-op if the
# tables don't exist yet (IF EXISTS guards in the SQL).
%w[db:schema:load db:schema:load:pgbus].each do |task_name|
  next unless Rake::Task.task_defined?(task_name)

  Rake::Task[task_name].enhance do
    Rake::Task["pgbus:tune_autovacuum"].invoke if Rake::Task.task_defined?("pgbus:tune_autovacuum")
  end
end
