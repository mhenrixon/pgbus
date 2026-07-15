# frozen_string_literal: true

require_relative "../integration_helper"

# Proves the multi-version PGMQ schema upgrade path end to end against a real
# database. The integration suite already has PGMQ installed; we mark it as
# v1.11.1 in the tracking table, seed a queue with messages, then run the exact
# SQL the generated `upgrade_pgmq` migration emits to move to v1.12.0
# (drop functions -> re-create at target version -> record the upgrade).
#
# After the upgrade the tracking table must report v1.12.0, existing queues and
# messages must survive, the new read_grouped_head_with_poll function must be
# available, and the core pgmq functions must remain callable.
#
# Guarded by :integration — skipped cleanly when PGBUS_DATABASE_URL is unset.
RSpec.describe "PGMQ schema upgrade path (integration)", :integration do
  let(:conn) { ActiveRecord::Base.connection }
  let(:from_version) { "1.11.1" }
  let(:to_version)   { "1.12.0" }
  let(:queue_name)   { "pgbus_int_upgrade_probe" }

  # Mirrors lib/generators/pgbus/templates/upgrade_pgmq.rb.erb: drop functions,
  # re-create at the target version, record the upgrade in the tracking table.
  def run_upgrade_migration_sql(version)
    conn.execute(Pgbus::PgmqSchema.drop_pgmq_functions_sql)
    conn.execute(Pgbus::PgmqSchema.install_sql(version))
    conn.execute(<<~SQL)
      CREATE TABLE IF NOT EXISTS pgbus_pgmq_schema_versions (
        id SERIAL PRIMARY KEY,
        version VARCHAR NOT NULL,
        installed_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        install_method VARCHAR NOT NULL DEFAULT 'embedded'
      );

      INSERT INTO pgbus_pgmq_schema_versions (version, install_method)
      VALUES ('#{version}', 'upgrade');
    SQL
  end

  def function_exists?(name)
    conn.select_value(<<~SQL).to_i.positive?
      SELECT count(*) FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = 'pgmq' AND p.proname = '#{name}'
    SQL
  end

  # Install a clean function/type set for a version on top of whatever pgmq
  # objects already exist. Dropping the functions and the composite types first
  # keeps install_sql (which uses bare CREATE TYPE / CREATE FUNCTION) idempotent.
  def install_pgmq(version)
    conn.execute(Pgbus::PgmqSchema.drop_pgmq_functions_sql)
    %w[message_record queue_record metrics_result].each do |type|
      conn.execute("DROP TYPE IF EXISTS pgmq.#{type} CASCADE")
    end
    conn.execute(Pgbus::PgmqSchema.install_sql(version))
  end

  before do
    # Establish a clean v1.11.1 baseline (functions + tracking record) and seed
    # a queue with messages that must survive the upgrade.
    install_pgmq(from_version)

    conn.execute("DROP TABLE IF EXISTS pgbus_pgmq_schema_versions")
    conn.execute(Pgbus::PgmqSchema.version_tracking_sql(from_version))

    conn.execute("SELECT pgmq.drop_queue('#{queue_name}')") rescue nil # rubocop:disable Style/RescueModifier
    conn.execute("SELECT pgmq.create('#{queue_name}')")
    conn.execute("SELECT pgmq.send('#{queue_name}', '{\"probe\": 1}'::jsonb)")
    conn.execute("SELECT pgmq.send('#{queue_name}', '{\"probe\": 2}'::jsonb)")
  end

  after do
    # Only reachable when the DB is configured (the suite skips :integration
    # examples without PGBUS_DATABASE_URL before this hook can touch a connection).
    next unless PGBUS_DATABASE_URL

    # Clean up the probe queue and tracking table, then leave PGMQ installed at
    # the latest version so subsequent integration specs run against an
    # up-to-date schema.
    conn.execute("SELECT pgmq.drop_queue('#{queue_name}')") rescue nil # rubocop:disable Style/RescueModifier
    conn.execute("DROP TABLE IF EXISTS pgbus_pgmq_schema_versions")
    install_pgmq(Pgbus::PgmqSchema.latest_version)
  end

  it "records v1.11.1 before the upgrade" do
    latest = conn.select_value(
      "SELECT version FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
    )
    expect(latest).to eq(from_version)
  end

  it "advances the tracking table to v1.12.0 after the upgrade" do
    run_upgrade_migration_sql(to_version)

    latest = conn.select_value(
      "SELECT version FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
    )
    expect(latest).to eq(to_version)
  end

  it "records the upgrade install method" do
    run_upgrade_migration_sql(to_version)

    method = conn.select_value(
      "SELECT install_method FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
    )
    expect(method).to eq("upgrade")
  end

  it "preserves the existing queue and its messages across the upgrade" do
    run_upgrade_migration_sql(to_version)

    queue_present = conn.select_value(
      "SELECT count(*) FROM pgmq.meta WHERE queue_name = '#{queue_name}'"
    ).to_i
    expect(queue_present).to eq(1)

    message_count = conn.select_value("SELECT count(*) FROM pgmq.q_#{queue_name}").to_i
    expect(message_count).to eq(2)
  end

  it "makes the new read_grouped_head_with_poll function available after the upgrade" do
    run_upgrade_migration_sql(to_version)

    expect(function_exists?("read_grouped_head_with_poll")).to be(true)
  end

  it "keeps the core pgmq functions callable after the upgrade" do
    run_upgrade_migration_sql(to_version)

    read = conn.select_value(
      "SELECT count(*) FROM pgmq.read('#{queue_name}', 0, 10)"
    ).to_i
    expect(read).to eq(2)
  end
end
