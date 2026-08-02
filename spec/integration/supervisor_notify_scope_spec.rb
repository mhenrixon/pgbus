# frozen_string_literal: true

require "integration_helper"

# Issue #381 acceptance, against real PostgreSQL + real LISTEN/NOTIFY:
#   - a NotifyHub serves multiple "forks" over ONE census-tagged connection
#   - wakes route only to the fork(s) reading the notifying queue
#   - killing the shared LISTEN backend (pg_terminate_backend) is survived:
#     the listener reconnects, re-LISTENs, and wakes flow again
#
# The degraded (P) broadcast is deliberately NOT asserted here: on a local
# Postgres the reconnect completes in well under one hub tick, so observing
# the transient P is a race. That transition is pinned deterministically in
# spec/pgbus/process/notify_hub_spec.rb; this spec pins the end-to-end
# recovery the broadcast exists to report.
RSpec.describe "Supervisor-owned shared LISTEN (issue #381)", :integration do
  let(:config) { Pgbus.configuration }
  let(:logger) { Logger.new(IO::NULL) }

  def wait_until(timeout: 5)
    deadline = Time.now + timeout
    until yield
      raise "timed out waiting for condition" if Time.now > deadline

      sleep 0.05
    end
  end

  def drain(reader)
    reader.read_nonblock(1024)
  rescue IO::WaitReadable
    ""
  end

  def wait_for_wake(reader, timeout: 5)
    deadline = Time.now + timeout
    buffer = +""
    while Time.now < deadline
      buffer << drain(reader)
      return buffer if buffer.include?("W")

      sleep 0.05
    end
    buffer
  end

  def listen_backend_pids
    ActiveRecord::Base.connection.select_values(<<~SQL)
      SELECT pid FROM pg_stat_activity
      WHERE application_name = 'pgbus-listen' AND datname = current_database()
    SQL
  end

  # The integration suite runs with listen_notify=false to keep its
  # connection footprint small; this spec needs the NOTIFY triggers, so flip
  # it (and a matching capsule list for the hub's union) around each example.
  around do |example|
    original_listen_notify = config.listen_notify
    original_workers = config.workers
    config.listen_notify = true
    config.workers = [{ queues: %w[wake_a wake_b], threads: 1 }]
    example.run
  ensure
    config.listen_notify = original_listen_notify
    config.workers = original_workers
  end

  before do
    Pgbus.client.ensure_queue("wake_a")
    Pgbus.client.ensure_queue("wake_b")
    Pgbus.client.purge_queue("wake_a")
    Pgbus.client.purge_queue("wake_b")
  end

  it "routes wakes over one shared connection and survives a killed LISTEN backend" do
    hub = Pgbus::Process::NotifyHub.new(config: config, logger: logger)
    reader_a, writer_a = IO.pipe
    reader_b, writer_b = IO.pipe

    begin
      # Baseline before OUR hub starts, so a stray listener from another
      # example (or a future parallel runner) can't break the census delta.
      baseline_pids = listen_backend_pids
      hub.start
      wait_until(timeout: 10) { hub.healthy? }
      hub.register_fork(pid: 100, queues: [config.queue_name("wake_a")], pipe: writer_a)
      hub.register_fork(pid: 200, queues: [config.queue_name("wake_b")], pipe: writer_b)
      drain(reader_a)
      drain(reader_b)

      # Acceptance: the whole "host" (two forks) pins exactly ONE direct
      # LISTEN connection beyond the baseline, countable via the census
      # application_name.
      expect((listen_backend_pids - baseline_pids).size).to eq(1)

      # Targeted routing: an insert on wake_a wakes fork A only. The hub
      # writes each fork's pipe independently, so give a mis-routed byte for
      # fork B a bounded settle window before asserting its absence —
      # otherwise the negative assertion could pass against a byte still in
      # flight.
      Pgbus.client.send_message("wake_a", { "n" => 1 })
      expect(wait_for_wake(reader_a)).to include("W")
      sleep 0.2
      expect(drain(reader_b)).not_to include("W")

      # Chaos: kill the shared LISTEN backend out from under the hub.
      old_pids = listen_backend_pids
      ActiveRecord::Base.connection.execute(<<~SQL)
        SELECT pg_terminate_backend(pid) FROM pg_stat_activity
        WHERE application_name = 'pgbus-listen' AND datname = current_database()
      SQL

      # Recovery: a NEW backend appears (fresh connection, re-LISTENed) …
      wait_until(timeout: 10) do
        pids = listen_backend_pids
        !pids.empty? && !pids.intersect?(old_pids)
      end
      wait_until(timeout: 10) do
        hub.tick
        hub.healthy?
      end
      drain(reader_a)

      # … and wakes flow again. Sleep past the 250ms NOTIFY throttle window
      # so this insert fires a fresh NOTIFY rather than being coalesced.
      sleep 0.3
      Pgbus.client.send_message("wake_a", { "n" => 2 })
      expect(wait_for_wake(reader_a, timeout: 10)).to include("W")
    ensure
      hub.stop
      [reader_a, reader_b, writer_a, writer_b].each { |io| io.close unless io.closed? }
      Pgbus.client.purge_queue("wake_a")
      Pgbus.client.purge_queue("wake_b")
    end
  end
end
