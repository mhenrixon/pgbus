# frozen_string_literal: true

require "integration_helper"

# Issue #437 regression, against a real PostgreSQL LISTEN connection:
#
# A forked child inherits a copy of the parent's LISTEN socket fd AND the
# PG::Connection object that owns it. When the child's GC frees that object,
# libpq's PQfinish sends a Terminate message down the fd — i.e. down the
# PARENT's connection — and the server closes it. The parent then logs
# "connection error ... reconnecting" once per fork and loses LISTEN until
# reconnect! completes.
#
# Closing the Ruby IO wrapper does not help: pg builds socket_io with
# autoclose=false. The child must repoint the fd at /dev/null instead.
#
# The Timeout.timeout below bounds a Process.waitpid2 on a forked CHILD — no
# pooled PG::Connection is in the timed block, so the Thread#raise hazard
# Pgbus/NoRubyTimeout guards against does not apply.
RSpec.describe "NotifyListener fork hygiene (issue #437)", :integration do
  let(:config) { Pgbus.configuration }
  let(:log_io) { StringIO.new }
  let(:logger) { Logger.new(log_io) }
  let(:physical_queue) { config.queue_name("fork_hygiene") }
  let(:channel) { "pgmq.q_#{physical_queue}.INSERT" }

  def poll_until(timeout:)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      result = yield
      return result if result
      return false if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.05
    end
  end

  def listen_backend_pids
    ActiveRecord::Base.connection.select_values(<<~SQL)
      SELECT pid FROM pg_stat_activity
      WHERE application_name = 'pgbus-listen' AND datname = current_database()
    SQL
  end

  around do |example|
    original_listen_notify = config.listen_notify
    config.listen_notify = true
    example.run
  ensure
    config.listen_notify = original_listen_notify
  end

  before { Pgbus.client.ensure_queue("fork_hygiene") }

  it "keeps the parent's LISTEN backend alive after a child releases and GCs the inherited connection" do
    wakes = Concurrent::AtomicFixnum.new(0)
    listener = Pgbus::Process::NotifyListener.new(
      physical_queues: [physical_queue],
      on_wake: ->(_channel) { wakes.increment },
      connection_options: config.worker_notify_connection_options,
      health_check_ms: 200,
      logger: logger
    )
    listener.start

    begin
      expect(poll_until(timeout: 5.0) { listener.listening_to.include?(channel) }).to be_truthy
      pids_before = listen_backend_pids
      expect(pids_before.size).to eq(1)

      # Mirror Supervisor#setup_child_process: the child drops its copy of the
      # LISTEN connection, then GC frees the inherited PG::Connection, which
      # runs PQfinish. Several GC passes make the finalization deterministic.
      child = fork do
        listener.close_inherited_socket!
        5.times { GC.start }
        exit!(0)
      end

      Timeout.timeout(10) do # rubocop:disable Pgbus/NoRubyTimeout -- bounds waitpid2 on a child, not a PG connection
        _, status = Process.waitpid2(child)
        expect(status.exitstatus).to eq(0)
      end

      # Give the server time to act on a stray Terminate and the listener a
      # full health-check cycle to notice a dead backend before we assert.
      sleep 1.0

      before = wakes.value
      poll_until(timeout: 5.0) do
        Pgbus.client.send_message("fork_hygiene", { "job_class" => "Noop" })
        wakes.value > before
      end
      expect(wakes.value).to be > before

      expect(listen_backend_pids).to eq(pids_before)
      expect(log_io.string).not_to include("connection error")
    ensure
      listener.stop
    end
  end
end
