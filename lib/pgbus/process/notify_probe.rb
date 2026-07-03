# frozen_string_literal: true

module Pgbus
  module Process
    # One-shot self-probe that verifies a freshly built LISTEN connection can
    # actually receive a NOTIFY it sends to itself. Two real-world failure modes
    # break LISTEN/NOTIFY silently, and both are caught here:
    #
    #   1. Transaction-mode PgBouncer — LISTEN registrations do not survive the
    #      transaction boundaries the pooler inserts, so a self-NOTIFY is never
    #      delivered (the wait times out).
    #   2. Read-only replica — `pg_notify()` runs inside a read-only transaction
    #      and raises, so the probe never even reaches the wait.
    #
    # In both cases the caller "connected successfully" but would never wake on a
    # real INSERT, degrading to slow fallback polling with no explanation. The
    # probe surfaces the condition with an actionable error naming the direct-
    # connection overrides, then returns false so the caller can degrade
    # gracefully (workers keep polling; the streamer starts anyway).
    #
    # These listeners own raw PG::Connections by design — the documented
    # exception to the "all PGMQ access through Pgbus::Client" rule — so the
    # probe operates directly on a PG::Connection.
    module NotifyProbe
      PROBE_TIMEOUT_SECONDS = 2

      class << self
        # Runs the probe on an already-built LISTEN connection. Returns true when
        # the self-NOTIFY is delivered within PROBE_TIMEOUT_SECONDS, false on
        # timeout or when pg_notify raises. Never raises: a probe is diagnostic,
        # so a failure must not take down listener startup.
        def probe_notify_delivery!(conn, logger: Pgbus.logger)
          channel = probe_channel
          begin
            conn.exec(%(LISTEN "#{channel}"))
            conn.exec_params("SELECT pg_notify($1, '')", [channel])
            delivered = wait_for_probe(conn)
            log_failure(logger) unless delivered
            delivered
          rescue PG::Error => e
            # Read-only replica: pg_notify raised inside a read-only transaction.
            log_failure(logger, error: e)
            false
          ensure
            safe_unlisten(conn, channel)
          end
        end

        private

        # Unique per process AND per invocation so two probes (worker + streamer
        # in the same process, or a retry) never collide on the same channel.
        def probe_channel
          "pgbus_probe_#{::Process.pid}_#{probe_nonce}"
        end

        def probe_nonce
          require "securerandom"
          SecureRandom.hex(4)
        end

        def wait_for_probe(conn)
          delivered = false
          conn.wait_for_notify(PROBE_TIMEOUT_SECONDS) { |_channel, _pid, _payload| delivered = true }
          delivered
        end

        def safe_unlisten(conn, channel)
          conn.exec(%(UNLISTEN "#{channel}"))
        rescue PG::Error
          nil
        end

        def log_failure(logger, error: nil)
          detail = error ? " (#{error.class}: #{error.message})" : ""
          logger.error do
            "[Pgbus::NotifyProbe] LISTEN/NOTIFY self-probe failed#{detail}: a self-NOTIFY was not delivered " \
              "within #{PROBE_TIMEOUT_SECONDS}s. This connection cannot receive wake-ups — it is likely a " \
              "transaction-mode pooler (e.g. PgBouncer) that drops LISTEN, or a read-only replica. The " \
              "listener will fall back to slow polling. Point the LISTEN connection at a DIRECT database " \
              "port: for workers set worker_notify_database_url (or worker_notify_host / worker_notify_port); " \
              "for the streamer set streams_database_url (or streams_host / streams_port)."
          end
        end
      end
    end
  end
end
