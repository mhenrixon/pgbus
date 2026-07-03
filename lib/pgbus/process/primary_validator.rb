# frozen_string_literal: true

module Pgbus
  module Process
    # Rejects a freshly built LISTEN connection that landed on a read-only
    # replica. After a PostgreSQL failover, stale DNS can point a fresh
    # `PG.connect` at the demoted master — now a replica. NOTIFY fires only on
    # the primary, so the listener would connect "successfully" and then sit
    # deaf forever, waking on nothing and degrading to slow fallback polling
    # with no explanation.
    #
    # `SELECT pg_is_in_recovery()` returns `t` on a replica (a standby in
    # recovery) and `f` on the primary. On a replica we raise
    # ReplicaConnectionError so the caller's existing reconnect loop closes the
    # connection, backs off, and retries. Each retry calls `PG.connect` fresh,
    # which re-resolves DNS, so the loop converges on the promoted master once
    # DNS catches up.
    #
    # These listeners own raw PG::Connections by design — the documented
    # exception to the "all PGMQ access through Pgbus::Client" rule — so the
    # validator operates directly on a PG::Connection. It is a sibling of
    # NotifyProbe (delivery self-probe); PrimaryValidator is the cheap
    # every-connection recovery check, NotifyProbe the one-shot start-only
    # delivery check.
    module PrimaryValidator
      RECOVERY_QUERY = "SELECT pg_is_in_recovery()"

      class << self
        # Runs the recovery check. Returns the connection unchanged on a primary
        # so callers can write `validate_primary!(build_connection)`. Raises
        # ReplicaConnectionError on a replica. A PG::Error from exec (dead
        # connection, etc.) propagates unchanged — callers already rescue
        # PG::Error and treat it as a reconnect trigger.
        def validate_primary!(conn)
          return conn unless in_recovery?(conn)

          raise ReplicaConnectionError,
                "LISTEN connection landed on a read-only replica " \
                "(pg_is_in_recovery() => t). After a failover, stale DNS can " \
                "point a fresh connection at the demoted master. NOTIFY fires " \
                "only on the primary, so this connection would never wake. " \
                "Rejecting it; the reconnect loop will re-resolve DNS and retry."
        end

        private

        def in_recovery?(conn)
          conn.exec(RECOVERY_QUERY).getvalue(0, 0) == "t"
        end
      end
    end
  end
end
