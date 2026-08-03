# frozen_string_literal: true

module Pgbus
  module Process
    # Immutable container-local readiness state, published by the supervisor
    # (one atomic swap per monitor pass) and read by the standalone health
    # server's accept thread — the immutability is what makes the cross-thread
    # handoff safe without a lock (issue #386).
    #
    # `expected` is the child count forked by boot_processes; `live` is the
    # current fork-table size. A child sitting in crash-restart backoff keeps
    # `live < expected`, which is exactly the signal a rolling deploy's health
    # gate needs to fail on: the replacement container never goes ready, and
    # the orchestrator keeps the old container running.
    ReadinessSnapshot = Data.define(:booted, :shutting_down, :expected, :live) do
      def ready?
        booted && !shutting_down && live >= expected
      end

      # DRAINING wins over BOOTING: a supervisor told to stop mid-boot is
      # leaving, not arriving, and must never look like it will become ready.
      def status
        return "DRAINING" if shutting_down
        return "BOOTING" unless booted

        ready? ? "OK" : "DEGRADED"
      end
    end
  end
end
