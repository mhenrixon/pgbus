# frozen_string_literal: true

module RuboCop
  module Cop
    module Pgbus
      # Flags any use of Ruby's `Timeout.timeout`. It interrupts the target
      # thread with `Thread#raise`, which can fire at an arbitrary point —
      # including mid-libpq-call — and leave a pooled `PG::Connection` in a
      # corrupt state that re-hangs or returns wrong results on reuse. Prefer a
      # server-side/socket-level bound instead (Postgres `statement_timeout`,
      # libpq `tcp_user_timeout`/`keepalives`, an explicit `IO`/socket timeout).
      #
      # There is no autocorrect on purpose: the safe replacement depends on what
      # is being bounded, so a human must choose it.
      #
      # @example
      #   # bad
      #   Timeout.timeout(5) { do_work }
      #
      #   # good — bound the actual resource (DB query, socket) directly
      #   conn.exec("SET statement_timeout = 5000")
      #   do_work
      class NoRubyTimeout < Base
        MSG = "Please be careful, Timeout is dangerous. Timeout.timeout uses " \
              "Thread#raise and can corrupt a pooled connection mid-call. Bound the " \
              "resource itself instead (statement_timeout / tcp_user_timeout / socket timeout)."

        # Matches `Timeout.timeout(...)` and `::Timeout.timeout(...)`.
        # @!method ruby_timeout?(node)
        def_node_matcher :ruby_timeout?, <<~PATTERN
          (send (const {nil? cbase} :Timeout) :timeout ...)
        PATTERN

        def on_send(node)
          return unless ruby_timeout?(node)

          add_offense(node.loc.selector)
        end
      end
    end
  end
end
