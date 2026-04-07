# frozen_string_literal: true

module Pgbus
  module Web
    # The worker-local singleton that owns SSE connections, one PG LISTEN
    # session, and the dispatch/heartbeat threads. Phase 1 ships only the
    # namespace and the in-memory Registry — the threaded coordinator and
    # integration with Puma arrive in Phase 3.
    module Streamer
    end
  end
end
