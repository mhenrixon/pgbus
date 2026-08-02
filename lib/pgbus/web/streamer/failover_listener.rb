# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # The worker-side seam between the two listening modes (issue #382):
      # starts on the master hub (HubClient) and fails over — once, one-way —
      # to a per-worker Listener when the hub transport dies (master gone,
      # ack deadline, eviction). The Dispatcher/Instance consume the same
      # ensure_listening/remove_listening/stop surface either way and never
      # learn which mode is active.
      #
      # Fallback direction is settled on #382: per-worker listener, not
      # poll-only — ephemeral broadcasts have no polling equivalent (their
      # payload exists only in the NOTIFY), so an outage trades connections
      # for unchanged semantics. Once fallen back, the worker stays local
      # until it recycles; no flap-back.
      #
      # The subscription set is recorded here so failover can rebuild the
      # exact LISTEN set on the fresh local connection before anything else
      # relies on it. ensure_listening NEVER raises to the dispatcher: on a
      # double failure (hub dead AND local build failing — e.g. DB down) it
      # logs and returns nil, matching the Listener's own ack-timeout
      # contract, which the dispatcher already tolerates.
      class FailoverListener
        def initialize(hub_client:, local_listener_factory:, logger: Pgbus.logger)
          @hub_client = hub_client
          @local_listener_factory = local_listener_factory
          @logger = logger
          @mutex = Mutex.new
          @subscriptions = Set.new
          @impl = hub_client
          @failed_over = false
        end

        def ensure_listening(queue)
          @mutex.synchronize { @subscriptions.add(queue) }
          current_impl.ensure_listening(queue)
        rescue HubClient::HubUnavailableError
          fail_over!
          begin
            current_impl.ensure_listening(queue)
          rescue HubClient::HubUnavailableError
            # fail_over! itself failed (factory raised) and @impl is still the
            # dead client — reported there; honor the nil-on-timeout contract.
            nil
          end
        end

        def remove_listening(queue)
          @mutex.synchronize { @subscriptions.delete(queue) }
          current_impl.remove_listening(queue)
        rescue HubClient::HubUnavailableError
          nil
        end

        # Idempotent, callable from the client's on_failure (reader thread)
        # and from a synchronous ensure failure (dispatcher thread) — the
        # mutex serializes them; the second caller finds the swap done.
        def fail_over!
          @mutex.synchronize do
            return if @failed_over

            @failed_over = true
            local = @local_listener_factory.call
            @subscriptions.each { |q| local.ensure_listening(q) }
            @impl = local
          end
        rescue StandardError => e
          # Hub dead AND the local listener can't be built (DB down, config
          # broken). Leave @impl on the dead client — every ensure_listening
          # resolves nil and the dispatcher rides its existing timeout
          # tolerance until the worker recycles.
          @logger.error do
            "[Pgbus::Streamer::FailoverListener] fallback listener failed to build " \
              "(#{e.class}: #{e.message}) — streams degraded until this worker recycles"
          end
        end

        def stop
          current_impl.stop
        end

        private

        def current_impl
          @mutex.synchronize { @impl }
        end
      end
    end
  end
end
