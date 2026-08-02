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
          # @state_mutex guards the cheap shared state (@subscriptions, @impl,
          # @failed_over) and is only ever held for constant-time work — the
          # dispatcher's ensure/remove path must never wait behind a failover
          # build. @failover_mutex serializes the (blocking) build + replay:
          # a fresh PG connect + N re-LISTEN acks can stall for seconds when
          # the trigger IS a database problem (review on #384).
          @state_mutex = Mutex.new
          @failover_mutex = Mutex.new
          @subscriptions = Set.new
          @impl = hub_client
          @failed_over = false
        end

        # Interface parity with Listener for Instance#start: the hub client
        # connected at construction and the fallback starts itself on swap.
        def start
          self
        end

        def ensure_listening(queue)
          @state_mutex.synchronize { @subscriptions.add(queue) }
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
          @state_mutex.synchronize { @subscriptions.delete(queue) }
          current_impl.remove_listening(queue)
        rescue HubClient::HubUnavailableError => e
          @logger.debug do
            "[Pgbus::Streamer::FailoverListener] remove_listening on a dead hub client " \
              "(#{e.message}) — ignoring, unlisten GC is best-effort"
          end
          nil
        end

        # Idempotent, callable from the client's on_failure (reader thread)
        # and from a synchronous ensure failure (dispatcher thread).
        # @failover_mutex serializes concurrent callers — the second blocks
        # until the first finishes and then no-ops, so a synchronous retry
        # after fail_over! always lands on the swapped-in local listener.
        # The blocking build + replay runs OUTSIDE @state_mutex so concurrent
        # ensure/remove/stop calls never stall behind it.
        def fail_over!
          @failover_mutex.synchronize do
            return if @state_mutex.synchronize { @failed_over }

            local = @local_listener_factory.call
            @state_mutex.synchronize { @subscriptions.dup }.each { |q| local.ensure_listening(q) }
            # Subscriptions recorded between the snapshot and this swap arrive
            # via their own retried ensure_listening call on the new impl.
            @state_mutex.synchronize do
              @impl = local
              @failed_over = true
            end
          end
        rescue StandardError => e
          # Hub dead AND the local listener can't be built (DB down, config
          # broken). Mark failed-over so callers stop rebuilding; @impl stays
          # on the dead client — every ensure_listening resolves nil and the
          # dispatcher rides its existing timeout tolerance until the worker
          # recycles.
          @state_mutex.synchronize { @failed_over = true }
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
          @state_mutex.synchronize { @impl }
        end
      end
    end
  end
end
