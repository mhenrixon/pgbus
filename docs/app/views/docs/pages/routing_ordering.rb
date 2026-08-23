# frozen_string_literal: true

# Controlling which work runs first and in what order: priority sub-queues,
# fair share across tenants, consumer priority for active/standby workers, and
# single-active-consumer for strict ordering.
class Views::Docs::Pages::RoutingOrdering < DocsUI::Page
  title "Routing & ordering"
  eyebrow "Guide"

  def lead = "Priority sub-queues, fair share across tenants, active/standby consumer priority, and single-active-consumer for strict order."

  def content
    priority_queues
    fair_share
    consumer_priority
    single_active
  end

  private

  def priority_queues
    DocsUI::Section("Priority queues", description: "High-priority work processed first.") do
      md <<~'MD'
        Enable priority levels and each logical queue gains sub-queues (`_p0`
        highest through `_p2`). A worker drains `_p0` before it touches `_p1`, and
        `_p1` before `_p2`:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.priority_levels  = 3 # creates _p0, _p1, _p2 per logical queue
          config.default_priority = 1 # jobs without a priority go to _p1
        end
      RUBY
      md <<~'MD'
        Set a job's priority with Active Job's built-in `queue_with_priority`:
      MD
      DocsUI::Code(<<~RUBY)
        class CriticalAlertJob < ApplicationJob
          queue_as :default
          queue_with_priority 0 # highest
        end

        class ReportJob < ApplicationJob
          queue_as :default
          queue_with_priority 2 # lowest
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "With "
        code { "priority_levels" }
        plain " unset (the default), priority queues are off and each logical queue is a single queue."
      end
    end
  end

  def fair_share
    DocsUI::Section("Fair share across tenants", description: "One tenant's backlog must not starve everyone else.") do
      md <<~'MD'
        A queue is FIFO, so a tenant that enqueues 100 000 jobs puts every other
        tenant's work behind them. `config.fair_share` fixes that without
        per-tenant queues: a callable tags each job with a key (and an optional
        weight) at enqueue time, and the worker's read interleaves across keys —
        a weighted round-robin inside each queue.
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          # Return a key (String/Symbol/Integer), [key, weight], or nil to leave a job unkeyed.
          config.fair_share = ->(job) { [Current.tenant&.id, Current.tenant&.plan_weight || 1] }
        end
      RUBY
      md <<~'MD'
        How a read is split (batch of `qty`, the worker's idle capacity):

        - every key with **visible** messages gets its oldest messages ranked 1, 2, 3 …
        - a message's virtual time is `rank / weight`; the `qty` lowest win.
        - weight 3 vs weight 1 → a 3:1 split while both have work (default weight 1 = equal share).
        - **work-conserving**: a lone tenant still fills the whole batch — nobody is throttled on an idle worker.
        - memoryless across batches: each read is proportional on its own; there is no deficit carry-over.

        The key and weight ride inside the job payload (`pgbus_fair_key`,
        `pgbus_fair_weight`) — like `pgbus_concurrency_key` — so they survive
        concurrency-blocked promotion, dead-letter retry, the dashboard's retry,
        and `perform_all_later`. A weight change applies to newly enqueued jobs only.
      MD
      DocsUI::Table(
        [ "Combined with", "Behaviour" ],
        [
          [ [ :code, "priority_levels" ], "Strict between levels, fair within each level — p0 drains before p1, each level interleaved across keys." ],
          [ "multiple queues in a capsule", "Strict list-order priority across queues is kept; fair share applies within each queue." ],
          [ [ :code, "limits_concurrency" ], "Composes — use it with a tenant key when you also want a hard per-tenant in-flight cap." ],
          [ [ :code, "single_active_consumer" ], "Composes — the one active worker reads fairly." ],
          [ [ :code, "group_mode" ], "Mutually exclusive (raises at boot). PGMQ FIFO groups serialize a group; fair share interleaves it." ]
        ]
      )
      md <<~'MD'
        **Index.** The fair read uses an expression index
        `q_<queue>_fair_idx ((COALESCE(message->>'pgbus_fair_key','')), vt, msg_id)`.
        Queues created after you enable the option get it at creation; a worker
        builds it `CONCURRENTLY` for every queue it already serves at boot, so a
        populated queue is never write-locked. If a concurrent build is
        interrupted Postgres leaves an `INVALID` index behind — the worker logs
        the exact `DROP INDEX` to run before it will retry.

        **Cost.** Roughly 20 µs per key that currently has visible work, independent
        of backlog depth (200 active tenants ≈ 5 ms per read, single connection). Within
        a key messages are taken oldest-visible first (`vt, msg_id`), so a retried job
        sorts by when it became visible again rather than by its original position —
        a deliberate trade so a tenant's whole backlog is never sorted per read.
        See `docs/performance.md` for the before/after numbers.

        **Events too.** `config.event_fair_share = ->(event) { … }` applies the
        same read to event-bus consumers — the key is resolved at publish time
        and rides in the event envelope. See [Event bus](/docs/event-bus).
      MD
    end
  end

  def consumer_priority
    DocsUI::Section("Consumer priority", description: "Active/standby workers on the same queues.") do
      md <<~'MD'
        When several workers serve the same queues, higher-priority workers process
        first; lower-priority ones back off (3× the polling interval) while a
        higher-priority peer is active, and resume automatically when it goes
        stale.
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure do |c|
          c.capsule :primary,  queues: %w[default], threads: 10, consumer_priority: 10
          c.capsule :fallback, queues: %w[default], threads: 5,  consumer_priority: 0
        end
      RUBY
      md <<~'MD'
        Priority lives in heartbeat metadata; workers discover higher-priority
        peers by reading the `pgbus_processes` table.
      MD
    end
  end

  def single_active
    DocsUI::Section("Single active consumer", description: "Strict ordering via advisory locks.") do
      md <<~'MD'
        For a queue that must be processed in order, mark its capsules
        `single_active_consumer: true`. Only one worker reads the queue at a time —
        others skip it and work elsewhere. It uses non-blocking PostgreSQL
        session-level advisory locks, which auto-release on connection close, so a
        standby takes over within one polling tick if the primary dies:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure do |c|
          c.capsule :ordered_primary, queues: %w[ordered_events], threads: 1, single_active_consumer: true
          c.capsule :ordered_standby, queues: %w[ordered_events], threads: 1, single_active_consumer: true
        end
      RUBY
      DocsUI::Callout(:warning) do
        plain "Single active consumer serializes the queue to one worker thread — "
        plain "throughput is bounded by that one consumer. Use it only where ordering truly matters."
      end
    end
  end
end
