# Pgbus

PostgreSQL-native job processing and event bus for Rails, built on [PGMQ](https://github.com/tembo-io/pgmq).

**Why Pgbus?** If you already run PostgreSQL, you don't need Redis for background jobs. Pgbus gives you ActiveJob integration, AMQP-style topic routing, dead letter queues, worker memory management, and a live dashboard -- all backed by your existing database.

[![Ruby](https://github.com/mhenrixon/pgbus/actions/workflows/main.yml/badge.svg)](https://github.com/mhenrixon/pgbus/actions/workflows/main.yml)

## Table of contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Concurrency controls](#concurrency-controls)
- [Batches](#batches)
- [Job uniqueness](#job-uniqueness)
- [Priority queues](#priority-queues)
- [Single active consumer](#single-active-consumer)
- [Consumer priority](#consumer-priority)
- [Circuit breaker and queue pause/resume](#circuit-breaker-and-queue-pauseresume)
- [Prefetch flow control](#prefetch-flow-control)
- [Transactional outbox](#transactional-outbox)
- [Archive compaction](#archive-compaction)
- [Configuration reference](#configuration-reference)
- [Architecture](#architecture)
- [CLI](#cli)
- [Dashboard](#dashboard)
- [Real-time broadcasts (turbo-streams replacement)](#real-time-broadcasts-turbo-streams-replacement)
- [Database tables](#database-tables)
- [Switching from another backend](#switching-from-another-backend)
- [Development](#development)
- [License](#license)

## Features

- **ActiveJob adapter** -- drop-in replacement, zero config migration from other backends
- **Turbo Streams replacement** -- `pgbus_stream_from` drops into turbo-rails apps with no ActionCable, no Redis, no lost messages on reconnect (fixes rails/rails#52420, hotwired/turbo#1261)
- **Event bus** -- publish/subscribe with AMQP-style topic routing (`orders.#`, `payments.*`)
- **Dead letter queues** -- automatic DLQ routing after configurable retries
- **Worker recycling** -- memory, job count, and lifetime limits prevent runaway processes
- **LISTEN/NOTIFY** -- instant wake-up, polling as fallback only
- **Idempotent events** -- deduplication via `(event_id, handler_class)` unique index with in-memory cache
- **Live dashboard** -- Turbo Frames auto-refresh with throughput rate, no ActionCable required
- **Supervisor/worker model** -- forked processes with heartbeat monitoring and lifecycle state machine
- **Priority queues** -- route jobs to priority sub-queues, highest-priority-first processing
- **Circuit breaker** -- auto-pause queues after consecutive failures, exponential backoff
- **Queue pause/resume** -- manual or automatic via dashboard
- **Prefetch flow control** -- cap in-flight messages per worker to prevent overload
- **Archive compaction** -- automatic purge of old archived messages
- **Transactional outbox** -- publish events atomically inside database transactions
- **Single active consumer** -- advisory-lock-based exclusive queue processing for strict ordering
- **Consumer priority** -- higher-priority workers get first dibs, lower-priority workers back off
- **Job uniqueness** -- prevent duplicate jobs with reaper-based crash recovery, no TTL-driven expiry

## Requirements

- Ruby >= 3.3
- Rails >= 7.1
- PostgreSQL with the [PGMQ extension](https://github.com/tembo-io/pgmq)

## Installation

Add to your Gemfile:

```ruby
gem "pgbus"
```

Then install the PGMQ extension in your database:

```sql
CREATE EXTENSION IF NOT EXISTS pgmq;
```

## Quick start

### 1. Configure (optional)

Pgbus works with zero config in Rails -- it uses your existing `ActiveRecord` connection. For custom setups, create `config/pgbus.yml`:

```yaml
production:
  queue_prefix: myapp
  default_queue: default
  pool_size: 10
  max_retries: 5
  prefetch_limit: 20
  workers:
    - queues: [default, mailers]
      threads: 10
      consumer_priority: 10
    - queues: [critical]
      threads: 5
      single_active_consumer: true
    - queues: [default, mailers]
      threads: 5
      consumer_priority: 0    # fallback worker
  event_consumers:
    - queues: [orders, payments]
      threads: 5
  max_jobs_per_worker: 10000
  max_memory_mb: 512
  max_worker_lifetime: 3600
```

Or configure in an initializer:

```ruby
# config/initializers/pgbus.rb
Pgbus.configure do |config|
  config.queue_prefix = "myapp"
  config.max_retries = 5
  config.max_jobs_per_worker = 10_000
  config.max_memory_mb = 512
  config.max_worker_lifetime = 3600

  config.workers = [
    { queues: %w[default mailers], threads: 10 },
    { queues: %w[critical], threads: 5 }
  ]
end
```

### 2. Use as ActiveJob backend

```ruby
# config/application.rb
config.active_job.queue_adapter = :pgbus
```

That's it. Your existing jobs work unchanged:

```ruby
class OrderConfirmationJob < ApplicationJob
  queue_as :mailers

  def perform(order)
    OrderMailer.confirmation(order).deliver_now
  end
end

# Enqueue
OrderConfirmationJob.perform_later(order)

# Schedule
OrderConfirmationJob.set(wait: 5.minutes).perform_later(order)
```

### 3. Event bus (optional)

Publish events with AMQP-style topic routing:

```ruby
# Publish an event
Pgbus::EventBus::Publisher.publish(
  "orders.created",
  { order_id: order.id, total: order.total }
)

# Publish with delay
Pgbus::EventBus::Publisher.publish_later(
  "invoices.due",
  { invoice_id: invoice.id },
  delay: 30.days
)
```

Subscribe with handlers:

```ruby
# app/handlers/order_created_handler.rb
class OrderCreatedHandler < Pgbus::EventBus::Handler
  idempotent!  # Deduplicate by (event_id, handler_class)

  def handle(event)
    order_id = event.payload["order_id"]
    Analytics.track_order(order_id)
    InventoryService.reserve(order_id)
  end
end

# Register in an initializer
Pgbus::EventBus::Registry.instance.subscribe(
  "orders.created",
  OrderCreatedHandler
)

# Wildcard patterns
Pgbus::EventBus::Registry.instance.subscribe(
  "orders.#",           # matches orders.created, orders.updated, orders.shipped.confirmed
  OrderAuditHandler
)
```

### 4. Start workers

```bash
bundle exec pgbus start
```

This boots a supervisor that manages:
- **Workers** -- process ActiveJob queues
- **Dispatcher** -- runs maintenance tasks (idempotency cleanup, stale process reaping)
- **Consumers** -- process event bus messages

### 5. Mount the dashboard

```ruby
# config/routes.rb
mount Pgbus::Engine => "/pgbus"
```

The dashboard shows queues, jobs, processes, failures, dead letter messages, and event subscribers. It auto-refreshes via Turbo Frames with no WebSocket dependency.

Protect it in production with a simple auth lambda:

```ruby
Pgbus.configure do |config|
  config.web_auth = ->(request) {
    request.env["warden"].user&.admin?
  }
end
```

Or inherit from your own authenticated controller (like mission_control-jobs):

```ruby
Pgbus.configure do |config|
  config.base_controller_class = "Admin::BaseController"
end
```

When `base_controller_class` is set, all dashboard controllers inherit from that class instead of `ActionController::Base`. This is the recommended approach when mounting the dashboard inside an authenticated namespace -- your base controller's `before_action` filters, helper methods, and authentication logic apply automatically without monkey-patching.

Add a "back to app" button in the dashboard nav to return to your main application:

```ruby
Pgbus.configure do |config|
  config.return_to_app_url = "/admin"
end
```

## Concurrency controls

Limit how many jobs with the same key can run concurrently:

```ruby
class ProcessOrderJob < ApplicationJob
  limits_concurrency to: 1,
                     key: ->(order_id) { "ProcessOrder-#{order_id}" },
                     duration: 15.minutes,
                     on_conflict: :block

  def perform(order_id)
    # Only one job per order_id runs at a time
  end
end
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `to:` | (required) | Maximum concurrent jobs for the same key |
| `key:` | Job class name | Proc receiving job arguments, returns a string key |
| `duration:` | `15.minutes` | Safety expiry for the semaphore (crashed worker recovery) |
| `on_conflict:` | `:block` | What to do when the limit is reached |

### Conflict strategies

| Strategy | Behavior |
|----------|----------|
| `:block` | Hold the job in a blocked queue. It is automatically released when a slot opens or the semaphore expires. |
| `:discard` | Silently drop the job. |
| `:raise` | Raise `Pgbus::ConcurrencyLimitExceeded` so the caller can handle it. |

### How it works

1. **Enqueue**: The adapter checks a semaphore table for the concurrency key. If under the limit, it increments the counter and sends the job to PGMQ. If at the limit, it applies the `on_conflict` strategy.
2. **Complete**: After a job succeeds or is dead-lettered, the executor signals the concurrency system via an `ensure` block (guaranteeing the signal fires even if the archive step fails). It first tries to promote a blocked job (atomic delete + enqueue in a single transaction). If nothing to promote, it releases the semaphore slot.
3. **Safety net**: The dispatcher periodically cleans up expired semaphores and orphaned blocked executions to recover from crashed workers.

### Concurrency compared to other backends

Pgbus, SolidQueue, GoodJob, and Sidekiq all offer concurrency controls, but with fundamentally different locking strategies and trade-offs.

#### Architecture comparison

| | **Pgbus** | **SolidQueue** | **GoodJob** | **Sidekiq Enterprise** |
|---|---|---|---|---|
| **Lock backend** | PostgreSQL rows (`pgbus_semaphores` table) | PostgreSQL rows (`solid_queue_semaphores`) | PostgreSQL advisory locks (`pg_advisory_xact_lock`) | Redis sorted sets (lease-based) |
| **Lock granularity** | Counting semaphore (allows N concurrent) | Counting semaphore (allows N concurrent) | Count query under advisory lock | Sorted set entries with TTL |
| **Acquire mechanism** | Atomic `INSERT ... ON CONFLICT DO UPDATE WHERE value < max` (single SQL) | `UPDATE ... SET value = value + 1 WHERE value < limit` | `pg_advisory_xact_lock` then `SELECT COUNT(*)` in rolled-back txn | Redis Lua script (atomic check-and-add) |
| **At-limit behavior** | `:block` (hold in queue), `:discard`, or `:raise` | Blocks in `solid_queue_blocked_executions` | Enqueue: silently dropped. Perform: retry with backoff (forever) | Reschedule with backoff (raises `OverLimit`, middleware re-enqueues) |
| **Blocked job storage** | `pgbus_blocked_executions` table with priority ordering | `solid_queue_blocked_executions` table | No blocked queue — retries via ActiveJob retry mechanism | No blocked queue — job returns to Redis queue with delay |
| **Release on completion** | `ensure` block: promote next blocked job or decrement semaphore | Inline after `finished`/`failed_with` (inside same transaction as of PR #689) | Release advisory lock via `pg_advisory_unlock` | Lease auto-expires from sorted set |
| **Crash recovery** | Semaphore `expires_at` + dispatcher `expire_stale` cleanup | Semaphore `expires_at` + concurrency maintenance task | Advisory locks auto-release on session disconnect | TTL-based lease expiry (default 5 min) |
| **Message lifecycle** | PGMQ visibility timeout (`FOR UPDATE SKIP LOCKED`) — message stays in queue until archived | AR-backed `claimed_executions` table | AR-backed `good_jobs` table with advisory lock per row | Redis list + sorted set |

#### Key design differences

**Pgbus** uses PGMQ's native `FOR UPDATE SKIP LOCKED` for message claiming and a separate semaphore table for concurrency control. This two-layer approach means the message queue and concurrency system are independent — PGMQ handles exactly-once delivery, the semaphore handles admission control. The semaphore acquire is a single atomic SQL (`INSERT ... ON CONFLICT DO UPDATE WHERE value < max`), avoiding the need for explicit row locks.

**SolidQueue** uses AR models for everything — jobs, claimed executions, and semaphores all live in PostgreSQL tables. This means the entire lifecycle can be wrapped in AR transactions. However, as documented in [rails/solid_queue#689](https://github.com/rails/solid_queue/pull/689), this model is vulnerable to race conditions when semaphore expiry, job completion, and blocked-job release interleave across transactions. Pgbus avoids several of these by design: PGMQ's visibility timeout handles message recovery without a `claimed_executions` table, and there is no "release during shutdown" codepath.

**GoodJob** takes a different approach entirely: advisory locks. Each job dequeue acquires a session-level advisory lock on the job row, and concurrency checks use transaction-scoped advisory locks on the concurrency key. This means the check and the perform are serialized at the database level. The downside is that advisory locks are session-scoped — if a connection is returned to the pool without unlocking, the lock persists. GoodJob handles this by auto-releasing on session disconnect, but connection pool sharing between web and worker can cause surprising behavior.

**Sidekiq Enterprise** uses Redis sorted sets with TTL-based leases. Each concurrent slot is a sorted set entry with an expiry timestamp. This is fast and simple but has no durability guarantee — Redis failover can lose leases, temporarily allowing over-limit execution. The `sidekiq-unique-jobs` gem (open-source) uses a similar Lua-script approach but with more lock strategies (`:until_executing`, `:while_executing`, `:until_and_while_executing`) and configurable conflict handlers (`:reject`, `:reschedule`, `:replace`, `:raise`).

#### Race condition resilience

| Scenario | Pgbus | SolidQueue | GoodJob | Sidekiq |
|---|---|---|---|---|
| **Worker crash mid-execution** | PGMQ visibility timeout expires → message re-read. Semaphore expires via `expire_stale`. | `claimed_execution` survives → supervisor's process pruning calls `fail_all_with`. | Advisory lock released on session disconnect. | Lease TTL expires in Redis. |
| **Blocked job released while original still executing** | Not possible — promote only happens in `signal_concurrency`, which only runs after job success/DLQ. | Fixed in PR #689 — now checks for claimed executions before releasing. | N/A — no blocked queue; retries independently. | N/A — no blocked queue. |
| **Archive succeeds but signal fails** | `ensure` block guarantees signal fires even if archive raises. For SIGKILL: semaphore expires via dispatcher. | Fixed in PR #689 — `unblock_next_job` moved inside same transaction as `finished`. | Advisory lock released by session disconnect. | Lease auto-expires. |
| **Concurrent enqueue and signal race** | Semaphore acquire is a single atomic SQL — no read-then-write gap. | Fixed in PR #689 — `FOR UPDATE` lock on semaphore row serializes enqueue with signal. | `pg_advisory_xact_lock` serializes the concurrency check. | Redis Lua script is atomic. |

## Batches

Coordinate groups of jobs with callbacks when all complete:

```ruby
batch = Pgbus::Batch.new(
  on_finish: BatchFinishedJob,
  on_success: BatchSucceededJob,
  on_discard: BatchFailedJob,
  description: "Import users",
  properties: { initiated_by: current_user.id }
)

batch.enqueue do
  users.each { |user| ImportUserJob.perform_later(user.id) }
end
```

### Callbacks

| Callback | Fired when |
|----------|------------|
| `on_finish` | All jobs completed (success or discard) |
| `on_success` | All jobs completed successfully (zero discarded) |
| `on_discard` | At least one job was dead-lettered |

Callback jobs receive the batch `properties` hash as their argument:

```ruby
class BatchFinishedJob < ApplicationJob
  def perform(properties)
    user = User.find(properties["initiated_by"])
    ImportMailer.complete(user).deliver_later
  end
end
```

### How it works

1. `Batch.new(...)` creates a tracking row in `pgbus_batches` with `status: "pending"`
2. `batch.enqueue { ... }` tags each enqueued job with the `pgbus_batch_id` in its payload
3. After each job completes or is dead-lettered, the executor atomically updates the batch counters
4. When `completed_jobs + discarded_jobs == total_jobs`, the batch status flips to `"finished"` and callback jobs are enqueued
5. The dispatcher cleans up finished batches older than 7 days

## Job uniqueness

Prevent duplicate jobs from running. Unlike `limits_concurrency` (which controls *how many* jobs with the same key run), uniqueness guarantees *at most one* job with a given key exists in the system at any time.

```ruby
class ImportOrderJob < ApplicationJob
  ensures_uniqueness strategy: :until_executed,
                     key: ->(order_id) { "import-order-#{order_id}" },
                     on_conflict: :reject

  def perform(order_id)
    # Only ONE instance per order_id can exist — from enqueue through completion.
    # If another ImportOrderJob for this order_id is already enqueued or running,
    # the duplicate is rejected immediately.
  end
end
```

### Strategies

| Strategy | Lock acquired | Lock released | Prevents |
|----------|--------------|---------------|----------|
| `:until_executed` | At enqueue | On completion or DLQ | Duplicate enqueue AND execution |
| `:while_executing` | At execution start | On completion or DLQ | Duplicate execution only |

### Conflict policies

| Policy | Behavior |
|--------|----------|
| `:reject` | Raise `Pgbus::JobNotUnique` (default) |
| `:discard` | Silently drop the duplicate |
| `:log` | Log a warning and drop |

### Lock lifecycle

The lock is **never released by a timer**. It is held as long as the job exists in the system:

```text
Enqueue ──→ pgbus_job_locks (state: queued, owner_pid: nil)
                  │
  Worker picks up job
                  │
                  ▼
           claim_for_execution! (state: executing, owner_pid: PID)
                  │
          ┌───────┴───────┐
          ▼               ▼
      Success           Crash
      release!        (lock orphaned)
      (row deleted)       │
                          ▼
                    Reaper checks:
                    Is owner_pid in pgbus_processes
                    with fresh heartbeat?
                          │
                    ┌─────┴─────┐
                    No          Yes
                    ▼            ▼
                release!      (keep lock,
                (orphaned)     job is running)
```

**Crash recovery** works through the reaper (runs every 5 minutes in the dispatcher). It cross-references `owner_pid` in `pgbus_job_locks` against `pgbus_processes` heartbeats. If the owning worker has no fresh heartbeat, the lock is orphaned and released — the PGMQ message's visibility timeout will expire and the job will be retried by another worker.

A last-resort TTL (default 24 hours) handles the case where the entire pgbus supervisor is dead and the reaper itself can't run.

### Uniqueness vs concurrency controls

| | `ensures_uniqueness` | `limits_concurrency` |
|---|---|---|
| **Purpose** | Prevent duplicate jobs | Limit concurrent execution slots |
| **Lock type** | Binary lock (one or none) | Counting semaphore (up to N) |
| **At enqueue** | `:until_executed` blocks duplicates | Checks semaphore, blocks/discards/raises |
| **At execution** | `:while_executing` blocks duplicate runs | Not checked (semaphore acquired at enqueue) |
| **Duplicate in queue** | `:until_executed`: impossible. `:while_executing`: allowed, only one runs | Allowed up to N, rest blocked |
| **Crash recovery** | Reaper checks heartbeats | Semaphore `expires_at` + dispatcher cleanup |
| **Use when** | "This exact job must not run twice" | "At most N of these can run at once" |

**When to use which:**
- Payment processing, order import, unique email sends → `ensures_uniqueness`
- Rate-limited API calls, resource-constrained tasks → `limits_concurrency`
- Both at once → combine them (they use separate tables, no conflicts)

### Setup

```bash
rails generate pgbus:add_job_locks                  # Add the migration
rails generate pgbus:add_job_locks --database=pgbus # For separate database
```

## Priority queues

Route jobs to priority sub-queues so high-priority work is processed first:

```ruby
Pgbus.configure do |config|
  config.priority_levels = 3    # Creates _p0, _p1, _p2 sub-queues per logical queue
  config.default_priority = 1   # Jobs without explicit priority go to _p1
end
```

Workers read from `_p0` (highest) first, then `_p1`, then `_p2`. Only when higher-priority sub-queues are empty does the worker read from lower ones.

Use ActiveJob's built-in `priority` attribute:

```ruby
class CriticalAlertJob < ApplicationJob
  queue_as :default
  queue_with_priority 0  # Highest priority

  def perform(alert_id)
    # ...
  end
end

class ReportJob < ApplicationJob
  queue_as :default
  queue_with_priority 2  # Lowest priority

  def perform(report_id)
    # ...
  end
end
```

When `priority_levels` is `nil` (default), priority queues are disabled and all jobs go to a single queue per logical name.

## Single active consumer

For queues that require strict ordering, enable single active consumer mode. Only one worker process can read from a queue at a time -- others skip it and process other queues.

```yaml
# config/pgbus.yml
production:
  workers:
    - queues: [ordered_events]
      threads: 1
      single_active_consumer: true
    - queues: [ordered_events]
      threads: 1
      single_active_consumer: true  # Standby — takes over if the first worker dies
```

Uses PostgreSQL session-level advisory locks (`pg_try_advisory_lock`). The lock is non-blocking -- workers that can't acquire it simply skip the queue. Locks auto-release on connection close (including crashes), so failover is automatic.

## Consumer priority

When multiple workers subscribe to the same queues, higher-priority workers process messages first. Lower-priority workers back off (3x polling interval) when a higher-priority worker is active.

```yaml
# config/pgbus.yml
production:
  workers:
    - queues: [default]
      threads: 10
      consumer_priority: 10     # Primary — polls at base interval
    - queues: [default]
      threads: 5
      consumer_priority: 0      # Fallback — polls at 3x interval when primary is healthy
```

Priority is stored in heartbeat metadata. Workers check the `pgbus_processes` table to discover higher-priority peers. When a high-priority worker goes stale (no heartbeat for 5 minutes), lower-priority workers automatically resume normal polling.

## Circuit breaker and queue pause/resume

Pgbus automatically pauses queues that fail repeatedly, preventing cascading failures.

```ruby
Pgbus.configure do |config|
  config.circuit_breaker_enabled = true   # default
  config.circuit_breaker_threshold = 5    # consecutive failures before tripping
  config.circuit_breaker_base_backoff = 30  # seconds (doubles per trip)
  config.circuit_breaker_max_backoff = 600  # 10 minute cap
end
```

When a queue hits the failure threshold:
1. The circuit breaker **auto-pauses** the queue with exponential backoff
2. After the backoff expires, the queue **auto-resumes** and the trip counter resets
3. If failures continue, each trip doubles the backoff (capped at `max_backoff`)

You can also **manually pause/resume** queues from the dashboard. The pause state is stored in the `pgbus_queue_states` table and survives restarts.

```bash
rails generate pgbus:add_queue_states           # Add the queue_states migration
rails generate pgbus:add_queue_states --database=pgbus  # For separate database
```

## Prefetch flow control

Cap the number of in-flight (claimed but unfinished) messages per worker:

```ruby
Pgbus.configure do |config|
  config.prefetch_limit = 20  # nil = unlimited (default)
end
```

The worker tracks in-flight messages with an atomic counter and only fetches `min(idle_threads, prefetch_available)` messages per cycle. The counter is decremented in an `ensure` block so it never gets stuck.

## Transactional outbox

Publish events atomically inside your database transactions. A background poller moves outbox entries to PGMQ.

```bash
rails generate pgbus:add_outbox                  # Add the outbox migration
rails generate pgbus:add_outbox --database=pgbus # For separate database
```

```ruby
Pgbus.configure do |config|
  config.outbox_enabled = true
  config.outbox_poll_interval = 1.0  # seconds
  config.outbox_batch_size = 100
  config.outbox_retention = 24 * 3600  # keep published entries for 24h
end
```

Usage:

```ruby
ActiveRecord::Base.transaction do
  order = Order.create!(params)

  # Published atomically with the order — if the transaction rolls back,
  # the outbox entry is also rolled back. No lost or phantom events.
  Pgbus::Outbox.publish("default", { order_id: order.id })

  # For topic-based event bus:
  Pgbus::Outbox.publish_event("orders.created", { order_id: order.id })
end
```

The outbox poller uses `FOR UPDATE SKIP LOCKED` inside a transaction to claim entries, publishes them to PGMQ, and marks them as published. Failed entries are skipped and retried next cycle.

## Archive compaction

PGMQ archive tables grow unbounded. Pgbus automatically purges old entries:

```ruby
Pgbus.configure do |config|
  config.archive_retention = 7 * 24 * 3600       # 7 days (default)
  config.archive_compaction_interval = 3600       # run every hour (default)
  config.archive_compaction_batch_size = 1000     # delete in batches (default)
end
```

The dispatcher runs archive compaction as part of its maintenance loop, deleting archived messages older than `archive_retention` in batches to avoid long-running transactions.

## Configuration reference

| Option | Default | Description |
|--------|---------|-------------|
| `database_url` | `nil` | PostgreSQL connection URL (auto-detected in Rails) |
| `queue_prefix` | `"pgbus"` | Prefix for all PGMQ queue names |
| `default_queue` | `"default"` | Default queue for jobs without explicit queue |
| `pool_size` | `5` | Connection pool size |
| `workers` | `[{queues: ["default"], threads: 5}]` | Worker process definitions |
| `event_consumers` | `nil` | Event consumer process definitions (same format as workers) |
| `polling_interval` | `0.1` | Seconds between polls (LISTEN/NOTIFY is primary) |
| `visibility_timeout` | `30` | Seconds before unacked message becomes visible again |
| `max_retries` | `5` | Failed reads before routing to dead letter queue |
| `max_jobs_per_worker` | `nil` | Recycle worker after N jobs (nil = unlimited) |
| `max_memory_mb` | `nil` | Recycle worker when memory exceeds N MB |
| `max_worker_lifetime` | `nil` | Recycle worker after N seconds |
| `listen_notify` | `true` | Use PGMQ's LISTEN/NOTIFY for instant wake-up |
| `prefetch_limit` | `nil` | Max in-flight messages per worker (nil = unlimited) |
| `dispatch_interval` | `1.0` | Seconds between dispatcher maintenance ticks |
| `circuit_breaker_enabled` | `true` | Enable auto-pause on consecutive failures |
| `circuit_breaker_threshold` | `5` | Consecutive failures before tripping |
| `circuit_breaker_base_backoff` | `30` | Base backoff seconds (doubles per trip) |
| `circuit_breaker_max_backoff` | `600` | Max backoff cap in seconds |
| `priority_levels` | `nil` | Number of priority sub-queues (nil = disabled, 2-10) |
| `default_priority` | `1` | Default priority for jobs without explicit priority |
| `archive_retention` | `604800` | Seconds to keep archived messages (7 days) |
| `archive_compaction_interval` | `3600` | Seconds between archive cleanup runs |
| `archive_compaction_batch_size` | `1000` | Rows deleted per batch during compaction |
| `outbox_enabled` | `false` | Enable transactional outbox poller process |
| `outbox_poll_interval` | `1.0` | Seconds between outbox poll cycles |
| `outbox_batch_size` | `100` | Max entries per outbox poll cycle |
| `outbox_retention` | `86400` | Seconds to keep published outbox entries (1 day) |
| `idempotency_ttl` | `604800` | Seconds to keep processed event records (7 days, cleaned hourly) |
| `base_controller_class` | `"::ActionController::Base"` | Base class for dashboard controllers (string, constantized at load time) |
| `return_to_app_url` | `nil` | URL for "back to app" button in dashboard nav (nil hides the button) |
| `web_auth` | `nil` | Lambda for dashboard authentication |
| `web_refresh_interval` | `5000` | Dashboard auto-refresh interval in milliseconds |
| `web_live_updates` | `true` | Enable Turbo Frames auto-refresh on dashboard |
| `stats_enabled` | `true` | Record job execution stats for insights dashboard |
| `stats_retention` | `604800` | Seconds to keep job stats (7 days) |

## Architecture

```text
Supervisor (fork manager)
  ├── Worker 1        (queues: [default, mailers], threads: 10, priority: 10)
  ├── Worker 2        (queues: [critical], threads: 5, single_active_consumer: true)
  ├── Dispatcher      (maintenance: cleanup, compaction, reaping, circuit breaker)
  ├── Scheduler       (recurring tasks via cron)
  ├── Consumer        (event bus topics)
  └── Outbox Poller   (transactional outbox → PGMQ, when enabled)

PostgreSQL + PGMQ
  ├── pgbus_default          (job queue)
  ├── pgbus_default_dlq      (dead letter queue)
  ├── pgbus_critical         (job queue)
  ├── pgbus_critical_dlq     (dead letter queue)
  ├── pgbus_mailers          (job queue)
  └── pgbus_queue_states     (pause/resume + circuit breaker state)
```

### How it works

1. **Enqueue**: ActiveJob serializes the job to JSON, Pgbus sends it to the appropriate PGMQ queue
2. **Read**: Workers poll queues (or wake instantly via LISTEN/NOTIFY) and claim messages with a visibility timeout
3. **Execute**: The job is deserialized and executed within the Rails executor
4. **Archive/Retry**: On success, the message is archived. On failure, the visibility timeout expires and the message becomes available again. PGMQ's `read_ct` tracks delivery attempts
5. **Dead letter**: When `read_ct` exceeds `max_retries`, the message is moved to the `_dlq` queue for manual inspection

### Worker recycling

Unlike solid_queue, Pgbus workers recycle themselves to prevent memory bloat:

```ruby
Pgbus.configure do |config|
  config.max_jobs_per_worker = 10_000  # Restart after 10k jobs
  config.max_memory_mb = 512           # Restart if memory exceeds 512MB
  config.max_worker_lifetime = 3600    # Restart after 1 hour
end
```

When a limit is hit, the worker drains its thread pool, exits, and the supervisor forks a fresh process.

## CLI

```bash
pgbus start     # Start supervisor with workers + dispatcher
pgbus status    # Show running processes
pgbus queues    # List queues with depth/metrics
pgbus version   # Print version
pgbus help      # Show help
```

## Dashboard

The dashboard is a mountable Rails engine at `/pgbus` with:

- **Overview** -- queue depths, enqueued count, active processes, failure count, throughput rate
- **Queues** -- per-queue metrics, purge/pause/resume/delete actions
- **Jobs** -- enqueued and failed jobs, retry/discard actions
- **Dead letter** -- DLQ messages with retry/discard, bulk actions
- **Processes** -- active workers/dispatcher/consumers with heartbeat status
- **Events** -- registered subscribers and processed events
- **Outbox** -- transactional outbox entries pending publication
- **Locks** -- active job uniqueness locks with state (queued/executing), owner PID@hostname, age
- **Insights** -- throughput chart (jobs/min), status distribution donut, slowest job classes table

All tables use Turbo Frames for periodic auto-refresh without page reloads. Destructive actions use styled confirmation dialogs (not browser `confirm()`), and flash messages appear as auto-dismissing toast notifications.

### Queue management

The queues page lets you manage PGMQ queues directly:

- **Purge** -- removes all messages from the queue (the queue itself remains)
- **Delete** -- permanently drops the queue from PGMQ (removes the queue table and metadata)
- **Pause / Resume** -- pauses or resumes job processing for a queue

All destructive actions require confirmation. Pause/resume and delete are available on both the queue index and detail pages.

### Dark mode

The dashboard supports dark mode via Tailwind CSS `dark:` classes. It respects your system preference on first visit and persists your choice via localStorage. Toggle with the sun/moon button in the nav bar.

### Job stats and insights

The executor records every job completion to `pgbus_job_stats` (job class, queue, status, duration). The insights page visualizes this data with ApexCharts (loaded via CDN, zero npm dependencies).

```bash
rails generate pgbus:add_job_stats           # Add the stats migration
rails generate pgbus:add_job_stats --database=pgbus
```

Stats collection is enabled by default (`config.stats_enabled = true`). Old stats are cleaned up by the dispatcher based on `config.stats_retention` (default: 7 days). If the migration hasn't been run yet, stat recording is silently skipped.

## Real-time broadcasts (turbo-streams replacement)

Pgbus ships a drop-in replacement for turbo-rails' `turbo_stream_from` helper that fixes several well-known ActionCable correctness bugs by using PGMQ message IDs as a replay cursor. Same API as turbo-rails. No Redis. No ActionCable. No lost messages on reconnect.

**Bugs fixed:**

- [**rails/rails#52420**](https://github.com/rails/rails/issues/52420) -- "page born stale": a broadcast that fires between controller render and WebSocket subscribe is silently lost with ActionCable. Pgbus captures a PGMQ `msg_id` watermark at render time and replays any messages published in the gap via the SSE `Last-Event-ID` mechanism.
- [**hotwired/turbo#1261**](https://github.com/hotwired/turbo/issues/1261) -- missed messages on reconnect. Pgbus persists the cursor on the client (EventSource's built-in `Last-Event-ID`) and replays from the PGMQ archive on every reconnect.
- [**hotwired/turbo-rails#674**](https://github.com/hotwired/turbo-rails/issues/674) -- no way to detect disconnect. Pgbus dispatches `pgbus:open`, `pgbus:gap-detected`, and `pgbus:close` DOM events on the stream element.

### Usage

Swap `turbo_stream_from` for `pgbus_stream_from` in your view:

```erb
<%# Before %>
<%= turbo_stream_from @order %>

<%# After %>
<%= pgbus_stream_from @order %>
```

Everything else stays the same. The model concern keeps working unchanged:

```ruby
class Order < ApplicationRecord
  broadcasts_to ->(order) { [order.account, :orders] }
end
```

`broadcasts_to`, `broadcast_replace_to`, `broadcasts_refreshes`, `broadcast_append_later_to`, and every other `Turbo::Broadcastable` helper funnels through a single `Turbo::StreamsChannel.broadcast_stream_to` method that pgbus monkey-patches at engine boot. The signed-stream-name verification reuses `Turbo.signed_stream_verifier_key` so existing signed tokens Just Work.

Add the Puma plugin to `config/puma.rb` so SSE connections drain cleanly on deploy:

```ruby
# config/puma.rb
plugin :pgbus_streams
```

Without the plugin, Puma closes hijacked SSE sockets abruptly during graceful restart, which looks to browsers like a network error and triggers an immediate reconnect. With the plugin, the streamer writes a `pgbus:shutdown` sentinel before the socket closes; browsers reconnect to the new worker and replay missed messages via `Last-Event-ID`.

### Requirements

- **Puma 6.1 or newer.** Streams use `rack.hijack` + partial hijack (both supported in 6.1+). Unicorn, Pitchfork, and Passenger return HTTP 501 from the streams endpoint.
- **PostgreSQL LISTEN/NOTIFY.** `config.listen_notify = true` (the default). Stream queues override PGMQ's 250ms NOTIFY throttle to 0 so every broadcast fires individually.
- **HTTP/2 or HTTP/3 in production.** SSE has a 6-connection-per-origin limit on HTTP/1.1; HTTP/2 lifts it.

### Configuration

```ruby
Pgbus.configure do |c|
  c.streams_enabled                = true          # default
  c.streams_queue_prefix           = "pgbus_stream"
  c.streams_default_retention      = 5 * 60        # 5 minutes
  c.streams_retention              = {             # per-stream overrides
    /^chat_/        => 7 * 24 * 3600,              # 7 days for chat history
    "presence_room" => 30                          # 30 seconds for presence
  }
  c.streams_heartbeat_interval     = 15            # seconds
  c.streams_max_connections        = 2_000         # per Puma worker
  c.streams_idle_timeout           = 3_600         # close idle connections after 1h
  c.streams_listen_health_check_ms = 5_000         # PG LISTEN keepalive
  c.streams_write_deadline_ms      = 5_000         # write_nonblock deadline
end
```

### How it works

Stream broadcasts are stored in PGMQ queues prefixed `pgbus_stream_*`. Each broadcast is assigned a monotonic `msg_id` by PGMQ. The `pgbus_stream_from` helper captures the current `MAX(msg_id)` at render time and embeds it in the HTML as `since-id`. When the SSE client connects, it sends that cursor as `?since=` on the first request and as `Last-Event-ID` on reconnects. The streamer replays from `pgmq.q_*` (live) UNION `pgmq.a_*` (archive) for any `msg_id > cursor`, then switches to LISTEN/NOTIFY for the live path. There is no message identity gap between the render and the subscribe — the cursor model guarantees every broadcast is delivered exactly once, in order, even across reconnects.

One Puma worker hosts one `Pgbus::Web::Streamer::Instance` singleton with three threads (Listener / Dispatcher / Heartbeat) and one dedicated PG connection for LISTEN. Hijacked SSE sockets are held outside Puma's thread pool -- confirmed by an integration test that fires 20 concurrent hijacked connections and observes them complete in parallel on an 8-thread Puma server ([puma/puma#1009](https://github.com/puma/puma/issues/1009)).

Per-stream retention is handled by the main pgbus dispatcher process on the same interval as `archive_compaction_interval`. Streams default to a 5-minute retention because SSE clients reconnect within seconds; chat-style applications override the retention to days via `streams_retention`.

### Transactional broadcasts

**This is the feature no other Rails real-time stack can offer.** A broadcast issued inside an open ActiveRecord transaction is deferred until the transaction commits. If it rolls back, the broadcast silently drops — clients never see the change the database never persisted.

```ruby
ActiveRecord::Base.transaction do
  @order.update!(status: "shipped")
  @order.broadcast_replace_to :account           # ← deferred until commit
  RelatedService.update_counters!(@order)        # ← might raise, rolling back the update
end
# If RelatedService raised, the database state is unchanged AND no SSE client
# ever saw a "shipped" broadcast. The broadcast and the data mutation are
# atomic with respect to each other.
```

ActionCable can't do this because its broadcast path goes through Redis pub/sub, which has no concept of your application's transaction boundary. Pgbus detects the open AR transaction via `ActiveRecord::Base.connection.current_transaction.after_commit`, which is a first-class Rails API — no outbox table, no background worker, no extra storage.

Outside an open transaction, broadcasts are synchronous and return the assigned `msg_id` as before. Inside a transaction, they return `nil` (the id isn't known until commit time).

### What's NOT included (yet)

- `broadcasts_with_replay`: chat-history-as-a-stream where new subscribers replay from the beginning of retention rather than just the render watermark.
- Server-side audience filtering (per-connection authorization that filters individual broadcasts).
- Presence as a first-class primitive.
- Falcon streaming-body code path. Puma is the only supported server in v1. A small `stream_app.rb` conditional will add Falcon in a follow-up.

## Database tables

Pgbus uses these tables (created via PGMQ and migrations):

| Table | Purpose |
|-------|---------|
| `q_pgbus_*` | PGMQ job queues (managed by PGMQ) |
| `a_pgbus_*` | PGMQ archive tables (managed by PGMQ, compacted by dispatcher) |
| `pgbus_processes` | Heartbeat tracking for workers/dispatcher/consumers |
| `pgbus_failed_events` | Failed event dispatch records |
| `pgbus_processed_events` | Idempotency deduplication (event_id, handler_class) |
| `pgbus_semaphores` | Concurrency control counting semaphores |
| `pgbus_blocked_executions` | Jobs waiting for a concurrency semaphore slot |
| `pgbus_batches` | Batch tracking with job counters and callback config |
| `pgbus_job_locks` | Job uniqueness locks (state, owner_pid, reaper correlation) |
| `pgbus_job_stats` | Job execution metrics (class, queue, status, duration) |
| `pgbus_queue_states` | Queue pause/resume and circuit breaker state |
| `pgbus_outbox_entries` | Transactional outbox entries pending publication |
| `pgbus_recurring_tasks` | Recurring job definitions |
| `pgbus_recurring_executions` | Recurring job execution history |

## Switching from another backend

Already using a different job processor? These guides walk you through the migration:

- **[Switch from Sidekiq](docs/switch_from_sidekiq.md)** -- remove Redis, convert native workers, replace middleware with callbacks
- **[Switch from SolidQueue](docs/switch_from_solid_queue.md)** -- similar architecture, swap config format, gain LISTEN/NOTIFY + worker recycling
- **[Switch from GoodJob](docs/switch_from_good_job.md)** -- both PostgreSQL-native, swap advisory locks for PGMQ visibility timeouts

See [docs/README.md](docs/README.md) for a full feature comparison table.

## Development

```bash
bundle install
bundle exec rake          # Run tests + rubocop
bundle exec rspec         # Run tests only
bundle exec rubocop       # Run linter only
```

System tests use Playwright via Capybara:

```bash
bun install
bunx --bun playwright install chromium
bundle exec rspec spec/system/
```

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
