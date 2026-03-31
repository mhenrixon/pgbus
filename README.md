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
- [Configuration reference](#configuration-reference)
- [Architecture](#architecture)
- [CLI](#cli)
- [Dashboard](#dashboard)
- [Database tables](#database-tables)
- [Switching from another backend](#switching-from-another-backend)
- [Development](#development)
- [License](#license)

## Features

- **ActiveJob adapter** -- drop-in replacement, zero config migration from other backends
- **Event bus** -- publish/subscribe with AMQP-style topic routing (`orders.#`, `payments.*`)
- **Dead letter queues** -- automatic DLQ routing after configurable retries
- **Worker recycling** -- memory, job count, and lifetime limits prevent runaway processes
- **LISTEN/NOTIFY** -- instant wake-up, polling as fallback only
- **Idempotent events** -- deduplication via `(event_id, handler_class)` unique index
- **Live dashboard** -- Turbo Frames auto-refresh, no ActionCable required
- **Supervisor/worker model** -- forked processes with heartbeat monitoring

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
  workers:
    - queues: [default, mailers]
      threads: 10
    - queues: [critical]
      threads: 5
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

Protect it in production:

```ruby
Pgbus.configure do |config|
  config.web_auth = ->(request) {
    request.env["warden"].user&.admin?
  }
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
| `dispatch_interval` | `1.0` | Seconds between dispatcher maintenance ticks |
| `idempotency_ttl` | `604800` | Seconds to keep processed event records (7 days, cleaned hourly) |
| `web_auth` | `nil` | Lambda for dashboard authentication |
| `web_refresh_interval` | `5000` | Dashboard auto-refresh interval in milliseconds |
| `web_live_updates` | `true` | Enable Turbo Frames auto-refresh on dashboard |

## Architecture

```text
Supervisor (fork manager)
  ├── Worker 1        (queues: [default, mailers], threads: 10)
  ├── Worker 2        (queues: [critical], threads: 5)
  ├── Dispatcher      (maintenance: idempotency cleanup, stale process reaping)
  └── Consumer        (event bus topics)

PostgreSQL + PGMQ
  ├── pgbus_default          (job queue)
  ├── pgbus_default_dlq      (dead letter queue)
  ├── pgbus_critical         (job queue)
  ├── pgbus_critical_dlq     (dead letter queue)
  └── pgbus_mailers          (job queue)
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

- **Overview** -- queue depths, enqueued count, active processes, failure count
- **Queues** -- per-queue metrics, purge actions
- **Jobs** -- enqueued and failed jobs, retry/discard actions
- **Dead letter** -- DLQ messages with retry/discard, bulk actions
- **Processes** -- active workers/dispatcher/consumers with heartbeat status
- **Events** -- registered subscribers and processed events

All tables use Turbo Frames for periodic auto-refresh without page reloads.

## Database tables

Pgbus uses these tables (created via PGMQ and migrations):

| Table | Purpose |
|-------|---------|
| `q_pgbus_*` | PGMQ job queues (managed by PGMQ) |
| `a_pgbus_*` | PGMQ archive tables (managed by PGMQ) |
| `pgbus_processes` | Heartbeat tracking for workers/dispatcher/consumers |
| `pgbus_failed_events` | Failed event dispatch records |
| `pgbus_processed_events` | Idempotency deduplication (event_id, handler_class) |
| `pgbus_semaphores` | Concurrency control counting semaphores |
| `pgbus_blocked_executions` | Jobs waiting for a concurrency semaphore slot |
| `pgbus_batches` | Batch tracking with job counters and callback config |

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
