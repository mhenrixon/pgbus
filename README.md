# Pgbus

PostgreSQL-native job processing and event bus for Rails, built on [PGMQ](https://github.com/tembo-io/pgmq).

**Why Pgbus?** If you already run PostgreSQL, you don't need Redis for background jobs. Pgbus gives you ActiveJob integration, AMQP-style topic routing, dead letter queues, worker memory management, and a live dashboard -- all backed by your existing database.

[![Ruby](https://github.com/mhenrixon/pgbus/actions/workflows/main.yml/badge.svg)](https://github.com/mhenrixon/pgbus/actions/workflows/main.yml)

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
