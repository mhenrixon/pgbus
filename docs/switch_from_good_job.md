# Switch from GoodJob to Pgbus

## Overview

GoodJob and Pgbus are both PostgreSQL-native job processors with LISTEN/NOTIFY support. The biggest architectural difference: GoodJob uses advisory locks and a `good_jobs` table, while Pgbus uses PGMQ (a dedicated message queue extension) with visibility timeouts. Both are pure ActiveJob adapters.

**Effort estimate:** Low if you use standard ActiveJob. Medium if you rely on GoodJob's concurrency controls, batches, or cron.

## Step 1: Update dependencies

```ruby
# Gemfile

# Remove
gem "good_job"

# Add
gem "pgbus"
```

```bash
bundle install
rails generate pgbus:install
rails db:migrate
```

## Step 2: Switch the adapter

```ruby
# config/application.rb

# Before
config.active_job.queue_adapter = :good_job

# After
config.active_job.queue_adapter = :pgbus
```

Remove GoodJob configuration:

```ruby
# Before: Remove this block
config.good_job = {
  execution_mode: :async,
  queues: "critical:5;default:3;*:1",
  max_threads: 5,
  poll_interval: 10,
  enable_cron: true,
  enable_listen_notify: true,
  # ...
}
```

## Step 3: Convert worker configuration

```ruby
# Before: GoodJob config (in application.rb or environment files)
config.good_job = {
  execution_mode: :external,  # or :async
  queues: "critical:5;default:3;low:1",
  poll_interval: 10,
  max_threads: 5,
  shutdown_timeout: 30,
}
```

```yaml
# After: config/pgbus.yml
production:
  workers:
    - queues: [critical]
      threads: 5
    - queues: [default]
      threads: 3
    - queues: [low]
      threads: 1
  max_retries: 5
  max_jobs_per_worker: 10000
  max_memory_mb: 512
  max_worker_lifetime: 3600
```

### Configuration mapping

| GoodJob | Pgbus | Notes |
|---------|-------|-------|
| `execution_mode: :external` | `bundle exec pgbus start` | Separate process (recommended for production) |
| `execution_mode: :async` | N/A | Pgbus always runs as separate processes |
| `queues: "critical:5;default:3"` | `workers:` array | One entry per worker process |
| `max_threads` | `threads` per worker | Per-worker, not global |
| `poll_interval` | `polling_interval` | Pgbus defaults to 0.1s; LISTEN/NOTIFY is primary |
| `enable_listen_notify` | `listen_notify` | Both support LISTEN/NOTIFY |
| `shutdown_timeout` | Handled by supervisor | Graceful shutdown on SIGTERM |
| `on_thread_error` | Configure via `Pgbus.logger` | Error reporting via logging |
| `preserve_job_records` | Always archived | PGMQ archives completed messages |

## Step 4: Remove concurrency controls

GoodJob's concurrency extensions are GoodJob-specific. Remove them:

```ruby
# Before
class ProcessOrderJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency

  good_job_control_concurrency_with(
    total_limit: 1,
    enqueue_limit: 2,
    perform_limit: 1,
    enqueue_throttle: [10, 1.minute],
    perform_throttle: [100, 1.hour],
    key: -> { "ProcessOrder-#{arguments.first.id}" }
  )

  def perform(order)
    # ...
  end
end
```

```ruby
# After: Pgbus equivalent (auto-included, no explicit include needed)
class ProcessOrderJob < ApplicationJob
  limits_concurrency to: 1,
                     key: -> { "ProcessOrder-#{arguments.first.id}" },
                     duration: 15.minutes,
                     on_conflict: :block

  def perform(order)
    # ...
  end
end
```

Pgbus's `limits_concurrency` is auto-included into `ActiveJob::Base` by the engine. Note that GoodJob's `total_limit`, `enqueue_limit`, `perform_limit`, and throttle options don't have direct equivalents -- Pgbus uses a single `to:` limit with conflict strategies (`:block`, `:discard`, `:raise`).

## Step 5: Migrate batches

If you use `GoodJob::Batch`:

```ruby
# Before: GoodJob batches
GoodJob::Batch.enqueue(
  on_finish: BatchCallbackJob,
  on_success: SuccessNotifyJob,
  description: "Import users"
) do
  users.each { |u| ImportUserJob.perform_later(u) }
end
```

Pgbus has built-in batch support with callbacks:

```ruby
# After: Pgbus batch
batch = Pgbus::Batch.new(
  on_finish: BatchCallbackJob,
  on_success: SuccessNotifyJob,
  description: "Import users"
)

batch.enqueue do
  users.each { |u| ImportUserJob.perform_later(u) }
end
```

Pgbus additionally supports `on_discard` callbacks (fired when any job is dead-lettered) and batch `properties` for passing context to callbacks.

## Step 6: Migrate cron / recurring jobs

```ruby
# Before: GoodJob cron
config.good_job.enable_cron = true
config.good_job.cron = {
  daily_cleanup: {
    cron: "0 2 * * *",
    class: "CleanupJob",
    set: { priority: -10, queue: "maintenance" },
  },
  hourly_sync: {
    cron: "0 * * * *",
    class: "SyncJob",
    args: [42],
  }
}
```

Pgbus supports recurring tasks via `config/recurring.yml` with cron and human-readable syntax:

```yaml
# After: config/recurring.yml
production:
  daily_cleanup:
    class: CleanupJob
    schedule: "0 2 * * *"
    queue: maintenance
  hourly_sync:
    class: SyncJob
    schedule: "0 * * * *"
    args: [42]
```

Pgbus parses both standard cron (`0 2 * * *`) and human-readable (`every day at 2am`) syntax via Fugit. The scheduler runs as part of the supervisor process and deduplicates executions via a `pgbus_recurring_executions` table.

## Step 7: Replace the dashboard

```ruby
# Before: config/routes.rb
mount GoodJob::Engine => "good_job"

# After:
mount Pgbus::Engine => "/pgbus"
```

If you mount the dashboard inside an authenticated namespace:

```ruby
Pgbus.configure do |config|
  config.base_controller_class = "Admin::BaseController"
  config.return_to_app_url = "/admin"  # optional: adds a back button in the dashboard nav
end
```

## Step 8: Update process management

```bash
# Before (external mode)
bundle exec good_job start

# After
bundle exec pgbus start
```

If you ran GoodJob in `async` mode (in-process), note that Pgbus always runs as separate forked processes managed by a supervisor. Update your deployment accordingly -- you need `bundle exec pgbus start` as a separate process.

## Step 9: Clean up GoodJob tables

After verifying Pgbus is processing correctly and GoodJob's tables are drained:

```ruby
class RemoveGoodJob < ActiveRecord::Migration[7.1]
  def up
    drop_table :good_jobs, if_exists: true
    drop_table :good_job_batches, if_exists: true
    drop_table :good_job_executions, if_exists: true
    drop_table :good_job_processes, if_exists: true
    drop_table :good_job_settings, if_exists: true
  end
end
```

## What you gain

- **Dead letter queues** -- GoodJob retries in-place and marks jobs as discarded. Pgbus routes failures to dedicated `_dlq` queues for clear operational visibility.
- **Worker recycling** -- GoodJob workers run indefinitely. Pgbus recycles by job count, memory, or lifetime to prevent memory bloat.
- **Event bus** -- AMQP-style pub/sub with topic routing and idempotent handlers.
- **PGMQ** -- purpose-built message queue extension with atomic read/archive/delete, visibility timeouts, and `SKIP LOCKED` under the hood.
- **Supervisor/fork model** -- isolated worker processes. A memory leak or crash in one worker doesn't affect others.

## Feature comparison

| GoodJob feature | Pgbus equivalent |
|-----------------|-----------------|
| Concurrency controls (`good_job_control_concurrency_with`) | `limits_concurrency` DSL (auto-included, similar API) |
| Throttling (`enqueue_throttle`, `perform_throttle`) | Use `limits_concurrency` for most cases; sliding-window throttling not yet built |
| Batches (`GoodJob::Batch`) | `Pgbus::Batch` with `on_finish` / `on_success` / `on_discard` callbacks |
| Cron / recurring jobs | `config/recurring.yml` with cron + human-readable syntax via Fugit |
| Async execution mode (in-process) | Not planned (forked processes only) |
| Capsules (isolated thread pools) | Workers are isolated by design (forked processes) |
| Advisory locks | Replaced by PGMQ visibility timeouts |
| `ActiveJob::Continuation` (Rails 8.1+) | Supported -- `stopping?` wired to worker lifecycle |

## Gotchas

1. **PgBouncer**: Both GoodJob and Pgbus use LISTEN/NOTIFY, which requires session-mode PgBouncer or direct connections. If you already had GoodJob working with PgBouncer, the same configuration applies.

2. **No async mode**: GoodJob can run in-process (`:async` mode) alongside your Rails app server. Pgbus requires a separate supervisor process. Make sure your deployment runs `bundle exec pgbus start` alongside your web server.

3. **Advisory locks vs. visibility timeouts**: GoodJob uses PostgreSQL advisory locks to claim jobs. Pgbus uses PGMQ's visibility timeout -- a claimed message becomes invisible for `visibility_timeout` seconds. If a worker crashes without archiving the message, it automatically becomes available again after the timeout expires. This is more resilient than advisory locks, which release on disconnect.

4. **Job record preservation**: GoodJob has `preserve_job_records` for keeping completed job records. PGMQ automatically archives completed messages to `a_pgbus_*` tables, which serve a similar purpose for debugging and auditing.

5. **Queue naming**: GoodJob uses bare queue names. Pgbus prefixes all queues (`pgbus_default`). Your `queue_as` declarations work unchanged -- the prefix is applied automatically.
