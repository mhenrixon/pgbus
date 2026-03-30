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
# After
class ProcessOrderJob < ApplicationJob
  def perform(order)
    # ...
  end
end
```

> Pgbus supports concurrency controls via `Pgbus::Concurrency`:
> ```ruby
> class ProcessOrderJob < ApplicationJob
>   include Pgbus::Concurrency
>   limits_concurrency to: 1,
>                      key: -> { "ProcessOrder-#{arguments.first.id}" },
>                      duration: 15.minutes,
>                      on_conflict: :block
>   def perform(order)
>     # ...
>   end
> end
> ```

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

Pgbus does not yet have batch support. Workaround using a coordinator job:

```ruby
# After: Coordinator pattern
class ImportUsersJob < ApplicationJob
  def perform(user_ids)
    user_ids.each { |id| ImportUserJob.perform_later(id) }
    # Track completion via a counter in the database or Redis
  end
end
```

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

Pgbus does not yet have built-in recurring task support. Options:

1. **Use the `whenever` gem**:
   ```ruby
   # config/schedule.rb
   every 1.day, at: "2:00 am" do
     runner "CleanupJob.perform_later"
   end
   every :hour do
     runner "SyncJob.perform_later(42)"
   end
   ```

2. **Use system cron** directly:
   ```cron
   0 2 * * * cd /app && bin/rails runner "CleanupJob.perform_later"
   0 * * * * cd /app && bin/rails runner "SyncJob.perform_later(42)"
   ```

3. Wait for Pgbus recurring task support (planned).

## Step 7: Replace the dashboard

```ruby
# Before: config/routes.rb
mount GoodJob::Engine => "good_job"

# After:
mount Pgbus::Engine => "/pgbus"
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

## What you lose (for now)

| GoodJob feature | Status in Pgbus |
|-----------------|-----------------|
| Concurrency controls (`good_job_control_concurrency_with`) | `Pgbus::Concurrency` with `limits_concurrency` DSL |
| Throttling (`enqueue_throttle`, `perform_throttle`) | Planned |
| Batches (`GoodJob::Batch`) | `Pgbus::Batch` with on_finish/on_success/on_discard callbacks |
| Cron / recurring jobs | Planned |
| Async execution mode (in-process) | Not planned (forked processes only) |
| Capsules (isolated thread pools) | Workers are isolated by design (forked processes) |
| Advisory locks | Replaced by PGMQ visibility timeouts |

## Gotchas

1. **PgBouncer**: Both GoodJob and Pgbus use LISTEN/NOTIFY, which requires session-mode PgBouncer or direct connections. If you already had GoodJob working with PgBouncer, the same configuration applies.

2. **No async mode**: GoodJob can run in-process (`:async` mode) alongside your Rails app server. Pgbus requires a separate supervisor process. Make sure your deployment runs `bundle exec pgbus start` alongside your web server.

3. **Advisory locks vs. visibility timeouts**: GoodJob uses PostgreSQL advisory locks to claim jobs. Pgbus uses PGMQ's visibility timeout -- a claimed message becomes invisible for `visibility_timeout` seconds. If a worker crashes without archiving the message, it automatically becomes available again after the timeout expires. This is more resilient than advisory locks, which release on disconnect.

4. **Job record preservation**: GoodJob has `preserve_job_records` for keeping completed job records. PGMQ automatically archives completed messages to `a_pgbus_*` tables, which serve a similar purpose for debugging and auditing.

5. **Queue naming**: GoodJob uses bare queue names. Pgbus prefixes all queues (`pgbus_default`). Your `queue_as` declarations work unchanged -- the prefix is applied automatically.
