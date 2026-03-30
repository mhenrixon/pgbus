# Switch from SolidQueue to Pgbus

## Overview

SolidQueue and Pgbus are both PostgreSQL-backed job processors with similar architectures: supervisor/worker process model, `FOR UPDATE SKIP LOCKED` for contention-free polling, and forked processes. The migration is straightforward since both are ActiveJob adapters.

**Key differences:** Pgbus adds LISTEN/NOTIFY for instant wake-up (SolidQueue only polls), dead letter queues, worker recycling, and an event bus. Pgbus uses PGMQ under the hood instead of custom tables.

**Effort estimate:** Low. Both are pure ActiveJob adapters, so your jobs work unchanged.

## Step 1: Update dependencies

```ruby
# Gemfile

# Remove
gem "solid_queue"
gem "mission_control-jobs"  # if used

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
config.active_job.queue_adapter = :solid_queue

# After
config.active_job.queue_adapter = :pgbus
```

If you set the adapter per-environment or use `config.solid_queue.connects_to` for a separate queue database, update those too.

## Step 3: Convert worker configuration

```yaml
# Before: config/queue.yml (SolidQueue)
production:
  dispatchers:
    - polling_interval: 1
      batch_size: 500
  workers:
    - queues: "critical"
      threads: 5
      processes: 2
      polling_interval: 0.1
    - queues: "default,low"
      threads: 3
      processes: 3
      polling_interval: 1
```

```yaml
# After: config/pgbus.yml
production:
  workers:
    - queues: [critical]
      threads: 5
    - queues: [critical]
      threads: 5
    - queues: [default, low]
      threads: 3
    - queues: [default, low]
      threads: 3
    - queues: [default, low]
      threads: 3
  max_retries: 5
  max_jobs_per_worker: 10000
  max_memory_mb: 512
```

> **Note:** SolidQueue's `processes: N` forks N identical workers. In Pgbus, list the same worker config N times, or let the supervisor handle it via configuration (one entry per process).

### Configuration mapping

| SolidQueue | Pgbus | Notes |
|------------|-------|-------|
| `polling_interval` | `polling_interval` | Pgbus defaults to 0.1s; LISTEN/NOTIFY makes this a fallback only |
| `threads` | `threads` | Same concept |
| `processes` | Repeat worker entry | One entry per forked process |
| `dispatchers[].batch_size` | N/A | Pgbus dispatcher does maintenance, not dispatch |
| `queues: "a,b"` (string) | `queues: [a, b]` (array) | Different format |
| `queues: "*"` (wildcard) | List queues explicitly | PGMQ queues are explicit |

## Step 4: Remove concurrency controls

SolidQueue's `limits_concurrency` is a SolidQueue-specific mixin. Remove it from your jobs:

```ruby
# Before: SolidQueue concurrency control
class ProcessOrderJob < ApplicationJob
  limits_concurrency to: 1,
                     key: ->(order) { order.account_id },
                     duration: 15.minutes,
                     on_conflict: :block

  def perform(order)
    # ...
  end
end
```

```ruby
# After: Remove the SolidQueue mixin
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
>                      key: ->(order) { order.account_id },
>                      duration: 15.minutes,
>                      on_conflict: :block
>   def perform(order)
>     # ...
>   end
> end
> ```

## Step 5: Migrate recurring tasks

If you use SolidQueue's `config/recurring.yml`:

```yaml
# Before: config/recurring.yml (SolidQueue)
production:
  daily_cleanup:
    class: CleanupJob
    schedule: "every day at 2am"
    queue: maintenance
  hourly_sync:
    class: SyncJob
    schedule: "0 * * * *"
    args: [42, "sync"]
```

Pgbus does not yet have built-in recurring task support. Options:

1. **Use the `whenever` gem** for cron-based scheduling:
   ```ruby
   # config/schedule.rb (whenever gem)
   every 1.day, at: "2:00 am" do
     runner "CleanupJob.perform_later"
   end
   every :hour do
     runner "SyncJob.perform_later(42, 'sync')"
   end
   ```

2. **Use system cron** directly:
   ```cron
   0 2 * * * cd /app && bin/rails runner "CleanupJob.perform_later"
   0 * * * * cd /app && bin/rails runner "SyncJob.perform_later(42, 'sync')"
   ```

3. Wait for Pgbus recurring task support (planned).

## Step 6: Replace the dashboard

```ruby
# Before: config/routes.rb
mount MissionControl::Jobs::Engine, at: "/jobs"

# After:
mount Pgbus::Engine => "/pgbus"
```

## Step 7: Update process management

```bash
# Before
bundle exec rake solid_queue:start

# After
bundle exec pgbus start
```

## Step 8: Clean up SolidQueue tables

After verifying Pgbus processes jobs correctly and SolidQueue's tables are drained:

```ruby
class RemoveSolidQueue < ActiveRecord::Migration[7.1]
  def up
    drop_table :solid_queue_blocked_executions, if_exists: true
    drop_table :solid_queue_claimed_executions, if_exists: true
    drop_table :solid_queue_failed_executions, if_exists: true
    drop_table :solid_queue_pauses, if_exists: true
    drop_table :solid_queue_processes, if_exists: true
    drop_table :solid_queue_ready_executions, if_exists: true
    drop_table :solid_queue_recurring_executions, if_exists: true
    drop_table :solid_queue_recurring_tasks, if_exists: true
    drop_table :solid_queue_scheduled_executions, if_exists: true
    drop_table :solid_queue_semaphores, if_exists: true
    drop_table :solid_queue_jobs, if_exists: true
  end
end
```

## What you gain

- **LISTEN/NOTIFY** -- SolidQueue only polls (100-150ms latency baseline). Pgbus wakes workers instantly via PostgreSQL LISTEN/NOTIFY, with polling as fallback only.
- **Dead letter queues** -- SolidQueue marks jobs as failed but keeps them in the same table. Pgbus routes failures to dedicated `_dlq` queues after `max_retries` for clear separation.
- **Worker recycling** -- SolidQueue workers run indefinitely. Pgbus recycles workers by job count, memory, or lifetime to prevent memory bloat.
- **Event bus** -- AMQP-style pub/sub with topic routing, not available in SolidQueue.
- **PGMQ under the hood** -- battle-tested message queue extension with visibility timeouts and atomic operations.

## What you lose (for now)

| SolidQueue feature | Status in Pgbus |
|--------------------|-----------------|
| `limits_concurrency` | `Pgbus::Concurrency` with `limits_concurrency` DSL |
| `config/recurring.yml` | Planned |
| Queue pausing (`SolidQueue::Queue.pause`) | Planned |
| Separate queue database | Not planned (PGMQ lives in your primary DB) |
| Numeric job priorities | PGMQ reads in FIFO order per queue; use separate queues for priority |

## Gotchas

1. **PgBouncer**: If you run PgBouncer in transaction mode, LISTEN/NOTIFY won't work. Use session mode or direct connections for Pgbus worker processes. This also applies to PGMQ's `FOR UPDATE SKIP LOCKED`.

2. **Separate queue database**: SolidQueue supports `connects_to` for a dedicated queue DB. Pgbus requires PGMQ in the same database it connects to. If you need isolation, use a separate PostgreSQL database with PGMQ installed and configure `Pgbus.configuration.database_url`.

3. **Queue naming**: SolidQueue uses bare queue names (`default`). Pgbus prefixes all queues (`pgbus_default`). Your `queue_as :default` declarations work unchanged -- the prefix is applied automatically.

4. **`ActionMailer::MailDeliveryJob`**: This can bypass the application-level adapter setting in some Rails versions. If mailer jobs don't appear in Pgbus, add to `ApplicationMailer`:
   ```ruby
   class ApplicationMailer < ActionMailer::Base
     self.deliver_later_queue_name = :mailers
   end
   ```
