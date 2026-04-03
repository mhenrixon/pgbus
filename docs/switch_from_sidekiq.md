# Switch from Sidekiq to Pgbus

## Overview

Sidekiq uses Redis as its message broker. Pgbus uses PostgreSQL via PGMQ. Switching eliminates Redis from your infrastructure and gives you dead letter queues, worker recycling, and an event bus -- all backed by your existing database.

**Effort estimate:** Low if you use ActiveJob exclusively. Medium-high if you use native Sidekiq workers or Pro/Enterprise features.

## Step 1: Update dependencies

```ruby
# Gemfile

# Remove
gem "sidekiq"
gem "sidekiq-cron"          # if used
gem "sidekiq-unique-jobs"   # if used

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
# config/application.rb (or config/environments/production.rb)

# Before
config.active_job.queue_adapter = :sidekiq

# After
config.active_job.queue_adapter = :pgbus
```

## Step 3: Convert native Sidekiq workers

If all your jobs inherit from `ApplicationJob` (ActiveJob), skip this step -- they work unchanged.

If you have native Sidekiq workers using `include Sidekiq::Job`, convert them:

```ruby
# Before: Native Sidekiq worker
class HardWorker
  include Sidekiq::Job
  sidekiq_options queue: :critical, retry: 5

  def perform(user_id, action)
    user = User.find(user_id)
    # ...
  end
end

# Enqueue
HardWorker.perform_async(user.id, "activate")
HardWorker.perform_at(5.minutes.from_now, user.id, "activate")
```

```ruby
# After: ActiveJob
class HardWorker < ApplicationJob
  queue_as :critical
  retry_on StandardError, wait: :polynomially_longer, attempts: 5

  def perform(user_id, action)
    user = User.find(user_id)
    # ...
  end
end

# Enqueue
HardWorker.perform_later(user.id, "activate")
HardWorker.set(wait: 5.minutes).perform_later(user.id, "activate")
```

### API mapping

| Sidekiq | ActiveJob / Pgbus |
|---------|-------------------|
| `perform_async(args)` | `perform_later(args)` |
| `perform_at(time, args)` | `.set(wait_until: time).perform_later(args)` |
| `perform_in(duration, args)` | `.set(wait: duration).perform_later(args)` |
| `sidekiq_options queue: :name` | `queue_as :name` |
| `sidekiq_options retry: N` | `retry_on StandardError, attempts: N` |
| `sidekiq_retries_exhausted` | `discard_on` + `after_discard` callback |

## Step 4: Replace middleware with ActiveJob callbacks

Sidekiq middleware wraps job push (client) and execution (server) in a Rack-style chain. ActiveJob provides equivalent hooks.

```ruby
# Before: Sidekiq server middleware
class LoggingMiddleware
  include Sidekiq::ServerMiddleware
  def call(job_instance, job_payload, queue)
    Rails.logger.info("Starting #{job_payload['class']}")
    yield
    Rails.logger.info("Finished #{job_payload['class']}")
  end
end
```

```ruby
# After: ActiveJob callback (add to ApplicationJob or per-job)
class ApplicationJob < ActiveJob::Base
  around_perform do |job, block|
    Rails.logger.info("Starting #{job.class.name}")
    block.call
    Rails.logger.info("Finished #{job.class.name}")
  end
end
```

### Callback mapping

| Sidekiq middleware | ActiveJob callback |
|--------------------|-------------------|
| Server middleware (before yield) | `before_perform` |
| Server middleware (around yield) | `around_perform` |
| Server middleware (after yield) | `after_perform` |
| Client middleware (before yield) | `before_enqueue` |
| Client middleware (around yield) | `around_enqueue` |
| Client middleware (after yield) | `after_enqueue` |

## Step 5: Configure workers

```yaml
# Before: config/sidekiq.yml
:concurrency: 10
:queues:
  - [critical, 3]
  - [default, 2]
  - [low, 1]
```

```yaml
# After: config/pgbus.yml
production:
  workers:
    - queues: [critical]
      threads: 5
    - queues: [default, low]
      threads: 10
  max_jobs_per_worker: 10000
  max_memory_mb: 512
  max_worker_lifetime: 3600
```

## Step 6: Replace the dashboard

```ruby
# Before: config/routes.rb
require "sidekiq/web"
mount Sidekiq::Web => "/sidekiq"

# After:
mount Pgbus::Engine => "/pgbus"
```

If you used `Sidekiq::Web`'s authentication, you have two options:

**Option A: Simple auth lambda** (like Sidekiq's `Sidekiq::Web` constraint):

```ruby
Pgbus.configure do |config|
  config.web_auth = ->(request) {
    request.env["warden"].user&.admin?
  }
end
```

**Option B: Inherit from your authenticated controller** (recommended for admin namespaces):

```ruby
Pgbus.configure do |config|
  config.base_controller_class = "Admin::BaseController"
  config.return_to_app_url = "/admin"  # adds a back button in the dashboard nav
end
```

This is the equivalent of Sidekiq's `Sidekiq::Web` authentication constraint but more flexible -- your base controller's `before_action` filters, helpers, and layout apply automatically.

## Step 7: Update process management

```bash
# Before
bundle exec sidekiq

# After
bundle exec pgbus start
```

Update your `Procfile`, systemd units, or container entrypoints accordingly.

## Step 8: Remove Redis

Once all Sidekiq jobs have drained and you've verified Pgbus is processing correctly, remove Redis from your infrastructure:

- Remove `REDIS_URL` / Sidekiq Redis config from environment
- Remove `Sidekiq.configure_server` / `Sidekiq.configure_client` blocks
- Remove Redis from `docker-compose.yml`, Terraform, etc.

## What you gain

- **No Redis dependency** -- one fewer service to operate
- **Dead letter queues** -- failed jobs route to `_dlq` queues after `max_retries`, visible in the dashboard
- **Worker recycling** -- memory, job count, and lifetime limits prevent the memory bloat that plagues long-running Sidekiq processes
- **Event bus** -- AMQP-style topic routing for pub/sub, built into the same infrastructure
- **LISTEN/NOTIFY** -- instant job wake-up without polling overhead

## Feature comparison

| Sidekiq feature | Pgbus equivalent |
|-----------------|-----------------|
| Batches (Pro) | `Pgbus::Batch` with `on_finish` / `on_success` / `on_discard` callbacks |
| Concurrency controls (Enterprise) | `limits_concurrency` DSL (auto-included into ActiveJob::Base) |
| Unique jobs (Enterprise / `sidekiq-unique-jobs`) | `ensures_uniqueness` DSL (auto-included into ActiveJob::Base) |
| Cron / recurring jobs (`sidekiq-cron`) | `config/recurring.yml` with cron syntax (Fugit) |
| Rate limiting (Enterprise) | Use `limits_concurrency` for most cases; sliding-window rate limiting not yet built |
| Real-time metrics (Sidekiq Web) | Pgbus dashboard covers queue depth, failures, processes, recurring tasks |
| `ActiveJob::Continuation` (Rails 8.1+) | Supported -- `stopping?` wired to worker lifecycle |

## Gotchas

1. **Argument serialization**: Sidekiq passes raw JSON; ActiveJob uses GlobalID for ActiveRecord objects. If you pass raw IDs (`user.id`), both work the same. If you pass AR objects (`user`), ActiveJob serializes via GlobalID automatically.

2. **`Sidekiq::Testing.fake!`**: Replace with `ActiveJob::TestHelper`:
   ```ruby
   include ActiveJob::TestHelper
   assert_enqueued_jobs 1 { MyJob.perform_later(args) }
   ```

3. **`Sidekiq.redis { |conn| ... }`**: If you use Sidekiq's Redis connection for custom caching or distributed locks, you'll need a separate solution (e.g., PostgreSQL advisory locks, `with_advisory_lock` gem).

4. **Job priorities**: Sidekiq uses queue weight ordering. Pgbus processes queues in the order listed in the worker config. Put higher-priority queues first.
