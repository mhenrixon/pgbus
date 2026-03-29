# Pgbus

PostgreSQL-native job processing and event bus for Rails, built on PGMQ.

## Architecture

- **Transport**: pgmq-ruby wraps PGMQ PostgreSQL extension
- **ActiveJob Adapter**: `Pgbus::ActiveJob::Adapter` — enqueue/enqueue_at/enqueue_all
- **Event Bus**: AMQP-style topic routing via PGMQ topics, idempotent handlers
- **Process Model**: Supervisor forks Workers/Dispatcher/Consumers with memory recycling
- **Dashboard**: Mountable Rails engine (planned)

## Key Design Decisions

- Worker recycling via `max_jobs_per_worker`, `max_memory_mb`, `max_worker_lifetime` — fixes solid_queue's memory leak problem
- LISTEN/NOTIFY via PGMQ's `enable_notify_insert` for instant wake-up (polling as fallback only)
- Dead letter queues: after `max_retries` failed reads (tracked by PGMQ's `read_ct`), move to `_dlq` queue
- Idempotent events: `pgbus_processed_events` table with (event_id, handler_class) unique index
- No ActiveSupport dependency in core (only railties for engine/generators)

## Commands

```bash
bundle exec rspec          # Run tests
bundle exec rubocop        # Lint
bundle exec rake           # Both
```

## File Structure

- `lib/pgbus/client.rb` — Wraps pgmq-ruby with queue prefix management
- `lib/pgbus/active_job/` — ActiveJob adapter + executor
- `lib/pgbus/event_bus/` — Publisher, Subscriber, Registry, Handler
- `lib/pgbus/process/` — Supervisor, Worker, Dispatcher, Consumer, Heartbeat
- `lib/generators/pgbus/` — Install generator with migration template
- `exe/pgbus` — Gem executable

## Queue Naming

All PGMQ queues are prefixed: `{queue_prefix}_{name}` (default: `pgbus_default`).
DLQ queues append `_dlq` suffix.
