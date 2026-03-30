# Pgbus

PostgreSQL-native job processing and event bus for Rails, built on PGMQ.

## Tech Stack

- **Ruby**: >= 3.3 | **Rails**: >= 7.1
- **Transport**: pgmq-ruby (PGMQ PostgreSQL extension)
- **Concurrency**: concurrent-ruby
- **Autoloading**: zeitwerk
- **Testing**: RSpec
- **Linting**: RuboCop

## Critical Rules

### Never Do
1. **NO direct PGMQ calls** — always go through `Pgbus::Client`
2. **NO hardcoded queue names** — use `config.queue_name()`
3. **NO raw SQL in dashboard** — use `Web::DataSource`
4. **NO `Marshal.load`** — JSON serialization only
5. **NO unsynchronized shared state** — use Mutex or Concurrent primitives
6. **NO swallowing errors** — log via `Pgbus.logger`, track in `pgbus_failed_events`

### Always Do
1. **TDD**: Write tests BEFORE implementation
2. **Worker recycling**: Configure `max_jobs`, `max_memory_mb`, `max_lifetime`
3. **Dead letter routing**: Check `read_ct` > `max_retries`
4. **LISTEN/NOTIFY**: Use `enable_notify_insert` for instant wake-up
5. **Queue prefix**: All queues through `config.queue_name()`
6. **Visibility timeout**: Always pass `vt:` parameter on reads

## Commands

```bash
bundle exec rspec          # Run tests
bundle exec rubocop        # Lint
bundle exec rake           # Both
```

## Slash Commands

| Command | Purpose |
|---------|---------|
| `/lfg` | Full autonomous workflow: branch → understand → explore → plan → TDD → verify → PR |
| `/github-review-comments` | Process unresolved PR review comments |
| `/review-pr` | Review a PR for pattern compliance |
| `/tdd` | Enforce RED → GREEN → REFACTOR cycle |
| `/security` | Security audit (PGMQ ops, connections, auth, deserialization) |
| `/architect` | Coordinate multi-layer development |

## Architecture

```
Layer 6: Dashboard       app/controllers/pgbus/, app/views/pgbus/
Layer 5: CLI             lib/pgbus/cli.rb
Layer 4: Process Model   lib/pgbus/process/ (supervisor, worker, dispatcher, consumer)
Layer 3: Event Bus       lib/pgbus/event_bus/ (publisher, subscriber, registry, handler)
Layer 2: ActiveJob       lib/pgbus/active_job/ (adapter, executor)
Layer 1: Client          lib/pgbus/client.rb (PGMQ wrapper)
Layer 0: Config          lib/pgbus/configuration.rb, config_loader.rb
```

## Key Design Decisions

- Worker recycling via `max_jobs_per_worker`, `max_memory_mb`, `max_worker_lifetime` — fixes solid_queue's memory leak problem
- LISTEN/NOTIFY via PGMQ's `enable_notify_insert` for instant wake-up (polling as fallback only)
- Dead letter queues: after `max_retries` failed reads (tracked by PGMQ's `read_ct`), move to `_dlq` queue
- Idempotent events: `pgbus_processed_events` table with (event_id, handler_class) unique index
- Dashboard via Tailwind CDN + Turbo CDN — zero npm dependency

## Queue Naming

All PGMQ queues are prefixed: `{queue_prefix}_{name}` (default: `pgbus_default`).
DLQ queues append `_dlq` suffix.

## More Documentation

See `.claude/` directory:
- `commands/` — Slash command definitions
- `rules/` — Coding style, git workflow, testing, agents, performance, security
