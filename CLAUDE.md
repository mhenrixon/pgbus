# Pgbus

PostgreSQL-native job processing and event bus for Rails, built on PGMQ.

## Tech Stack

- **Ruby**: >= 3.3 | **Rails**: >= 7.1
- **Transport**: pgmq-ruby (PGMQ — extension or embedded SQL)
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
7. **NO `Record` suffix on model classes** — see Model Naming below

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

## Model Naming

ActiveRecord models live in `app/models/pgbus/` and inherit from `Pgbus::BaseModel`.
**Never use a `Record` suffix.** Resolve naming conflicts as follows:

| Model Class | Table | Why not the obvious name |
|---|---|---|
| `Pgbus::BaseModel` | (abstract) | Avoids conflict with Rails `ApplicationRecord` |
| `Pgbus::BatchEntry` | `pgbus_batches` | `Pgbus::Batch` is the batch API class |
| `Pgbus::BlockedExecution` | `pgbus_blocked_executions` | — |
| `Pgbus::ProcessEntry` | `pgbus_processes` | `Process` conflicts with Ruby's `Process` module |
| `Pgbus::ProcessedEvent` | `pgbus_processed_events` | — |
| `Pgbus::RecurringExecution` | `pgbus_recurring_executions` | — |
| `Pgbus::RecurringTask` | `pgbus_recurring_tasks` | `Pgbus::Recurring::Task` is a different namespace |
| `Pgbus::Semaphore` | `pgbus_semaphores` | `Pgbus::Concurrency::Semaphore` is a different namespace |

When a model name collides with a service/module name, prefer `Entry` suffix or a descriptive alternative over `Record`.

## Separate Database Support

Pgbus supports running in the primary database or a dedicated database (like SolidQueue).

**Configuration** (`config.connects_to`):
- `nil` (default) — uses the primary Rails database
- `{ database: { writing: :pgbus } }` — uses a separate database

**Generator flags**:
- `rails generate pgbus:install --database=pgbus` — migrations go to `db/pgbus_migrate/`
- Without `--database` — migrations go to `db/migrate/` (default)

**database.yml example**:
```yaml
production:
  primary:
    <<: *default
    database: myapp_production
  pgbus:
    <<: *default
    database: myapp_pgbus_production
    migrations_paths: db/pgbus_migrate
```

## Key Design Decisions

- Worker recycling via `max_jobs_per_worker`, `max_memory_mb`, `max_worker_lifetime` — fixes solid_queue's memory leak problem
- LISTEN/NOTIFY via PGMQ's `enable_notify_insert` for instant wake-up (polling as fallback only)
- Dead letter queues: after `max_retries` failed reads (tracked by PGMQ's `read_ct`), move to `_dlq` queue
- Idempotent events: `pgbus_processed_events` table with (event_id, handler_class) unique index
- Dashboard via Tailwind CDN + Turbo CDN — zero npm dependency
- PGMQ schema install: extension-first with embedded SQL fallback (`pgmq_schema_mode: :auto | :extension | :embedded`)

## PGMQ Schema Management

PGMQ can be installed via PostgreSQL extension or embedded SQL (no extension required).

**Configuration** (`config.pgmq_schema_mode`):
- `:auto` (default) — tries extension, falls back to embedded SQL
- `:extension` — requires the pgmq PostgreSQL extension
- `:embedded` — uses vendored SQL, no extension needed

**Generators**:
- `rails generate pgbus:install --pgmq-schema-mode=auto` — initial setup
- `rails generate pgbus:upgrade_pgmq` — upgrade PGMQ schema to latest vendored version

**Rake tasks**:
- `rake pgbus:pgmq:status` — show installed vs available PGMQ version
- `rake pgbus:pgmq:versions` — list vendored PGMQ versions

**Key files**: `lib/pgbus/pgmq_schema.rb`, `lib/pgbus/pgmq_schema/pgmq_v*.sql`

## Queue Naming

All PGMQ queues are prefixed: `{queue_prefix}_{name}` (default: `pgbus_default`).
DLQ queues append `_dlq` suffix.

## More Documentation

See `.claude/` directory:
- `commands/` — Slash command definitions
- `rules/` — Coding style, git workflow, testing, agents, performance, security
