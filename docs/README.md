# Pgbus Documentation

## Migration guides

Switching to Pgbus from another job backend? These guides cover what changes, what stays the same, and what to watch out for.

| Guide | Backend | Key differences |
|-------|---------|-----------------|
| [Switch from Sidekiq](switch_from_sidekiq.md) | Sidekiq (+ Pro/Enterprise) | Remove Redis, convert native workers to ActiveJob, replace middleware with callbacks |
| [Switch from SolidQueue](switch_from_solid_queue.md) | SolidQueue | Similar architecture (PostgreSQL + `SKIP LOCKED`), swap config format, gain LISTEN/NOTIFY + worker recycling |
| [Switch from GoodJob](switch_from_good_job.md) | GoodJob | Both PostgreSQL-native with LISTEN/NOTIFY, swap advisory locks for PGMQ visibility timeouts, gain worker recycling |

## Feature comparison

| Feature | Sidekiq | SolidQueue | GoodJob | Pgbus |
|---------|---------|------------|---------|-------|
| Infrastructure | Redis | PostgreSQL | PostgreSQL | PostgreSQL (PGMQ) |
| ActiveJob adapter | Yes | Yes | Yes | Yes |
| Bulk enqueue | No | Yes | Yes | Yes |
| LISTEN/NOTIFY | N/A | No (polling only) | Yes | Yes |
| Dead letter queues | No (retries only) | No | No | Yes |
| Worker recycling | No | No | No | Yes |
| Event bus | No | No | No | Yes |
| Idempotent events | No | No | No | Yes |
| Concurrency controls | Enterprise | `limits_concurrency` | `good_job_control_concurrency_with` | `Pgbus::Concurrency` |
| Recurring/cron jobs | `sidekiq-cron` gem | `config/recurring.yml` | `config.good_job.cron` | Planned |
| Batches | Pro | No | `GoodJob::Batch` | Planned |
| Web dashboard | `Sidekiq::Web` | Mission Control | `GoodJob::Engine` | `Pgbus::Engine` |
