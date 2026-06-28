# Performance Rules

Pgbus must be fast in the places that run on every job enqueue, dequeue, and
execute cycle. These rules make performance a standing part of every change.
See `docs/performance.md` for the harness and current numbers.

## The prime directive

**Measure before you change. Measure after. Report both, honestly.**

A performance claim without a same-machine before/after is not allowed in a PR
or a commit message. If you didn't baseline, you don't have a delta — say
"measured after only" or capture the baseline (`/perf` automates this with a
worktree).

## When performance is in scope

Any change to a **hot path** must come with a bench and a before/after:

| Hot path | File | Bench |
|----------|------|-------|
| Message enqueue | `lib/pgbus/client.rb` (`send_message`, `send_batch`) | `client_bench.rb` |
| Message dequeue | `lib/pgbus/client.rb` (`read_batch`) | `client_bench.rb` |
| Job execution | `lib/pgbus/active_job/executor.rb` | `executor_bench.rb` |
| JSON serialization | payload encoding/decoding | `serialization_bench.rb` |
| Connection pool | `lib/pgbus/execution_pools/` | `connection_pool_bench.rb`, `execution_pool_bench.rb` |
| SSE streaming | `lib/pgbus/streams/` | `streams_bench.rb` |

A pure docs/test/refactor change with no hot-path edit does not need a bench.

## Always Do

1. **Baseline first** — capture `main` numbers BEFORE editing.
   Run `rake bench:all` or `rake bench:one[name]` for a focused check.
2. **Add a bench for a new hot path** — `benchmarks/<name>_bench.rb` using the
   shared `BenchSupport` harness. `rake bench:all` discovers unit benches automatically.
3. **Report throughput AND allocations** — i/s + obj/call. Flag any non-zero
   *retained* per operation (a leak).
4. **Distinguish method-level from system-level wins** — a 2x faster
   `send_message` does NOT mean 2x faster job processing (PGMQ round-trip +
   database dominate); it means less gem overhead per enqueue. Say which.
5. **Update docs + CHANGELOG** — if representative numbers moved, update
   `docs/performance.md`; note the change under a `perf:` CHANGELOG entry.

## Never Do

1. **Never claim a speedup without a measured before/after.** No "this should be
   faster." Prove it or don't say it.
2. **Never optimize a cold path** the bench shows isn't hot — three clear lines
   beat a clever micro-optimization that obscures behavior for no measured gain.
3. **Never break a correctness invariant for speed** — the PGMQ client wrapper,
   queue prefixing, dead letter routing, and visibility timeout are not
   negotiable. A faster wrong answer is wrong.
4. **Never trade carefully-tested behavior for a marginal allocation win.** If
   the bench doesn't prove it matters and the tests don't still pass, don't ship.
5. **Never add a hard CI perf gate on a flaky threshold** — the `bench` CI job is
   run-and-report (uploads an artifact), not a merge blocker. Shared runners are
   too noisy for a hard cutoff.

## Context Window Management

- Keep under 10 MCPs enabled per project
- Avoid loading large files unnecessarily
- Use targeted searches over broad exploration

## PGMQ Performance

### Polling vs LISTEN/NOTIFY

```ruby
# Good: Use LISTEN/NOTIFY for instant wake-up
pgmq.enable_notify_insert(queue_name, throttle_interval_ms: 250)

# Polling as fallback only -- not primary mechanism
sleep(config.polling_interval) # only when LISTEN/NOTIFY unavailable
```

### Batch Operations

```ruby
# Good: Batch reads for throughput
client.read_batch(queue, qty: idle_threads)

# Bad: Single reads in a loop
idle_threads.times { client.read_message(queue) }
```

### Connection Pool Sizing

- Pool size should match worker thread count
- Don't over-provision -- each connection holds PostgreSQL resources
- Use `-> { ActiveRecord::Base.connection.raw_connection }` in Rails to share the pool

### Queue Table Performance

- PGMQ's `q_` tables use `vt ASC` index for fast reads
- `FOR UPDATE SKIP LOCKED` prevents contention between workers
- Archive tables grow unbounded -- consider partitioning for high-volume queues
- Purge processed events table periodically (idempotency dedup)

### Autovacuum Tuning for Queue Tables

Queue and archive tables have high write/delete churn. Default autovacuum settings
are too conservative. Apply per-table tuning:

```sql
-- Queue tables: aggressive vacuum (high delete rate)
ALTER TABLE pgmq.q_pgbus_default SET (
  autovacuum_vacuum_scale_factor = 0.01,    -- vacuum at 1% dead (default 20%)
  autovacuum_vacuum_cost_delay = 2,          -- faster vacuum cycles (default 20ms)
  autovacuum_analyze_scale_factor = 0.05     -- re-analyze at 5% change
);

-- Archive tables: append-heavy, vacuum less critical but still important
ALTER TABLE pgmq.a_pgbus_default SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_vacuum_cost_delay = 5,
  autovacuum_analyze_scale_factor = 0.05
);
```

**Why this matters**: Dead tuples accumulate when autovacuum can't keep up.
This causes B-tree index bloat (each dead index entry requires I/O), increasing
lock acquisition time. Long-running transactions pin the MVCC horizon, preventing
vacuum from cleaning any dead tuples created while the transaction is open.

Monitor via the dashboard Queue Health panel or Prometheus metrics:
- `pgbus_table_dead_tuples` — dead tuple count per table
- `pgbus_table_bloat_ratio` — dead / (dead + live) per table
- `pgbus_oldest_transaction_age_seconds` — MVCC horizon pin risk

### Archive Retention Sizing

Default: `archive_retention = 7.days`. Tune based on:

- **High-volume queues (>100 msgs/sec)**: Consider 1-3 days to keep archive tables manageable
- **Audit-sensitive queues**: 30+ days if compliance requires it
- **Per-stream retention**: Use `streams_retention` hash for granular control

```ruby
config.archive_retention = 3.days                    # global default
config.streams_retention = {                          # per-stream overrides
  "orders.*" => 30.days,                              # keep order events longer
  "notifications.*" => 1.day                          # ephemeral, purge quickly
}
```

The dispatcher's `compact_archives` runs hourly (configurable) and deletes
archive entries older than the retention window in batches of 1000.

## Worker Performance

### Memory Management

```ruby
# Good: Recycling prevents memory bloat
max_jobs_per_worker: 10_000
max_memory_mb: 512
max_worker_lifetime: 3600

# Bad: Workers running forever without limits
# (this is solid_queue's problem)
```

### Thread Pool Sizing

- Match threads to expected concurrency
- More threads = more PGMQ connections needed
- Monitor with `pool.queue_length` vs `pool.max_length`

## Performance Checklist (before marking perf work complete)

- [ ] Baseline captured BEFORE the change (same machine, same script)
- [ ] After numbers captured; before/after in the PR body
- [ ] A bench exists for every hot path touched
- [ ] Throughput + allocations reported; retained-per-operation is 0
- [ ] Method-level vs system-level framing is honest
- [ ] `docs/performance.md` + CHANGELOG updated if numbers moved
- [ ] `bundle exec rspec` still green
- [ ] No correctness invariant traded for speed
- [ ] LISTEN/NOTIFY enabled for active queues
- [ ] Batch reads used where possible
- [ ] Connection pool sized appropriately
- [ ] Worker recycling configured
- [ ] No N+1 queries in dashboard DataSource
- [ ] Autovacuum tuned for queue/archive tables
- [ ] Archive retention sized for volume
- [ ] Queue Health dashboard monitored for dead tuple growth
- [ ] No long-running transactions pinning MVCC horizon
