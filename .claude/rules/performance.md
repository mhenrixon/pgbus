# Performance Rules

## Context Window Management

**Critical**: Your context window can shrink significantly with many tools enabled.

Guidelines:
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

## Performance Checklist

- [ ] LISTEN/NOTIFY enabled for active queues
- [ ] Batch reads used where possible
- [ ] Connection pool sized appropriately
- [ ] Worker recycling configured
- [ ] No N+1 queries in dashboard DataSource
