---
description: "Benchmark the current branch against main. Use when a change touches a hot path (client ops, executor, serialization, polling, connection pool) or when asked to measure performance."
model: sonnet
argument-hint: "optional: a specific bench name (e.g. client_bench)"
---

# Performance Command

Measure, don't guess. This command produces a **same-machine before/after** so
any performance claim is backed by numbers.

See `docs/performance.md` for the hot paths and how the harness works.

## The non-negotiable rule

**Measure BEFORE you change.** A delta you didn't baseline is not a delta. If a
change already landed without a baseline, reconstruct one from `main` in a
worktree (below) — never report a number against a baseline from another machine
or another day.

## Workflow

### 1. Baseline `main` (before)

Capture pristine `main` with the SAME bench script you'll run on the branch:

```bash
git worktree add --detach /tmp/pgbus-baseline main
cp -r benchmarks /tmp/pgbus-baseline/ && cp Gemfile Rakefile /tmp/pgbus-baseline/
(cd /tmp/pgbus-baseline && bundle install --quiet && bundle exec rake bench:all) > /tmp/before.txt
```

### 2. Measure the branch (after)

```bash
bundle exec rake bench:all > /tmp/after.txt
diff /tmp/before.txt /tmp/after.txt
git worktree remove --force /tmp/pgbus-baseline
```

For a single hot path, use `rake bench:one[client_bench]` in both trees.

### 3. Report HONESTLY

- Give throughput (i/s) AND allocations (obj/call, retained). Retained > 0
  per operation is a leak — call it out.
- **Distinguish method-level from system-level wins.** A 2x faster
  `send_message` does NOT mean 2x faster job processing — PGMQ round-trip +
  database dominate. Say which the number is.
- If a number is within run-to-run noise (`benchmark-ips` shows ±%), say
  "within noise."
- If you only measured *after* (no clean baseline), say so explicitly.

### 4. Keep perf continuous

- [ ] A bench exists for the changed hot path. Add one if missing.
- [ ] The before/after numbers are in the PR body.
- [ ] `docs/performance.md` updated if representative numbers moved.

## The hot paths to watch

| Path | Bench | Note |
|------|-------|------|
| `Client#send_message` / `#send_batch` | `client_bench.rb` | The enqueue path — overhead per job. |
| `Client#read_batch` | `client_bench.rb` | The dequeue path — runs every poll cycle. |
| `Executor#execute` | `executor_bench.rb` | Per-job execution overhead (deserialization + dispatch). |
| JSON serialization | `serialization_bench.rb` | Payload encoding/decoding — scales with payload size. |
| Connection pool checkout | `connection_pool_bench.rb` | Peak connections under concurrency. |
| Execution pool throughput | `execution_pool_bench.rb` | ThreadPool vs AsyncPool latency + memory. |
| Streams SSE broadcast | `streams_bench.rb` | Real Puma + SSE fan-out (requires DB). |

Argument (`$ARGUMENTS`): if a specific bench is named, focus with
`rake bench:one[$ARGUMENTS]`; otherwise run the full suite.
