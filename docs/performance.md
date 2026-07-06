# Performance

Pgbus aims to be fast in the places that run on every job: the enqueue path
(`Client#send_message`), the dequeue path (`Client#read_batch`), and the
execution path (`Executor#execute`). This page documents how those paths are
kept fast, how to measure them yourself, and how performance is part of every
change.

The honest summary: **the gem overhead (serialization, queue routing, stats) is
what we control; the PGMQ round-trip and database I/O dominate end-to-end
latency.** So the unit benchmarks isolate gem overhead with a mocked PGMQ
client, while the integration benchmarks (when available) measure the full
stack. Measure before you optimize; the harness below exists so you never have
to guess.

## The hot paths

| Path | When it runs | Bench |
|------|--------------|-------|
| `Client#send_message` / `#send_batch` | every job enqueue | `client_bench.rb` |
| `Client#read_batch` | every worker poll cycle | `client_bench.rb` |
| `Executor#execute` | every job execution (deserialize + dispatch + archive) | `executor_bench.rb` |
| JSON serialization | every enqueue/dequeue (payload encoding) | `serialization_bench.rb` |
| Connection pool checkout | every PGMQ operation under concurrency | `connection_pool_bench.rb` |
| Execution pool dispatch | every job dispatched to thread/async pool | `execution_pool_bench.rb` |
| SSE streaming | every Turbo Stream broadcast | `streams_bench.rb` |
| Streamer replay read | every durable-stream wake + SSE connect (`Client#read_after`) | `streams_read_pool_bench.rb` |

## Measuring

Everything is driven from `rake`:

```bash
rake bench              # the unit benchmark suite (alias for bench:all)
rake bench:all          # serialization, client, executor — saves report to tmp/benchmarks/
rake bench:one[name]    # a single benchmark by name (e.g. client_bench)
rake bench:memory       # detailed memory profiling with allocation breakdown
rake bench:integration  # real PostgreSQL + PGMQ (requires PGBUS_DATABASE_URL)
rake bench:streams      # real Puma + SSE fan-out (requires PGBUS_DATABASE_URL)
rake bench:one[streams_read_pool_bench]  # streamer replay-read pool (requires PGBUS_DATABASE_URL)
rake bench:execution_modes  # threads vs async DB-connection consumption (requires PGBUS_DATABASE_URL)
```

- **Unit benches** (`benchmarks/*_bench.rb`) isolate gem overhead with a mocked
  PGMQ client using [`benchmark-ips`](https://github.com/evanphx/benchmark-ips)
  (throughput) and [`memory_profiler`](https://github.com/SamSaffron/memory_profiler)
  (allocations).
- **Integration benches** (`benchmarks/integration_bench.rb`) hit a real
  PostgreSQL + PGMQ instance for end-to-end latency.
- **Streams benches** (`benchmarks/streams_bench.rb`) boot a real Puma server
  and measure SSE broadcast fan-out.
- **Streamer replay-read bench** (`benchmarks/streams_read_pool_bench.rb`)
  isolates the per-wake `Client#read_after` cost against a real DB, comparing
  a fresh `PG.connect` per call (the pre-#315 `with_raw_connection` behavior) to
  the dedicated streams pool.

### Streamer connection model (issue #315)

The durable-stream publish and replay hot paths run on a **dedicated streams
DB pool** (`config.streams_pool_size` / `streams_pool_timeout`), separate from
the job pool (`pool_size`):

- `Client#send_stream_message` (the broadcast INSERT) draws from the streams
  pool, so a saturated job pool can't delay a broadcast on pool checkout.
- The dispatcher's per-wake reads (`read_after`, `stream_current_msg_id`,
  `stream_oldest_msg_id`) check out a persistent pooled connection via
  `Client#with_streams_connection` instead of a fresh `PG.connect` per call —
  measured ~23× faster per read against a real DB (191 μs vs 4.42 ms), removing
  the TCP + auth + TLS setup cost from the single dispatcher thread. This is a
  connection-setup win at the client layer, **not** end-to-end broadcast
  latency (which the PGMQ round-trip + LISTEN/NOTIFY + socket write dominate).

On the shared-ActiveRecord (Proc) connection path no separate pool is created
(libpq isn't thread-safe): streams share the single serialized connection, so
the isolation applies only to the dedicated `database_url` / `connection_params`
config.

**Capacity planning:** on the dedicated path this streams pool is *in addition*
to the job pool, and every forked process (worker, dispatcher, scheduler,
consumer) builds its own — even `--workers-only` processes that rarely stream.
Both pools are lazy, so an idle process holds few real connections, but when
sizing Postgres/PgBouncer `max_connections`, budget `pool_size (or the
auto-tuned resolved size) + streams_pool_size` per process rather than
`pool_size` alone.

### Fanout head-of-line blocking (issue #315 item 3)

A single dispatcher thread per web-server process fans out every broadcast to
every connected SSE client on the stream, writing to each socket **serially**.
A slow client's socket write blocks that thread up to a deadline before the
client is marked dead — and the connections queued behind it in the same wake
wait. With the full `streams_write_deadline_ms` (5s) that meant K slow clients
could stack a `K × 5s` stall on every other browser on the worker.

Fanout writes now use a separate, shorter `config.streams_fanout_write_deadline_ms`
(default **250 ms**), bounding the worst-case serial stall to `K × 250 ms`
(~20× lower). A slow-but-alive client that can't absorb a fanout frame in that
window is marked dead and reconnects; its `EventSource` `Last-Event-ID` then
replays the gap from the durable archive (or triggers a fresh re-render for
ephemeral streams). **Connect-replay** writes keep the full
`streams_write_deadline_ms`, so a new client catching up a large backlog isn't
evicted. Set `streams_fanout_write_deadline_ms = streams_write_deadline_ms` to
restore the pre-fix timing.

This is a **system-level** latency bound, not a per-write speedup: the fanout
happy path (all-fast clients) is unchanged — the `deadline_ms` keyword is a
zero-allocation pass-through (`streams_fanout_bench.rb`: 585k vs 583k i/s,
within noise; 0 retained/op). `config.streams_dispatch_queue_limit` (default
`0` = unbounded) optionally caps distinct-stream wake backlog by dropping
durable wakes (never ephemeral/connect) when the Listener sees the queue at the
cap — safe because the next durable wake re-reads from the min cursor.

### Durable fanout offload (issue #321)

The #315 deadline only *bounds* the serial stall (`K × 250 ms`); the dispatcher
still writes to every client in turn. `config.streams_writer_threads` (default
**0** = inline, the behavior above) moves the **durable** fanout socket write
off the dispatcher into a pool of N writer threads. Each connection is pinned to
one worker by `id.hash % N`, so its frames stay ordered and its per-io mutex is
never cross-contended. The dispatcher hands the write to the pump and moves on;
the pump reports back the highest msg_id it actually committed, and the
dispatcher — still the **sole owner** of the read cursor — advances it only on
that ack (so a blocked/partial write never advances the cursor past a frame that
didn't reach the socket). A failed write posts a `DisconnectMessage`, and the
dispatcher scrubs the connection on its own thread.

Measured decoupling (`streams_writer_offload_bench.rb`, dispatcher
`handle_durable_wake` wall time, 50 fast + K slow clients @ 50 ms each):

| K slow clients | inline (OFF) | offload (ON, 2 writers) |
|---|---|---|
| 0 | 1.1 ms | 1.1 ms |
| 1 | 55.6 ms | 0.2 ms |
| 5 | 268 ms | 0.5 ms |
| 20 | **1071 ms** | **0.09 ms** |

Inline scales linearly with K (a slow client blocks the thread); offload stays
flat — fast clients no longer wait behind slow ones. This is a **system-level
latency win, not a per-write speedup**: total bytes written are unchanged and a
slow client still takes its time on its own worker. The dispatcher does slightly
more bookkeeping per wake under offload (the post + ack round-trip), which is why
it stays **opt-in and default-off**; enable it only when slow-client
head-of-line latency is a measured problem.

**Ephemerals stay inline** regardless of `streams_writer_threads` — they have no
archive to replay, so they must not risk an async drop (the pump raises if one is
ever routed to it). `config.streams_writer_buffer_limit` (default `0` =
unbounded) caps a connection's outbound buffer, dropping its **oldest** durable
frame on overflow — safe because durable frames are re-read from the archive on
reconnect; it's an OOM guard for a pathologically slow-but-alive client, not a
delivery guarantee.

### Execution mode and DB pool sizing (threads vs async)

**Offered concurrency is not connections held.** A worker's `threads:` setting is
how many jobs run at once; `resolved_pool_size` is how many *DB connections* the
pgmq pool holds. They are not the same number, because a job holds a pgmq
connection only for the `read_batch` + `archive` SQL round-trip — `perform_now`
runs with **zero** pgmq connections checked out (`executor.rb`). If your job does
its own database work, that uses ActiveRecord's *separate* pool, not this one.

This means the folklore "async saves connections" is only half true, and the
`benchmarks/execution_modes_bench.rb` harness (`rake bench:execution_modes`,
requires `PGBUS_DATABASE_URL`) measures exactly when it holds. It runs the same
offered load (240 jobs at concurrency 12) through both pools and samples
`peak_busy = pool size − available` — the live checkout count — going through the
real pool, so pool *sharing* is what's measured. Representative local numbers:

| mode | pool | io profile | peak_busy | throughput | note |
|------|------|-----------|-----------|-----------|------|
| threads | 12 | io_light | 12 | ~216 job/s | baseline: ~1 conn per busy thread's checkout window |
| async | 3 | io_light | **3** | ~222 job/s | **12 fibers sustained on 3 connections, full throughput** |
| threads | 3 | io_light | 3 | ~208 job/s | short 10 ms checkouts cycle fast enough that 3 conns serve 12 threads too |
| threads | 12 | db_bound | 12 | ~320 job/s | connection-bound — one conn per concurrent DB call |
| async | 12 | db_bound | 12 | ~324 job/s | async matches threads when the pool is sized right |
| async | **3** | db_bound | 3 | **~95 job/s** | **under-provisioned: throughput collapses ~3.4×** (p50 37 ms → 125 ms) |

**What the numbers actually say:**

- **Async's connection-density win is real for I/O-light work** — where a job
  spends most of its time *outside* the checkout (HTTP calls, app compute, waits).
  There, a handful of connections serves many fibers with no throughput loss. This
  is why async workers are auto-sized to a flat `ASYNC_POOL_CONNECTIONS` (3) per
  capsule rather than one-per-fiber.
- **For DB-bound work, async is connection-bound just like threads.** A fiber
  holds a connection for the whole SQL call (a blocking libpq call does not yield
  the reactor), so a too-small async pool doesn't share — it **serialises**, and
  throughput collapses.
- **Under-provisioning degrades throughput; it does not necessarily error.**
  Because `pool_timeout` (5 s) dwarfs a typical checkout (10–30 ms), a
  too-small pool rarely times out — it just serialises work behind the available
  connections. Watch `throughput` and p95/p99 latency, not just error counts.

**Sizing guidance:**

| mode | recommended `pool_size` | under-provision symptom | over-provision cost |
|------|------------------------|-------------------------|---------------------|
| threads | ≈ `Σ worker threads` (+ dispatcher/scheduler/consumers) — the auto-tuned default | throughput collapse; eventually `pool_timeout` (`enrich_pool_timeout_error`), `available → 0` | wastes Postgres `max_connections`; `warn_if_oversized` fires above 50 |
| async | `ASYNC_POOL_CONNECTIONS` (3) for **I/O-light** work; **≈ your peak concurrent DB calls** for **DB-bound** work — measure with `rake bench:execution_modes` | reactor fibers serialise on checkout → throughput collapse (harder to spot; no error) | forfeits the density win async exists for |

This is a connection-**density** tool, not a throughput tuner: a smaller pool means
fewer Postgres backends for the same offered load, not faster jobs (PGMQ round-trip
and job I/O dominate wall time). The numbers are single-box, single-process, no
PgBouncer — a per-process, per-io-profile sizing guide, not a production capacity
guarantee. Budget `resolved_pool_size + streams_pool_size` per forked process (see
Capacity planning above).

### Reading the output

`benchmark-ips` reports **i/s** (iterations per second — higher is better).
`memory_profiler` reports **objects/bytes allocated** (transient GC pressure) and
**retained** (objects that survive — a steady climb here is a leak). For a
`send_message` call, *retained should be 0*; non-zero retained is the smell the
allocation budget specs catch.

### Measure BEFORE you change

The first rule: capture the baseline **before** touching code, or you can't
claim a delta. The cleanest way is an isolated worktree so `main` and your
branch run the *same script* on the *same machine*:

```bash
git worktree add --detach /tmp/pgbus-baseline main
cp -r benchmarks /tmp/pgbus-baseline/ && cp Gemfile Rakefile /tmp/pgbus-baseline/
(cd /tmp/pgbus-baseline && bundle install --quiet && bundle exec rake bench:all) > /tmp/before.txt
bundle exec rake bench:all > /tmp/after.txt
diff /tmp/before.txt /tmp/after.txt
git worktree remove --force /tmp/pgbus-baseline
```

There is no committed baseline file (shared CI runners are too noisy for a hard
regression gate), which is exactly why the before/after has to be a deliberate
same-machine measurement, not a comparison against a number from another box.

## Allocation budgets

The spec suite includes allocation budget tests (`spec/pgbus/allocation_budget_spec.rb`)
that enforce hard limits:

| Operation | Budget |
|-----------|--------|
| `Client#send_message` | < 50 objects/call |
| `Client#send_batch` (per item) | < 25 objects/item |
| `Client#read_batch` | < 30 objects/call |
| JSON round-trip | < 20 objects |
| Retained objects (leak detection) | 0 across 100 cycles |

These run as part of `bundle exec rspec` on every PR. They are hard gates — a
regression that exceeds the budget fails the build.

## CI

The `bench` job in `.github/workflows/main.yml` runs the unit benchmark suite
on every PR and **uploads the report as the `benchmarks` artifact**. It is
**run-and-report, never a hard fail** — it surfaces trends, it does not gate
merges on a flaky threshold. Download the artifact from the PR's checks tab to
see the numbers for that branch.

The allocation budget specs in the `test` job ARE a hard gate — those fail the
build if allocations regress past the budget.

## Adding a benchmark

A new hot path gets a new `benchmarks/<name>_bench.rb`. Use the shared harness:

```ruby
require_relative "bench_helper"

BenchSupport.header("my hot path")
BenchSupport.ips { |x| x.report("thing") { thing_under_test } }
BenchSupport.allocations("thing") { thing_under_test }
```

For unit-level benches that should run in CI, add the filename to the
`unit_benches` list in the `Rakefile`'s `bench` namespace.
