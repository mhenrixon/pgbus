# NOTIFY-gated worker/consumer wakeups (spike)

**Status:** prototype, opt-in, **off by default** (`config.worker_notify_wakeup = false`).
**Goal:** stop idle queues from issuing a `pgmq.read` on every polling tick.

## The problem

The `Worker` and `Consumer` run loops poll: read a batch, and if it comes back
empty, sleep `polling_interval` and read again. On an idle queue this is one
`pgmq.read` per queue per tick, forever. At a 0.1s interval across a handful of
queues and forks, that read volume can dominate database runtime — in one
production deployment it reached **~93% of DB runtime, ~1.6M `pgmq.read`
calls/24h**. The usual mitigation is to raise `polling_interval`, which trades
pickup latency for read volume.

The PGMQ INSERT NOTIFY trigger is already installed on every queue
(`Client#enable_notify_if_needed`), but nothing in the job/event loops consumes
it — the signal goes nowhere for workers and consumers (only the SSE streamer
listens). This spike consumes it: block until a real insert, poll only as a
fallback.

## Why the obvious approach does not work

A naive "just call `wait_for_notify` in the loop" fails for three concrete
reasons, all verified against the gem and pgmq-ruby 0.7.0 source:

1. **`wait_for_notify` is single-queue.** `PGMQ::Client#wait_for_notify(queue,
   timeout:)` LISTENs on exactly one channel inside one `with_connection` block.
   A worker watches N queues (six in the motivating deployment); the API can't
   express that.
2. **It holds the connection for the whole wait.** `with_connection` keeps the
   pooled connection checked out for the entire blocking wait — it does not
   release while idle. Running that on the shared worker pool starves it.
3. **LISTEN dies under a transaction-pool PgBouncer.** A persistent LISTEN does
   not survive transaction-pool COMMIT boundaries; the pooler silently unbinds
   the backend and NOTIFYs never arrive — and it does **not** raise, so a
   detect-and-retry guard never fires.

The first two rule out using `wait_for_notify` directly in the loop. The third
is the gating operational constraint (see below).

## The design

Mirror the SSE streamer's proven `Streamer::Listener` shape:

- **`Pgbus::Process::NotifyListener`** — one dedicated thread per Worker/Consumer
  fork that owns a single raw `PG::Connection` and hand-rolls `LISTEN
  "pgmq.q_<queue>.INSERT"` for every queue the fork reads. On any NOTIFY it calls
  an injected `on_wake` callable. On wait-timeout it runs `SELECT 1` (TCP
  keepalive + dead-connection detection) and re-LISTENs everything after a
  reconnect. The connection is built from `config.worker_notify_connection_options`.
- **Worker** passes its existing `WakeSignal#notify!` as `on_wake`. The run loop
  already waits on the `WakeSignal` after an empty fetch; with the listener
  active, `wake_timeout` raises that wait to a long fallback ceiling
  (`NOTIFY_FALLBACK_POLL_SECONDS = 15`), because NOTIFY now provides the pickup
  latency and the timeout is only a safety net for a missed wakeup.
- **Consumer** passes `SignalHandler#wake!` (a new helper that writes the
  existing self-pipe) as `on_wake`, interrupting its `interruptible_sleep` the
  instant an insert arrives — composing cleanly with signal wakeups.

Net effect on an idle queue: one read, then a NOTIFY-gated wait, instead of a
read every `polling_interval`. A missed NOTIFY costs at most
`NOTIFY_FALLBACK_POLL_SECONDS` of extra latency, never a stuck queue.

### Safety properties

- **Single-waiter `WakeSignal` is preserved.** Only the worker's main loop calls
  `#wait`; the listener thread only calls `#notify!`, which is safe from any
  thread. No new waiter is introduced.
- **Listener failure degrades to polling.** If the listener can't start (or its
  connection can't be built), `@notify_listener` stays nil and `wake_timeout`
  reverts to `effective_polling_interval` — identical to today's behavior.
- **Coalescing is intentional.** The listener doesn't care which channel fired;
  one wake makes the loop re-read all its queues. Wakes coalesce, reads don't
  amplify.

## The gating constraint: connection budget

A persistent LISTEN connection must bypass a transaction-pool PgBouncer. The
`worker_notify_*` overrides mirror `streams_*`: point the listener at a **direct
port** (`worker_notify_port`), while workers keep going through the pooler.

The cost is one direct connection **per fork** (not per host, not per Puma
worker — `NotifyListener` lives in the worker/consumer processes). For a
5-worker + 2-consumer × 2-host deployment that is ~14 new direct connections,
**on top of** the streamer's existing per-Puma-worker direct connection. This is
the scarce resource (e.g. PlanetScale Postgres' direct-port `max_connections`).

**This is why the feature is off by default and labelled a prototype.** Before
enabling it in production, validate that the direct-port connection ceiling
tolerates the added per-fork connections. The connection-math, not the code, is
the real risk.

## Configuration

```ruby
Pgbus.configure do |c|
  c.worker_notify_wakeup = true          # opt in (default false)
  c.worker_notify_port   = 5432          # pin the LISTEN conn to the direct port
  # or: c.worker_notify_host / c.worker_notify_database_url
end
```

With `worker_notify_wakeup = false` (the default) nothing changes: no listener
thread, no extra connection, plain polling.

## What this spike does NOT do

- It does not change the *read* primitive (`read_batch`/`read_multi` stay). It
  only changes *when* the loop decides to read.
- It does not adopt `read_with_poll` — that blocks the loop's main thread (fatal
  under `:async` fiber mode) and holds a connection, and its PgBouncer story
  differs from LISTEN.
- It does not touch grouped/fair reads (`group_mode`), which are orthogonal.
