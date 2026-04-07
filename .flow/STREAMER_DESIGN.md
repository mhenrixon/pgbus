# Pgbus Turbo Streams — Phase 0 Design (v2)

Status: **APPROVED — ready for Phase 1 implementation**. Steps 1+2+3 of the roadmap memo: drop-in transport, page-born-stale fix, per-stream retention. Code outside markdown blocks is illustrative only — nothing in this doc should be copy-pasted into `.rb` files without scrutiny.

**v2 changes from v1** (after research into Puma hijack semantics, long-lived-process recycling, and real-world precedents from message_bus / AnyCable / NATS):

- **Architectural reframe**: the Streamer is no longer a separate OS process forked by `Supervisor`. It is a singleton thread group that lives **inside each Puma worker**, owning hijacked SSE sockets and a single PG `LISTEN` connection. Enabled by the Puma finding that hijack releases the worker thread (§6) — the conventional wisdom that hijack ties up threads is wrong, it only applies to `ActionController::Live`-style code that blocks inside `@app.call(env)`.
- **No Supervisor changes.** Puma's existing cluster-mode worker lifecycle (including phased restart) IS the Streamer lifecycle. All the v1 "lame-duck / pre-fork / drain sequence" complexity is delegated to Puma.
- **No `Socket.send_io` cross-process FD passing.** The hijacked IO stays in the Puma worker process that accepted it.
- **Streams endpoint is a mounted Rack app**, not a Rails controller. Bypasses the full Rails middleware stack; removes `ActionController::Live` as a temptation.
- **Falcon is a deferred v1.1 path** via Rack 3 streaming body. v1 is Puma-only. Unicorn / Pitchfork / Passenger return HTTP 501 from the endpoint with a clear error.
- **Resolutions to v1's three open questions** (§13):
  - Q1 (hijack + send_io vs controller-direct) — in-Puma-worker Streamer wins, question dies.
  - Q2 (archive `msg_id` index) — ship the index.
  - Q3 (watermark off-by-one) — use `MAX(msg_id)` from `q_*`, not `last_value`.

---

## 1. Overview

We are adding an SSE-based pub/sub transport to pgbus that is API-compatible with `turbo_stream_from`. A Rails developer writes `pgbus_stream_from @order` in a view; the page subscribes to a server-sent-events stream; when the application calls `@order.broadcast_replace_to(...)` (the unmodified `Turbo::Broadcastable` API), the resulting `<turbo-stream>` HTML fragment lands in the browser exactly as if it had come over ActionCable. No Redis, no ActionCable, no persistent WebSocket — one HTTP/2 GET per stream.

The differentiator versus turbo-rails is *correctness*. PGMQ assigns each broadcast a monotonic `msg_id`. SSE has `Last-Event-ID`. Together they let us solve rails/rails#52420 (page born stale: the page renders, then the broadcast happens, then the page subscribes — and the broadcast is lost). The helper embeds the current PGMQ watermark at render time; the Streamer honours `Last-Event-ID` on (re)connect; the archive table fills the gap.

**Deployment target for v1**: Puma 6.1 or newer, cluster or single mode. Each Puma worker runs an in-process Streamer (threads, not a forked child) that holds hijacked SSE sockets and a dedicated PG `LISTEN` connection. Multiple Puma workers give you multiple Streamers automatically, and Puma's phased restart provides graceful drain. The only pgbus-side recycling concern is writing a `Puma::Plugin` that cleanly closes registered SSE sockets on `before_worker_shutdown`.

---

## 2. File layout

New files (all paths absolute under `/Users/mhenrixon/Code/mhenrixon/pgbus-2/`):

```
lib/pgbus/streams.rb                                  # Pgbus::Streams module + Pgbus.stream(name) facade
lib/pgbus/streams/envelope.rb                         # SSE wire-format encoding (id/event/data/comment)
lib/pgbus/streams/cursor.rb                           # Last-Event-ID / ?since= parsing + validation
lib/pgbus/streams/signed_name.rb                      # Wraps Turbo.signed_stream_verifier_key
lib/pgbus/streams/turbo_broadcastable.rb              # Patches Turbo::StreamsChannel.broadcast_stream_to
lib/pgbus/streams/retention.rb                        # Per-stream retention config struct + pattern lookup
lib/pgbus/web/stream_app.rb                           # Rack app — SSE endpoint with hijack handoff
lib/pgbus/web/streamer.rb                             # Worker-local singleton (Listener + Dispatcher + Heartbeat)
lib/pgbus/web/streamer/registry.rb                    # Concurrent::Map<stream_name → Set<Connection>>
lib/pgbus/web/streamer/connection.rb                  # One SSE client (io, cursor, per-io mutex, write_nonblock+deadline)
lib/pgbus/web/streamer/listener.rb                    # One PG LISTEN connection, NOTIFY → dispatch_queue
lib/pgbus/web/streamer/dispatcher.rb                  # Consumes dispatch_queue, runs read_after, fans out
lib/pgbus/web/streamer/heartbeat.rb                   # Periodic SSE comment + dead-connection sweep
lib/pgbus/web/streamer/io_writer.rb                   # write_nonblock + deadline, per-io mutex helpers
lib/pgbus/web/streamer/puma_plugin.rb                 # before_worker_shutdown hook (close all sockets)
lib/pgbus/client/read_after.rb                        # Mixin: Client#read_after, #stream_current_msg_id, #stream_oldest_msg_id
lib/pgbus/client/ensure_stream_queue.rb               # Mixin: creates pgmq queue + msg_id archive index
app/helpers/pgbus/streams_helper.rb                   # pgbus_stream_from view helper
app/javascript/pgbus/stream_source_element.js         # <pgbus-stream-source> custom element (ESM)
spec/integration/streams/page_born_stale_spec.rb      # The headline test — real Puma, real hijack
spec/integration/streams/reconnect_spec.rb            # Last-Event-ID replay + dedup
spec/lib/pgbus/web/stream_app_spec.rb                 # Unit: auth, signed name, 501 fallback
spec/lib/pgbus/web/streamer/registry_spec.rb
spec/lib/pgbus/web/streamer/connection_spec.rb
spec/lib/pgbus/web/streamer/io_writer_spec.rb         # write_nonblock semantics, EAGAIN handling, deadline
spec/lib/pgbus/web/streamer/listener_spec.rb          # LISTEN health check + reconnect
spec/lib/pgbus/web/streamer/dispatcher_spec.rb
spec/lib/pgbus/streams/cursor_spec.rb
spec/lib/pgbus/streams/signed_name_spec.rb
spec/lib/pgbus/streams/envelope_spec.rb
spec/lib/pgbus/streams/turbo_broadcastable_spec.rb
spec/lib/pgbus/client/read_after_spec.rb
spec/lib/pgbus/client/ensure_stream_queue_spec.rb
```

Modified files:

```
lib/pgbus/configuration.rb       # add streams_* attrs (~25 lines)
lib/pgbus/client.rb              # include ReadAfter, EnsureStreamQueue mixins
lib/pgbus/engine.rb              # require streams files; conditional Turbo::Broadcastable patch; mount stream_app
config/routes.rb                 # mount stream_app at /pgbus/streams
```

**Files NOT modified** (deliberately, to keep the v2 architecture clean):

```
lib/pgbus/process/supervisor.rb          # Puma owns streamer lifecycle, not Supervisor
lib/pgbus/process/worker.rb              # unchanged
lib/pgbus/process/consumer.rb            # unchanged
lib/pgbus/process/dispatcher.rb          # EXCEPT: adds prune_stream_archives maintenance task (§8)
```

Wait — `Dispatcher` needs one addition for per-stream retention. Adding it back:

```
lib/pgbus/process/dispatcher.rb  # +1 maintenance task: prune_stream_archives (§8)
```

`turbo-rails` remains an **optional runtime dependency**: the `Pgbus::Streams::TurboBroadcastable` patch is only applied when `defined?(Turbo::StreamsChannel)` at engine initialization time (§3). This keeps pgbus usable without turbo-rails.

---

## 3. Public API

View helper (drop-in for `turbo_stream_from`):

```erb
<%# app/views/orders/show.html.erb %>
<%= pgbus_stream_from @order %>
<%= pgbus_stream_from "global_notifications" %>
<%= pgbus_stream_from @account, :alerts %>
```

Behind the scenes the helper renders:

```html
<pgbus-stream-source
  src="/pgbus/streams/eyJfcmFpbHMiOnsiZGF0YS..."   <!-- signed name -->
  since-id="1247"                                    <!-- watermark from MAX(msg_id) at render time -->
  signed-stream-name="eyJfcmFpbHMi..."
  channel="Turbo::StreamsChannel"                    <!-- compat shim, ignored by streamer -->
></pgbus-stream-source>
```

Model concern (unchanged — reuses `Turbo::Broadcastable`):

```ruby
class Order < ApplicationRecord
  broadcasts_to ->(order) { [order.account, :orders] }
end
```

`broadcasts_to` ends up calling `Turbo::StreamsChannel.broadcast_stream_to`, which we monkey-patch in `lib/pgbus/streams/turbo_broadcastable.rb` to call `Pgbus.stream(name).broadcast(content)` instead of `ActionCable.server.broadcast`. The signed-stream-name verification path, the `Turbo::Streams::TagBuilder`, and the entire `Turbo::Broadcastable` API are reused untouched.

Programmatic stream API:

```ruby
Pgbus.stream("global_notifications").broadcast(turbo_stream_html)
Pgbus.stream(@order).broadcast(html, retention: 6.hours)
Pgbus.stream(@order).current_msg_id   # → returns watermark for "since-id"
```

New configuration options on `Pgbus::Configuration`:

```ruby
Pgbus.configure do |c|
  c.streams_enabled                = true               # default: true if defined?(Turbo)
  c.streams_queue_prefix           = "pgbus_stream"     # PGMQ queue is "<prefix>_<digest>"
  c.streams_signed_name_secret     = nil                # nil → reuse Turbo.signed_stream_verifier_key
  c.streams_default_retention      = 5.minutes          # archive TTL for unseen streams
  c.streams_retention              = {}                 # { /^chat_/ => 7.days, "presence" => 30.seconds }
  c.streams_heartbeat_interval     = 15                 # SSE comment ping seconds
  c.streams_max_connections        = 2_000              # per Puma worker cap; overflow → 503
  c.streams_idle_timeout           = 3_600              # 1h: close connections idle this long
  c.streams_listen_health_check_ms = 5_000              # LISTEN keepalive ping interval (worry #2 from v1)
  c.streams_write_deadline_ms      = 5_000              # write_nonblock deadline before drop
  c.streams_falcon_streaming_body  = false              # v1.1 opt-in
end
```

Custom element DOM events (browser):

```js
element.addEventListener("pgbus:open",         (e) => { e.detail.lastEventId })
element.addEventListener("pgbus:replay-start", (e) => { e.detail.fromId, e.detail.toId })
element.addEventListener("pgbus:replay-end",   (e) => {})
element.addEventListener("pgbus:gap-detected", (e) => { e.detail.lastSeenId, e.detail.archiveOldestId })
element.addEventListener("pgbus:close",        (e) => { e.detail.code, e.detail.reason })
```

The element calls `connectStreamSource(this)` from `@hotwired/turbo` on connect (the hardcoded ActionCable channel in turbo-rails has zero protocol coupling — it emits Turbo Stream HTML strings, which is what we deliver).

---

## 4. The wire protocol

### Initial GET

```
GET /pgbus/streams/eyJfcmFpbHMiOnsi...digest HTTP/1.1
Host: example.com
Accept: text/event-stream
Cache-Control: no-cache
Last-Event-ID: 1247         # only on reconnect; from EventSource auto-resume
Cookie: _myapp_session=...; pgbus_stream_auth=...
```

Auth uses cookies because `EventSource` cannot send custom headers. The signed stream name in the URL path is the primary capability token (verified via `Turbo.signed_stream_verifier_key`); the cookie is the user-identity token, used by app-defined authorization.

### SSE response stream

```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache, no-transform
X-Accel-Buffering: no
Connection: keep-alive

retry: 2000

: pgbus stream open msg_id=1247 stream=order_42

id: 1248
event: turbo-stream
data: <turbo-stream action="replace" target="order_42">...</turbo-stream>

id: 1249
event: turbo-stream
data: <turbo-stream action="append" target="messages">...</turbo-stream>

: heartbeat 1700000015

id: 1250
event: turbo-stream
data: <turbo-stream ...>

```

Notes:

- `id:` is the PGMQ `msg_id` as ASCII decimal. EventSource persists this and resends it as `Last-Event-ID` on reconnect.
- `data:` is single-line — the `<turbo-stream>` HTML is rendered with newlines stripped (or each line gets its own `data:` continuation per the SSE spec). **Decision**: strip newlines. Turbo Streams are already flat HTML; stripping `\n` and `\r` is safe and simplifies the parser.
- `: ` lines are SSE comments. `retry: 2000` instructs the browser to back off 2s before reconnect.
- The first thing the Streamer writes after the status line is a `: pgbus stream open ...` comment, which forces proxies to flush the response envelope to the client. Some proxies eat buffered headers until the body starts.

### `signed-stream-name` verification

When the GET arrives:

1. `Pgbus::Streams::SignedName.verify!(token)` — uses `ActiveSupport::MessageVerifier` with `Turbo.signed_stream_verifier_key`. Reuses turbo-rails' verifier key so existing `broadcasts_to` calls Just Work.
2. The verifier returns the *logical* stream name (e.g. `["gid://app/Order/42", "messages"]`). We compute the queue digest from that.
3. If `Turbo` is not loaded the verifier falls back to `Pgbus.configuration.streams_signed_name_secret`.

### `since-id` watermark flow (the critical path)

1. View renders `pgbus_stream_from @order`.
2. Helper calls `Pgbus.stream(@order).current_msg_id` → a single `SELECT COALESCE(MAX(msg_id), 0) FROM pgmq.q_<name>` query. This is an index-only scan on the primary key, microseconds. **Not** `last_value` — see Q3 resolution in §13.
3. Helper renders `<pgbus-stream-source since-id="1247" ...>`.
4. The element on `connectedCallback`:
   - Reads `since-id="1247"` from the attribute.
   - Issues `fetch(src + "?since=1247")` with a `ReadableStream` SSE parser **only on the first connect** (because native `EventSource` can't send `Last-Event-ID` on first connect).
   - On subsequent reconnects uses native `EventSource`, which will resend the highest seen id via `Last-Event-ID` automatically.
5. Streamer reads `since` query param (first connect) or `Last-Event-ID` header (reconnect) and replays from `q_` ∪ `a_` where `msg_id > since`.

### Sentinel events

```
id: 1247
event: pgbus:gap-detected
data: {"lastSeenId":1100,"archiveOldestId":1180}

id: 1247
event: pgbus:shutdown
data: {"reason":"worker_restart"}
```

- `gap-detected` fires when `last_seen_id < archive_oldest_id` — the client has fallen so far behind that retention has expired. The client decides whether to do a full Turbo refresh or surface an error.
- `shutdown` fires from the Puma `before_worker_shutdown` hook. Browsers reconnect immediately; since PGMQ retains messages across the window, any broadcasts published during the gap will replay via `Last-Event-ID`.

---

## 5. The `Client#read_after` method (and siblings)

Lives in `lib/pgbus/client/read_after.rb`, included into `Pgbus::Client`.

```ruby
# Returns Array<Envelope> where Envelope is Data.define(:msg_id, :enqueued_at, :payload, :source).
# Reads non-destructively (no VT update, no row claim) — pure peek.
def read_after(stream_name, after_id:, limit: 500)
  full = config.queue_name(stream_name)          # "pgbus_stream_<digest>"
  sanitized = QueueNameValidator.sanitize!(full)
  rows = synchronized do
    with_raw_connection do |conn|
      conn.exec_params(<<~SQL, [after_id.to_i, limit.to_i])
        (
          SELECT msg_id, enqueued_at, message, 'live'::text AS source
          FROM pgmq.q_#{sanitized}
          WHERE msg_id > $1
          ORDER BY msg_id ASC
          LIMIT $2
        )
        UNION ALL
        (
          SELECT msg_id, enqueued_at, message, 'archive'::text AS source
          FROM pgmq.a_#{sanitized}
          WHERE msg_id > $1
          ORDER BY msg_id ASC
          LIMIT $2
        )
        ORDER BY msg_id ASC
        LIMIT $2
      SQL
    end
  end
  rows.map { |r| Envelope.new(msg_id: r["msg_id"].to_i, ...) }
end

def stream_current_msg_id(stream_name)
  # SELECT COALESCE(MAX(msg_id), 0) FROM pgmq.q_<name>
  # Index-only scan on the msg_id primary key. Used for the "since-id" watermark.
end

def stream_oldest_msg_id(stream_name)
  # SELECT LEAST((SELECT MIN(msg_id) FROM pgmq.q_...), (SELECT MIN(msg_id) FROM pgmq.a_...))
  # Used by the Streamer to decide whether to emit gap-detected.
end
```

Key properties:

- **Non-consuming**. No `FOR UPDATE SKIP LOCKED`, no VT bump. Workers and the Streamer read disjoint paths: workers `read_batch` (claim), Streamer `read_after` (peek).
- **Union of live + archive**. A msg_id may appear in one or the other, never both at a single instant — PGMQ's `archive()` is `DELETE FROM q_ ... INSERT INTO a_` in one transaction (see `pgmq.archive` in `lib/pgbus/pgmq_schema/pgmq_v1.11.0.sql:825-880`). Client-side dedup by `msg_id` in `Connection#enqueue_envelopes` handles edge cases.
- **Ordering**. PGMQ `msg_id` is strictly monotonic per queue (`BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY`, see `pgmq_v1.11.0.sql:1216`), so `ORDER BY msg_id` gives total order.
- **Archive table requires an index** on `msg_id`. The v1 design ignored this. Q2 in §13 resolves it: ship a `CREATE INDEX IF NOT EXISTS pgmq.a_<name>_msg_id_idx ON pgmq.a_<name> (msg_id)` as part of `Client#ensure_stream_queue`. Without this the archive path becomes a seq scan and `read_after` degrades to unacceptable latency once archives grow past a few thousand rows.
- **Consistency**. Read-committed defaults. A broadcast inserted *during* the union read may or may not appear; it doesn't matter — the Streamer's LISTEN will pick it up on the next NOTIFY.

### `Client#ensure_stream_queue`

New mixin at `lib/pgbus/client/ensure_stream_queue.rb`:

```ruby
def ensure_stream_queue(stream_name)
  full = config.queue_name(stream_name)
  ensure_queue(full)   # existing method — creates the PGMQ queue
  synchronized do
    with_raw_connection do |conn|
      # Index on msg_id in the archive table so read_after can use it
      conn.exec(<<~SQL)
        CREATE INDEX IF NOT EXISTS a_#{sanitized}_msg_id_idx
        ON pgmq.a_#{sanitized} (msg_id)
      SQL
    end
  end
end
```

Called from `Pgbus.stream(name).broadcast(...)` on first publish per stream (idempotent). Called from the Streamer on first subscription per stream too — so read-only subscribers with no publisher still get their index.

---

## 6. The Streamer (worker-local, thread-based)

**This is the v2 architectural reframe.** The Streamer is not a separate OS process. It is a singleton object living inside each Puma worker, started lazily on the first SSE connection to that worker.

### Why this works: Puma's hijack releases the thread

The v1 design assumed hijacked connections tie up Puma threads. The research disproved this. Trace from Puma 7.2.0 source:

1. `lib/puma/request.rb:73-74` — `env['rack.hijack'] = client.method(:full_hijack)` is always set.
2. `lib/puma/client.rb:134-139` — `full_hijack` sets `@hijacked = true` and stores the IO. It does not spawn anything.
3. `lib/puma/request.rb:114-115` — after `@app.call(env)` returns, Puma checks `client.hijacked` and returns `:async`.
4. `lib/puma/server.rb:503-506` — `process_client` sets `close_socket = false` and exits.
5. `lib/puma/thread_pool.rb:152-186` — the worker thread loops back to `todo.shift` to pick up the next request.

**After our Rack app calls `env['rack.hijack'].call`, hands the IO to the Streamer, and returns `[-1, {}, []]`, the Puma thread is released.** The hijacked IO is just a `TCPSocket` owned by whoever we handed it to.

The common mistake (and the reason `ActionController::Live` ties up threads): if your code sits in `while true; io.write(...); sleep; end` *inside* `@app.call`, Puma can't release the thread because the call never returns. We do not do this. We hand the IO to the Streamer and return in milliseconds.

**Puma does not track hijacked IOs.** `graceful_shutdown` only waits on `@thread_pool` — this is why we need the `Puma::Plugin` in §6.4 to close our IOs explicitly on `before_worker_shutdown`.

### 6.1 Threading model

Each Puma worker has **one** `Pgbus::Web::Streamer` instance, lazily created on the first SSE connection. It owns:

- **One dedicated `PG::Connection`** running `LISTEN` on every active stream channel. Managed by the `Listener` thread.
- **One dedicated client-side pool** (separate from `Pgbus.client`'s pool) for `read_after` peek queries — size 4, grows to 8 if needed.
- **N hijacked client sockets**, each wrapped in a `Connection` object holding the IO, cursor state, and a per-IO mutex.

Three long-running threads per Streamer:

```
Listener thread (1):
  loop:
    select on @pg_conn (raw libpq)
    on notify: extract stream name from "pgmq.q_<name>.INSERT"
               post WakeMessage(stream: name) to @dispatch_queue
    on timeout (@streams_listen_health_check_ms):
      conn.exec("SELECT 1")   # keepalive; fail → reconnect

Dispatcher thread (1):
  loop:
    msg = @dispatch_queue.pop
    case msg
    when WakeMessage:
      conns = @registry.connections_for(msg.stream)
      next if conns.empty?
      min_seen = conns.map(&:last_msg_id_sent).min
      envelopes = client.read_after(msg.stream, after_id: min_seen, limit: 500)
      conns.each do |c|
        c.enqueue(envelopes.select { |e| e.msg_id > c.last_msg_id_sent })
      end
    when ConnectMessage:
      replay_initial(msg.connection)   # see §6.5 — the 5-step race-free sequence
      @registry.register(msg.connection)
      @listener.ensure_listening(msg.connection.stream_name)
    when DisconnectMessage:
      @registry.unregister(msg.connection)
      @listener.maybe_unlisten(msg.connection.stream_name) if @registry.empty?(msg.connection.stream_name)

Heartbeat thread (1):
  loop:
    sleep(@streams_heartbeat_interval)
    @registry.each_connection do |c|
      c.write_comment("heartbeat #{Time.now.to_i}")   # goes through IoWriter
      @dispatch_queue.post(DisconnectMessage.new(c)) if c.idle_for > @streams_idle_timeout
      @dispatch_queue.post(DisconnectMessage.new(c)) if c.dead?
    end
```

No per-connection writer thread. Writes happen directly from the Dispatcher and Heartbeat threads via `IoWriter`, which uses `write_nonblock` with a deadline and a per-IO mutex. This is the message_bus pattern — simpler than spawning N threads, scales to a few thousand connections per worker.

### 6.2 The `IoWriter` — the failure-mode fix

From the web-server agent (puma#576): a blocking `io.write` on a dead/slow client can deadlock. The fix:

```ruby
# lib/pgbus/web/streamer/io_writer.rb (sketch)
module IoWriter
  # Writes bytes under the connection's mutex, using write_nonblock with a deadline.
  # Returns :ok, :blocked (too slow, drop), :closed (EOF), :error.
  def self.write(connection, bytes, deadline_ms:)
    connection.mutex.synchronize do
      deadline = monotonic_now + (deadline_ms / 1000.0)
      remaining = bytes.bytesize
      offset = 0
      while remaining > 0
        begin
          written = connection.io.write_nonblock(bytes.byteslice(offset, remaining))
          offset += written
          remaining -= written
        rescue IO::WaitWritable
          wait = deadline - monotonic_now
          return :blocked if wait <= 0
          IO.select(nil, [connection.io], nil, wait) || (return :blocked)
        rescue Errno::EPIPE, Errno::ECONNRESET, IOError
          return :closed
        end
      end
      :ok
    end
  end
end
```

**Thread safety**: two threads (Dispatcher + Heartbeat) may write to the same IO concurrently. The per-connection mutex serializes writes. We do NOT hold the mutex during `IO.select` — that would let a slow client block the Heartbeat thread. Actually we do hold it here during select. Reconsider: if a slow client holds its mutex during select for up to `deadline_ms`, the Heartbeat thread will block on `.synchronize` for that connection only; the Dispatcher can still serve every other connection. The blocked heartbeat is bounded by `deadline_ms` (default 5s), which is less than `streams_heartbeat_interval` (default 15s). Acceptable. Document.

### 6.3 The `Registry`

```ruby
# lib/pgbus/web/streamer/registry.rb (sketch)
class Registry
  def initialize
    @mutex = Mutex.new
    @by_stream = Hash.new { |h, k| h[k] = Set.new }   # stream_name → Set<Connection>
    @by_id     = {}                                    # connection_id → Connection
  end

  def register(connection)
  def unregister(connection)
  def connections_for(stream_name)  # returns frozen snapshot
  def each_connection                # yields snapshot
  def streams                        # snapshot list — Listener uses it to compute LISTEN diff
  def empty?(stream_name)
  def size
end
```

Snapshot semantics so iterators can traverse without holding the lock.

### 6.4 `Puma::Plugin` shutdown hook

```ruby
# lib/pgbus/web/streamer/puma_plugin.rb (sketch)
require "puma/plugin"
Puma::Plugin.create do
  def start(launcher)
    launcher.events.on_booted do
      # worker started; Streamer will be lazily created on first connection
    end

    # Before this worker shuts down (phased restart, SIGTERM, SIGUSR2), close all SSE sockets cleanly
    launcher.events.on_stopped do
      Pgbus::Web::Streamer.instance&.shutdown!
    end
  end
end
```

`Streamer#shutdown!`:

1. Stop the Listener thread (close the PG LISTEN connection).
2. For each registered connection: write `event: pgbus:shutdown\ndata: {"reason":"worker_restart"}\n\n`, `io.close`.
3. Stop the Dispatcher thread.
4. Stop the Heartbeat thread.
5. Return.

Bounded by `streams_write_deadline_ms` per connection; if a client is dead we drop it immediately. Total shutdown time for 2000 connections at 5s deadline each is *not* 10000 seconds — the writes are nonblock and only sit on the deadline if the TCP buffer is full. Typical case is milliseconds.

### 6.5 The replay race — 5-step race-free sequence

From the web-server agent's failure mode #7. Between "read the archive for replay" and "subscribe to LISTEN," new events can be missed. Sequence inside `replay_initial(connection)`:

1. **Subscribe first**: `@listener.ensure_listening(stream_name)`, and open a buffer on the Dispatcher keyed by `connection.id`. Any `WakeMessage` that arrives from now on that touches this stream causes the Dispatcher to push into the buffer *in addition to* fanning out to already-registered connections.
2. **Read the archive**: `client.read_after(stream_name, after_id: connection.since_id, limit: 500)`. This is a single union query over live+archive.
3. **Write replayed envelopes** to `connection.io` in order.
4. **Flush the buffer**: pop everything accumulated in step 1's buffer, filter by `msg_id > last_written`, write in order.
5. **Register**: `@registry.register(connection)`. From now on, step 1's buffer is drained and the Dispatcher's normal fanout path serves this connection.

The invariant is: between subscribe (step 1) and register (step 5), no envelope is lost because the Dispatcher is writing both to the existing registry AND to the per-connection buffer. After register, the buffer is empty and normal fanout applies. Dedup by `msg_id > last_written` handles any overlap between steps 2 and 4.

### 6.6 Discovering streams to LISTEN on

The Listener tracks `@listening_to = Set<String>`. On `ensure_listening("pgbus_stream_abc")`:

```sql
LISTEN "pgmq.q_pgbus_stream_abc.INSERT"
```

Channel name from `pgmq_v1.11.0.sql:1634` — `'pgmq.' || TG_TABLE_NAME || '.' || TG_OP`.

`maybe_unlisten` is **lazy with periodic GC**. Streams with zero subscribers are UNLISTENed during `Dispatcher`'s periodic sweep (every 60s), but only if they've been empty for >5 minutes. Avoids thrash on page navigation (subscribe → unsubscribe → subscribe within 100ms is common).

### 6.7 Connection lifetime and recycling

- **No `streamer_max_lifetime`**. Puma's phased restart drives recycling. Time-triggered kill of a connection-holding process is strictly harmful.
- **Idle reaping**: `streams_idle_timeout` (default 1h) closes connections with no writes in that window. Browsers reconnect transparently via EventSource.
- **Overflow handling**: when a Puma worker's Streamer hits `streams_max_connections` (default 2000), new SSE requests to that worker's `StreamApp` return HTTP 503 with `Retry-After: 2`. The user's load balancer (nginx, Puma's own cluster LB) retries against a sibling worker. **If a user runs `workers 1`, 503 means "too many tabs open"; document accordingly.**
- **Phased restart (normal deploy)**: Puma drains old workers one at a time. Each drain fires `on_stopped`, which invokes `Streamer#shutdown!`, which sends `pgbus:shutdown` to each connection and closes. Browsers reconnect to the new worker; `Last-Event-ID` replays anything published during the flip (bounded by per-stream retention). Invisible to the user.
- **Worker crash**: Puma forks a replacement. The LISTEN connection dies with the old worker; nothing to clean up. Clients get TCP RST, EventSource reconnects.

---

## 7. The Rack/SSE endpoint

**Architecture: a mounted Rack app, not a Rails controller.** `lib/pgbus/web/stream_app.rb`.

### Why a Rack app and not a controller

1. Bypasses `ActionDispatch::Cookies`, `ActiveRecord::QueryCache`, `ActionDispatch::Flash`, params body parsing — all of which either misbehave or leak resources on long-lived hijacked connections.
2. Removes `ActionController::Live` as a tempting wrong answer. (Add a CI guard that fails if any pgbus controller `include`s it — the web-server agent specifically flagged this.)
3. Feature-detects server capability at call time: Puma hijack → use hijack; Falcon → return Rack 3 streaming body (v1.1); anything else → HTTP 501 with error.
4. Easy to benchmark independently from Rails.

### Endpoint flow (Puma hijack path)

```ruby
# lib/pgbus/web/stream_app.rb (sketch)
class StreamApp
  def call(env)
    req = Rack::Request.new(env)
    return [404, {}, ["not found"]] unless req.get?
    return [501, {"content-type" => "text/plain"}, ["pgbus streams require Puma 6.1+ or Falcon"]] unless env["rack.hijack?"]

    signed_name = extract_signed_name(req)
    stream_name = SignedName.verify!(signed_name) rescue (return [404, {}, ["bad signed name"]])
    authorize!(env, stream_name)                     rescue (return [403, {}, ["forbidden"]])
    since_id = Cursor.parse(req, env)                # ?since or Last-Event-ID

    # Hijack
    env["rack.hijack"].call
    io = env["rack.hijack_io"]
    io.write("HTTP/1.1 200 OK\r\n")
    io.write("content-type: text/event-stream\r\n")
    io.write("cache-control: no-cache, no-transform\r\n")
    io.write("x-accel-buffering: no\r\n")
    io.write("connection: keep-alive\r\n")
    io.write("\r\n")
    io.write("retry: 2000\n\n")
    io.write(": pgbus stream open since_id=#{since_id} stream=#{stream_name}\n\n")

    # Hand off to the worker-local Streamer
    connection = Pgbus::Web::Streamer::Connection.new(
      id: SecureRandom.hex(8),
      io: io,
      stream_name: stream_name,
      since_id: since_id
    )
    Pgbus::Web::Streamer.instance.register(connection)

    [-1, {}, []]   # Puma async protocol
  rescue => e
    Pgbus.logger.error { "[Pgbus::Streams] stream_app error: #{e.class}: #{e.message}" }
    [500, {"content-type" => "text/plain"}, ["internal error"]]
  end
end
```

### Mounting

```ruby
# lib/pgbus/engine.rb — new initializer
initializer "pgbus.mount_stream_app" do |app|
  if Pgbus.configuration.streams_enabled
    app.routes.append do
      mount Pgbus::Web::StreamApp.new => "/pgbus/streams"
    end
  end
end
```

### Middleware ordering

Mount **before** `Rack::Deflater`, `Rack::ContentLength`, any body-length-assuming middleware, and any logger that expects the body to finish. In practice the `/pgbus/streams` mount at the Rails app level is outside all of these by default, but document it.

### Falcon path (v1.1, opt-in)

When `config.streams_falcon_streaming_body == true` and `env["rack.hijack?"]` is false (Falcon doesn't set it in the same way), return a Rack 3 streaming body:

```ruby
body = ->(stream) {
  Pgbus::Web::Streamer.instance.attach_streaming_body(stream, stream_name, since_id)
}
[200, headers, body]
```

Falcon will schedule this as a fiber. Not built in v1.

---

## 8. Per-stream retention

Existing primitives: `Client#purge_archive(queue_name, older_than:)` (`lib/pgbus/client.rb:224-243`) and `Dispatcher#compact_archives` (`lib/pgbus/process/dispatcher.rb:198-220`) already prune per-queue archives by age.

What we add:

1. **Config**: `c.streams_retention = { /^pgbus_stream_chat_/ => 7.days, "pgbus_stream_presence" => 30.seconds }`. Lookup is by exact queue name first, then regex.
2. **Dispatcher integration**: new method `Dispatcher#prune_stream_archives` runs on the same `ARCHIVE_COMPACTION_INTERVAL` schedule as `compact_archives`. It enumerates `pgmq.meta` for queues with the streams prefix and uses the per-stream retention if set, otherwise `streams_default_retention`.
3. **Why separate from `compact_archives`**: the existing one uses a single global `archive_retention` (default 7 days). Streams need much shorter retention by default (5 minutes) — keeping a 7-day archive of every UI broadcast is a disk-space disaster.
4. **Gap interaction**: when a client's `last_id` is below `MIN(q_)` AND `MIN(a_)`, the Streamer sends `pgbus:gap-detected` (computed via `Client#stream_oldest_msg_id`). The decision happens at replay time, not at retention time.

Defaults:

| Stream pattern | Retention |
|---|---|
| `streams_default_retention` | 5 minutes |
| User-configured per-stream | varies |

5 minutes is enough to absorb the page-born-stale window (~milliseconds) plus reconnect after network blips and phased restarts (~tens of seconds), with comfortable margin. Chat-like streams that need longer history opt in via config.

---

## 9. Page-born-stale fix flow (the headline)

A user opens `GET /orders/42`. The race rails/rails#52420 describes:

| t (ms) | Server | Browser |
|---|---|---|
| 0 | Controller renders `show.html.erb`, which evaluates `pgbus_stream_from @order` | — |
| 0 | Helper calls `Pgbus.stream(@order).current_msg_id` → `SELECT COALESCE(MAX(msg_id), 0) FROM pgmq.q_<name>` → **1247** | — |
| 1 | Helper renders `<pgbus-stream-source since-id="1247" src="...">` | — |
| 5 | Response sent | First byte received |
| 10 | **Order#update fires `broadcast_replace_to`** in another request | — |
| 11 | `Pgbus.stream(name).broadcast(html)` → `send_message` inserts row → msg_id=1248 | — |
| 11 | PG trigger fires `NOTIFY pgmq.q_<name>.INSERT` | — |
| 12 | Every Puma worker's Streamer Listener receives notify; posts WakeMessage to its dispatcher | — |
| 12 | Dispatcher: `connections_for(stream)` is empty on every worker — fanout no-op. **But the message is durably in PGMQ.** | — |
| 50 | — | HTML parses, custom element `connectedCallback` fires |
| 51 | — | Element issues `GET /pgbus/streams/...?since=1247` |
| 60 | Request lands on a Puma worker (any worker — any will do) | — |
| 60 | StreamApp hijacks, instantiates Streamer on first use | — |
| 61 | Streamer Dispatcher: `replay_initial(connection)` (§6.5) | — |
| 61 | Step 1: `ensure_listening(stream)` | — |
| 61 | Step 2: `client.read_after(stream, after_id: 1247, limit: 500)` | — |
| 62 | SQL returns row 1248 (in `q_` table — not yet archived) | — |
| 62 | Step 3: writes `id: 1248\nevent: turbo-stream\ndata: <turbo-stream...>\n\n` | — |
| 62 | Step 4: flush buffer (empty — no concurrent notifies during this 1ms window) | — |
| 62 | Step 5: register connection | — |
| 63 | — | EventSource fires `message`, custom element forwards to Turbo, DOM updates |

Without the watermark, t=10 would have been silently lost. With it, **the gap is closed**. The watermark moves the failure mode from "silent data loss" to "delivered with a few hundred milliseconds of latency."

**Correctness invariant**: the watermark is `MAX(msg_id)` from the `q_` table at render time. Any broadcast with id ≤ watermark was already assigned before the helper returned; any broadcast with id > watermark happens strictly after and will be replayed via `read_after` with `msg_id > since_id`. The v1 `last_value` approach had an unspecified-empty-sequence edge case; `MAX(msg_id)` is unconditional and correct.

---

## 10. The integration test scenario

**Test strategy**: the integration test boots a real Puma 6.1+ server with the StreamApp mounted, publishes real messages through a real PGMQ, and asserts against a real SSE client. No mocks. We need this because the whole point of Architecture A is proving the hijack + thread-release + IO-owned-by-Streamer path works end-to-end under Puma's semantics. A test that calls `StreamApp.new.call(env)` with a fake env would pass while the real system was broken.

File: `spec/integration/streams/page_born_stale_spec.rb`. Pseudocode:

```ruby
RSpec.describe "page-born-stale fix", :integration, :real_puma do
  let!(:puma_server) { PumaTestHarness.boot(rack_app: pgbus_engine, port: :ephemeral) }
  let(:base_url)     { "http://localhost:#{puma_server.port}" }
  let(:stream_name)  { "stream_test_#{SecureRandom.hex(4)}" }

  before do
    boot_real_postgres_and_pgmq
    Pgbus.client.ensure_stream_queue(stream_name)
  end

  after  { puma_server.shutdown }

  it "delivers broadcasts published in the gap between render and connect" do
    stream = Pgbus.stream(stream_name)

    # Step 1: simulate render — capture watermark BEFORE any broadcast
    rendered_since_id = stream.current_msg_id
    signed = sign(stream_name)

    # Step 2: simulate the gap — broadcasts happen BEFORE the client connects
    stream.broadcast(%(<turbo-stream action="replace" target="x">A</turbo-stream>))
    stream.broadcast(%(<turbo-stream action="replace" target="x">B</turbo-stream>))

    # Step 3: simulate the client connecting AFTER the broadcasts — real HTTP, real SSE
    client = SseTestClient.connect(
      url:     "#{base_url}/pgbus/streams/#{signed}?since=#{rendered_since_id}",
      timeout: 5
    )
    received = client.wait_for_events(count: 2, timeout: 5)

    # Assertion 1: both gap-period broadcasts were delivered via the replay path
    expect(received.map(&:data)).to eq([
      %(<turbo-stream action="replace" target="x">A</turbo-stream>),
      %(<turbo-stream action="replace" target="x">B</turbo-stream>)
    ])

    # Assertion 2: ids are monotonic and strictly after the watermark
    expect(received.map(&:id)).to all(be > rendered_since_id)
    expect(received.map(&:id)).to eq(received.map(&:id).sort)

    # Assertion 3: a broadcast AFTER connect also arrives (live NOTIFY path)
    stream.broadcast(%(<turbo-stream action="replace" target="x">C</turbo-stream>))
    received_after = client.wait_for_events(count: 1, timeout: 5)
    expect(received_after.first.data).to include("C")
    expect(received_after.first.id).to be > received.last.id

    # Assertion 4: reconnect with Last-Event-ID gets nothing new (cursor handoff works)
    last_seen = received_after.first.id
    client.close
    client2 = SseTestClient.connect(
      url:     "#{base_url}/pgbus/streams/#{signed}",
      headers: { "Last-Event-ID" => last_seen.to_s },
      timeout: 5
    )
    expect(client2.wait_for_events(count: 1, timeout: 0.5)).to be_empty
  end
end
```

Why each assertion is right:

- **#1** proves the archive/live replay path fires for messages produced before connect — that's the bug.
- **#2** proves we're using the watermark correctly (not replaying everything; not skipping the gap).
- **#3** proves the LISTEN/NOTIFY path works — we didn't accidentally build a replay-only system.
- **#4** proves cursor handoff via `Last-Event-ID` works on reconnect.

### `PumaTestHarness` — what it is

A small helper (`spec/support/puma_test_harness.rb`) that:
1. Starts a `Puma::Server` on an ephemeral port in a background thread.
2. Mounts the given Rack app.
3. Waits for the server to accept connections (TCP poll).
4. Exposes `port` and `shutdown` (calls `server.stop(true)` and joins the thread).

Puma's `Puma::Server.new(rack_app).add_tcp_listener("127.0.0.1", 0).run` is well-documented. This harness is probably 40 lines.

### `SseTestClient` — what it is

A small SSE client (`spec/support/sse_test_client.rb`) that uses `Net::HTTP` in streaming mode, parses `id:` / `event:` / `data:` / comment lines, and exposes `wait_for_events(count:, timeout:)`. We do not use a gem because SSE parsing is ~30 lines and we need full control over the `Last-Event-ID` header on reconnect (which most client gems don't expose). Approximately 60 lines.

### Integration test infrastructure caveats

- `spec/integration/` does not exist today. We create it and add a `:integration` tag that boots PG + PGMQ.
- The existing `pgmq_test_helpers.rb` handles the PG + PGMQ setup — reuse it.
- Puma is added to `pgbus.gemspec` as a **development dependency** (it's already in the Gemfile.lock transitively via Rails, but explicit is better).
- The integration spec runs under `bundle exec rspec spec/integration` — slower than unit tests, so it's gated by the `:integration` tag and excluded from the default `rake` run. CI runs both.

---

## 11. Things I'm worried about (v2)

Resolved from v1:

- ~~v1 worry #1 (rack.hijack + send_io compatibility)~~ — **resolved**. In-process Streamer, no send_io. Puma 6.1+ hijack works; Falcon deferred to v1.1; Unicorn/Pitchfork/Passenger return 501.
- ~~v1 worry #3 (archive msg_id index)~~ — **resolved**. Ship the index via `Client#ensure_stream_queue`. Documented as "pgbus stream queues add an index PGMQ doesn't normally have."
- ~~v1 worry #7 (watermark off-by-one)~~ — **resolved**. Use `MAX(msg_id)` from `q_*`. Unconditional correctness.
- ~~v1 worry #10 (in-flight connections during streamer recycling)~~ — **resolved via Puma**. Phased restart + `before_worker_shutdown` plugin + cursor replay.

Still open or new:

1. **Streamer-LISTEN connection silently dropping**. If the dedicated `PG::Connection` doing LISTEN has its TCP connection killed (NAT timeout, PG restart, network blip), libpq will not surface this until the next NOTIFY tries to arrive. The Listener thread does a `SELECT 1` every `streams_listen_health_check_ms` (default 5s) and reconnects on failure. Needs a unit test that simulates a dropped PG connection via `PG::Connection#reset`.

2. **Duplicate delivery during archive transition**. PGMQ's `archive()` is one transaction — a row is *either* in `q_` or `a_`, not both — but `read_after` runs outside that transaction with read-committed isolation. Client-side dedup in `Connection#enqueue` handles this: write only if `envelope.msg_id > last_msg_id_sent`.

3. **Connection capacity ceiling per Puma worker**. The message_bus pattern (writes from publish-side threads, not per-connection) scales to a few thousand per worker. Beyond that we'd need `nio4r` or Falcon. Default cap `streams_max_connections = 2000`; overflow returns 503. Document: "to serve >2000 streams on one host, either run more Puma workers or adopt Falcon."

4. **`signed-stream-name` reuse of Turbo's verifier**. We reuse `Turbo.signed_stream_verifier_key` so existing `broadcasts_to` calls Just Work. Risk: if turbo-rails ever rotates or namespaces this key, our signed names break. Mitigation: also accept names signed with `Pgbus.configuration.streams_signed_name_secret` if set; recommend setting it explicitly in production.

5. **Cookie-based auth + CSRF**. The stream endpoint is GET-only so CSRF doesn't apply. A malicious page can `<img src="/pgbus/streams/...">` and learn stream existence by timing. Signed stream names are unguessable; existence leaks are harmless. Flagging for completeness.

6. **Middleware ordering when the Rack app is mounted**. If a user puts `Rack::Deflater` ahead of our mount point in their `config.ru` or `application.rb`, hijack may fail. Document: mount `pgbus_engine` (or `/pgbus/streams` specifically) before any body-aware middleware. Add a startup check that logs a warning if `Rails.application.middleware` contains `Rack::Deflater` ahead of the engine mount.

7. **Slow-client write blocking a dispatcher**. Even with `write_nonblock` + deadline, the dispatcher thread spends time per connection. If 500 slow clients on one stream each take 5s to time out, the dispatcher is blocked for up to 500*5 = 2500s. Mitigation: on a per-stream fanout, skip to the next connection if the current one is blocked (schedule the slow one for retry on a side queue); treat blocked writes as "drop connection" after N strikes. Add to the `Dispatcher` spec.

8. **PGMQ archive index DDL outside PGMQ management**. We create `a_<name>_msg_id_idx` that PGMQ's own migrations don't know about. If PGMQ upgrades drop/recreate `a_*` tables, our index vanishes. Mitigation: `ensure_stream_queue` is idempotent (`CREATE INDEX IF NOT EXISTS`) and is called on first publish/subscribe per stream, so the index gets recreated automatically after any PGMQ schema change that recreates the table. Document.

9. **The `current_msg_id` query on every render**. `SELECT COALESCE(MAX(msg_id), 0) FROM pgmq.q_<name>` is an index-only scan, microseconds. But we do it on every page render. For pages with 5 streams that's 5 queries. **Mitigation**: cache in `RequestStore` or a thread-local for the duration of a Rack request.

10. **The Zeitwerk `PGMQ` pre-definition gotcha**. `lib/pgbus/client.rb:17-20` pre-defines `PGMQ` as an empty module to work around a Zeitwerk eager_load NameError. The Streamer must touch `Pgbus.client` (or otherwise trigger pgbus autoload) before any code path that uses `PGMQ::Client` directly. Call `Pgbus.client.list_queues` once at Streamer boot.

11. **Embedded vs extension PGMQ schema**. `pgmq_schema_mode: :auto` may install either path. Only the embedded `pgmq_v1.11.0.sql` has been inspected. At Streamer boot, sanity check: the trigger `pgmq.notify_queue_listeners` exists and creates channels of the expected format. Log loud error and refuse to start the Streamer if not.

12. **`Turbo::Broadcastable` patch loading order**. We need `Turbo::StreamsChannel` to exist before we patch it. Engine initializer runs via `ActiveSupport.on_load(:after_initialize)` and checks `defined?(Turbo::StreamsChannel)` before patching. If turbo-rails is not loaded, the patch is a no-op and pgbus runs fine.

13. **`ActionController::Live` CI guard**. Add a `rake pgbus:lint_no_live` task (or a rubocop rule) that fails if any file under `app/controllers/pgbus/` or `lib/pgbus/web/` `include`s `ActionController::Live`. Web-server agent's explicit recommendation.

14. **`ulimit -n` in production**. 2000 connections per worker × 4 workers = 8000 FDs just for SSE, plus whatever else the app needs. Add a startup warning if `Process.getrlimit(:NOFILE)[0] < (workers * streams_max_connections * 2)`. One-line check, big operational win.

15. **Puma::Plugin discovery**. Puma plugins must be in a `puma/plugin/` path and named to match. The plugin needs to be in `lib/puma/plugin/pgbus_streams.rb` (not under `lib/pgbus/`) so Puma can `require "puma/plugin/pgbus_streams"` from user `puma.rb` config, OR we can auto-register at engine boot. **Decision**: require users to add `plugin :pgbus_streams` to their `puma.rb`. Document clearly. Auto-registration is too magical and breaks when Puma isn't in use.

16. **Integration test flakiness risk**. Booting Puma on an ephemeral port, connecting via SSE, racing against the dispatcher — lots of timing. Use deterministic `wait_until` helpers (poll with 50ms intervals, 5s timeout) rather than `sleep`. Document in the harness.

17. **The `write_nonblock` + held mutex concern**. §6.2 acknowledges holding the per-connection mutex during `IO.select` means a slow client blocks any other thread that tries to write to that same connection, bounded by `streams_write_deadline_ms` (5s). In practice the Dispatcher and Heartbeat are the only two writers per connection; contention is rare. If it becomes a problem, we can buffer writes into a per-connection `Queue` and have a separate writer path — but that's a v1.2 problem.

---

## 12. What's explicitly OUT of scope for this phase

Explicitly **not** building in steps 1+2+3:

- **Transactional broadcasts as default** (roadmap step 4). `Order.transaction { @order.update! }` enqueueing the broadcast atomically with the row update — needs outbox integration. Users get manual control via `Pgbus.client.transaction` for now.
- **`broadcasts_with_replay`** (step 5). Chat-history-as-stream where new connects replay from beginning of retention. Architecturally trivial (`read_after(.., after_id: 0)`) but needs UX design.
- **Server-side audience filtering** (step 6). Out.
- **Optimistic broadcasts** (step 7). Out.
- **Connection lifecycle DOM events beyond what's in §3** (step 8). We ship `open`, `replay-start`, `replay-end`, `gap-detected`, `close`. No `pgbus:replay-progress`, `pgbus:rate-limited`, etc.
- **Presence as a stream** (step 9). Out.
- **WebSocket transport**. SSE only.
- **Falcon code path**. Rack 3 streaming body integration is v1.1. v1 is Puma 6.1+ only.
- **Unicorn / Pitchfork / Passenger support**. Return HTTP 501 with a clear message pointing to docs.
- **`Pgbus.streams_authorize` plug-in DSL**. v1 authorization: "if you have the signed stream name, you're authorized." Apps override `StreamApp#authorize!` via reopen.
- **Dashboard integration**. No "Streams" page in this phase.
- **`broadcasts_refreshes` debouncing**. Falls through unchanged to `Turbo::Streams::BroadcastStreamJob`, which routes through pgbus-as-ActiveJob-adapter and ends up calling our patched broadcast path. No special handling needed.
- **HTTP/2 requirement docs**. Mention in README in a follow-up PR.

---

## 13. Resolutions to v1's open questions

### Q1 — Hijack + `Socket.send_io` vs simpler controller-streams-directly?

**Resolved: in-Puma-worker Streamer, no cross-process FD passing, no separate Streamer process.** The web-server agent's Puma deep-dive confirmed that hijack releases the Puma worker thread as long as the Rack code returns promptly after hijack. This collapses the "separate process to decouple thread-holding" justification — we don't need to decouple because Puma releases the thread anyway. The v1 design's `Socket.send_io` complexity was solving a problem that doesn't exist.

One Streamer per Puma worker. Multiple Puma workers = multiple Streamers = multiple PG LISTEN connections. At 4 Puma workers that's 4 LISTEN connections on the database — totally acceptable. Real-world precedents (message_bus, AnyCable's JS SSE, Discourse) all do it this way.

### Q2 — `msg_id` index on PGMQ archive tables for streams?

**Resolved: ship the index.** Reasoning: the entire value prop of `read_after` is fast replay from `WHERE msg_id > $1`. With default 5-minute retention the archive is small and the seq scan is cheap — but the moment a user enables `streams_retention => 7.days` for chat, performance falls off a cliff. The index is cheap to create, idempotent to re-apply, and scoped to stream queues only via `Client#ensure_stream_queue`. Documented as "pgbus stream queues add an index PGMQ doesn't manage."

### Q3 — Watermark: `last_value` vs `MAX(msg_id)`?

**Resolved: use `SELECT COALESCE(MAX(msg_id), 0) FROM pgmq.q_<name>`.** Reasoning:
- `last_value` of an unused sequence is unspecified per PG docs — a real correctness risk.
- The v1 design had a documented off-by-one caveat about "broadcasts during controller render" that users would have to remember. `MAX(msg_id)` removes the caveat.
- Cost concern (heap query) is overblown — `q_*.msg_id` is the primary key, so `MAX(msg_id)` is an index-only scan returning in microseconds.
- With per-request caching (§11 #9), we pay this cost once per page render regardless of how many `pgbus_stream_from` helpers the page has.

---

## Appendix: decisions locked before Phase 1

- Streamer is a singleton per Puma worker, 3 threads (Listener / Dispatcher / Heartbeat), no per-connection writer threads.
- `IoWriter` uses `write_nonblock` + deadline + per-IO mutex (message_bus pattern).
- 5-step race-free replay sequence (subscribe → read → write → flush buffer → register).
- `Puma::Plugin` installed by the user via `plugin :pgbus_streams` in their `puma.rb`.
- Endpoint is a mounted Rack app, not a Rails controller. `ActionController::Live` is CI-guarded against.
- v1 supports Puma 6.1+ only. Falcon in v1.1. Unicorn / Pitchfork / Passenger return 501.
- Integration test boots a real Puma server via `PumaTestHarness`. `SseTestClient` is a local helper.
- Multiple streamers "day 1" requirement (recycling agent) is satisfied automatically by Puma cluster mode; single-worker users get a documented warning.
- Phase 1 TDD order: `Cursor`, `SignedName`, `Envelope`, `ReadAfter`, `EnsureStreamQueue`, `Registry`, `Connection`, `IoWriter` (pure units first), then `Listener`, `Dispatcher`, `Heartbeat`, `Streamer` (thread coordination), then `StreamApp` + Puma plugin + integration test.
