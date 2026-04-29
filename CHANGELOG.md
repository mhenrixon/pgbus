## [Unreleased]

### Breaking Changes

- **Queue names must be alphanumeric and underscores only.** Queue names containing dashes (e.g., `my-app-queue`) will now raise `ArgumentError`. Rename to underscored form (e.g., `my_app_queue`) before upgrading. This restriction prevents SQL injection via PGMQ queue identifiers, which are interpolated into table names and cannot be parameterized.

### Security

- Add `bundler-audit` to CI for dependency vulnerability scanning
- Add `QueueNameValidator` to enforce strict queue name validation (alphanumeric + underscores, 61 char max)
- Add `config.allowed_global_id_models` to restrict which models can be deserialized from event payloads
- Add security headers to dashboard (X-Frame-Options, X-Content-Type-Options, Referrer-Policy, Permissions-Policy)
- Warn when dashboard `web_auth` is unconfigured
- Add `globalid` as an explicit runtime dependency (was used but only transitively available via activejob)

### Fixed

- **EventBus handler invocations now run inside `Rails.application.executor` (or `.reloader` in dev).** `Pgbus::EventBus::Handler#process` previously ran outside any Rails executor wrapper, so `claim_idempotency?`'s `ProcessedEvent.insert` (and any `handle` body that touches AR) leased a connection that was never returned to the pool. With a small AR pool (Rails dev default with `RAILS_MAX_THREADS=3` is 6 connections), a handful of consumed events leaked the entire pool. The next request that triggers `clear_reloadable_connections!` (any reloadable change in dev) hung in `with_exclusively_acquired_all_connections`, surfacing as a confusing `Rack::Timeout` with a trace ending in `MonitorMixin#wait_for_cond`. Mirrors the executor-wrap pattern already used by `Pgbus::ActiveJob::Executor#execute_job`. No-op when Rails isn't loaded.
- **Defensive retry on stale pooled pgmq connections in the enqueue path.** `Pgbus::Client#send_message`, `#send_batch`, and `#publish_to_topic` now retry once when `@pgmq.produce*` raises `PGMQ::Errors::ConnectionError` with a message indicating the pooled `PG::Connection` was killed beneath pgmq-ruby — typically by PgBouncer hitting `server_idle_timeout` / `client_idle_timeout`, an admin disconnect, or a TCP RST. Observed in production as `PQsocket() can't get socket descriptor` on the first produce following an idle window. pgmq-ruby's `auto_reconnect` recovers on the *next* pool checkout, so a single retry is sufficient; non-stale errors (pool timeout, misconfiguration, unreachable database) still propagate unchanged. Upstream pgmq-ruby fix for the underlying misclassification is in-flight at mensfeld/pgmq-ruby#94.

## [0.5.1] - 2026-04-08

### Fixed

- **Capsule DSL: anonymous duplicate capsules are now allowed.** Configurations like `c.workers = "*: 3; *: 3; *: 3; *: 3; *: 3"` (the legacy YAML pattern of 5 forks × 3 threads, all reading every queue) were rejected at boot in 0.5.0 with `Pgbus::Configuration::CapsuleDSL::ParseError: wildcard '*' appears in two capsules`. PGMQ tolerates multiple processes reading the same queue natively (`FOR UPDATE SKIP LOCKED`), and this is the canonical way to scale CPU parallelism across forks, so the rejection was wrong.

  The fix introduces a "named vs anonymous" distinction:

  - The string DSL parser is now purely syntactic — it no longer enforces overlap rules.
  - `Pgbus::Configuration#workers=` auto-assigns `:name` only to capsules whose first queue would yield a *unique* name AND is not the bare wildcard. Wildcards stay anonymous; collision-prone first-queues stay anonymous.
  - `Pgbus::Configuration#validate_no_queue_overlap!` (called by `c.capsule :name, ...`) now only checks against existing **named** capsules. Anonymous capsules can overlap freely with each other and with named capsules.
  - Net result: `"*: 3; *: 3; *: 3"` produces 3 anonymous capsules (3 forks), `"critical: 5; default: 10"` produces 2 named capsules (CLI `--capsule critical` still works), and named-vs-named overlap is still rejected as before.

  No changes required to user configuration — legacy YAML patterns and the modern DSL both work as documented.

## [0.1.0] - 2026-03-30

- Initial release
