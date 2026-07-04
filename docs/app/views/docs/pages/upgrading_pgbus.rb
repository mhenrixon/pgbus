# frozen_string_literal: true

# The version-to-version upgrade guide: the standard procedure every hop
# follows, then per-version sections (newest first) carrying only the deltas.
class Views::Docs::Pages::UpgradingPgbus < DocsUI::Page
  title "Upgrading pgbus"
  eyebrow "Migrate"

  def lead = "The standard upgrade procedure, plus what changes on each hop from 0.9.7 to 1.0.0."

  def content
    overview
    standard_procedure
    v098
    v100_stub
  end

  private

  def overview
    DocsUI::Section("Overview") do
      md <<~'MD'
        Every pgbus upgrade — patch, minor, or major — follows the same five-step
        procedure: update the gem, run the migration generator, migrate the
        database, check the vendored PGMQ schema, deploy, then verify with
        `pgbus doctor`. The steps below are generic; per-version sections list
        only what's different for that hop.

        Work through the sections **oldest first** if you're behind by more than
        one release — each section assumes the previous one is done.
      MD
    end
  end

  def standard_procedure
    DocsUI::Section("The standard upgrade procedure", description: "Every version, every hop.") do
      md <<~'MD'
        1. **Update the gem**
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        bundle update pgbus
      SHELL
      md <<~'MD'
        2. **Review the upgrade plan** — `pgbus:update` inspects your live
           database and reports exactly which migrations are missing; `--dry-run`
           prints the plan without creating any files.
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:update --dry-run
      SHELL
      md <<~'MD'
        3. **Apply it** — drop `--dry-run` to create the migration files (and
           convert a legacy `config/pgbus.yml` to a Ruby initializer, if one still
           exists). The generator auto-detects a [separate database](/docs/separate-database)
           from `Pgbus.configuration.connects_to` or by scanning your initializer /
           `config/application.rb` — you don't need to pass `--database=pgbus`
           yourself unless auto-detection can't find it.
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:update
      SHELL
      md <<~'MD'
        4. **Migrate the database** — use the `:pgbus` variant if you run pgbus on
           a [separate database](/docs/separate-database).
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails db:migrate            # single database
        rails db:migrate:pgbus      # separate-database install
      SHELL
      md <<~'MD'
        5. **Check the vendored PGMQ schema** — `pgbus:update` only handles
           pgbus's own tables; PGMQ's internal schema (the `pgmq.*` functions and
           types) is versioned separately and upgraded on its own generator.
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rake pgbus:pgmq:status
      SHELL
      md <<~'MD'
        If it reports an update available, generate and run the upgrade
        migration:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:upgrade_pgmq
        rails db:migrate
      SHELL
      md <<~'MD'
        6. **Deploy**, then **verify**:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        bundle exec pgbus doctor
      SHELL
      md <<~'MD'
        `doctor` runs six checks — configuration validity, database connectivity,
        PGMQ schema version, queue existence, LISTEN/NOTIFY liveness, and process
        liveness — and exits non-zero on any failure, so it's safe to wire into a
        deploy gate or a post-deploy CI job.
      MD
      DocsUI::Callout(:note) do
        plain "Rolling deploys are safe. Pgbus's heartbeat and process metadata "
        plain "fields are additive-only across versions, so an old-version worker "
        plain "and a new-version worker can coexist during a rolling deploy "
        plain "without corrupting each other's heartbeat rows. Restart your "
        plain "supervisors "
        strong { "after" }
        plain " the web tier has migrated, so no process reads a schema column "
        plain "that doesn't exist yet."
      end
    end
  end

  def v098
    DocsUI::Section("0.9.x → 0.9.8", description: "Two behavior changes, one PGMQ schema bump.") do
      md <<~'MD'
        ### Breaking: queue names must be alphanumeric + underscores

        Queue names containing dashes (`my-app-queue`) now raise `ArgumentError`
        at boot. This closes a SQL-injection surface — PGMQ queue identifiers are
        interpolated into table names and can't be parameterized — but it means a
        dashed queue name that worked on 0.9.7 will crash on 0.9.8.

        **Rename any dashed queue names to underscored form *before* upgrading**
        (`my-app-queue` → `my_app_queue`), in both your `Pgbus.configure` block
        and any code that references the queue name directly. There is no
        automatic migration for this — a queue is just a Postgres table name, so
        renaming means creating the new queue and draining the old one.

        ### Breaking: configuration is now validated eagerly at boot

        `Pgbus.configure` now calls `Configuration#validate!` automatically after
        your block runs (and `ConfigLoader.apply` does the same for
        `config/pgbus.yml`). An invalid value — `visibility_timeout = 0`, for
        example — now raises `ArgumentError` at boot instead of surfacing later,
        far from the misconfiguration, the first time a worker touches that
        setting.

        If you rely on a config that is transiently invalid between multiple
        sequential `configure` blocks, opt out with:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.eager_validation = false
        end
      RUBY
      md <<~'MD'
        ### PGMQ vendored schema: 1.11.0 → 1.11.1

        This hop moves the vendored PGMQ schema forward one patch version — a
        concrete example of step 5 in the [standard procedure](#the-standard-upgrade-procedure)
        above. Run `rake pgbus:pgmq:status` after updating the gem; it will
        report `installed 1.11.0, vendored 1.11.1` and tell you to run:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:upgrade_pgmq
        rails db:migrate
      SHELL
      md <<~'MD'
        ### New in 0.9.8 (all opt-in)

        None of the following change existing behavior — each is a new,
        off-by-default capability:
      MD
      DocsUI::Table(
        [ "Feature", "What it adds" ],
        [
          [ [ :md, "[Observability](/docs/observability)" ], [ :md, "`metrics_backend` — Prometheus/StatsD metrics without hand-writing subscribers." ] ],
          [ [ :md, "[Running workers](/docs/running-workers)" ], [ :md, "`health_port` / HTTP `/livez` and `/readyz` endpoints for orchestrators." ] ],
          [ [ :code, "pgbus dlq" ], "CLI dead-letter management (list/show/retry/purge) without the dashboard." ],
          [ [ :code, "pgbus doctor" ], "The single preflight command this guide uses to verify every upgrade." ]
        ]
      )
    end
  end

  def v100_stub
    DocsUI::Section("0.9.8 → 1.0.0", description: "Forward-looking — this section is a stub.") do
      DocsUI::Callout(:warning, title: "Stub section") do
        plain "1.0.0 hasn't shipped yet. The renames and removals below are "
        plain "tracked by the two API-freeze issues that own this scope — "
        a(href: "https://github.com/mhenrixon/pgbus/issues/282", class: "link") { "#282" }
        plain " (error hierarchy) and "
        a(href: "https://github.com/mhenrixon/pgbus/issues/283", class: "link") { "#283" }
        plain " (config renames + dead-surface removal) — and "
        strong { "must be updated by those issues as they land" }
        plain ". Treat everything here as a plan, not a shipped behavior."
      end
      md <<~'MD'
        ### The 1.0.0 commitment

        1.0.0 marks pgbus's semver commitment: after 1.0.0, a breaking change to
        any documented public API bumps the major version. The 0.x series has
        made breaking changes in minor releases (see the 0.9.8 section above);
        1.0.0 is where that stops. Everything below is surface the two
        API-freeze issues identified as needing to change *before* that
        commitment takes effect — either because it's a genuine correctness fix
        (the error hierarchy) or because it's free to rename now and expensive to
        rename after (config keys, dead code).

        ### Planned: unified error hierarchy (tracked by #282)

        Today, `Pgbus::Error` exists with well-formed subclasses, but several
        call sites still raise bare stdlib errors instead of a `Pgbus::Error`
        descendant. 1.0.0 is expected to close that gap:
      MD
      DocsUI::Table(
        [ "Raises today", "Becomes in 1.0.0", "Where" ],
        [
          [ [ :code, "ArgumentError" ], [ :code, "Pgbus::ConfigurationError" ], "Configuration#validate! and its setters" ],
          [ [ :code, "RuntimeError" ], [ :code, "Pgbus::ExecutionPoolError" ], [ :code, "AsyncPool" ] ],
          [ [ :code, "RuntimeError" ], [ :code, "Pgbus::EnqueueError" ], [ :md, "`ActiveJob` adapter (`perform_all_later` msg_id mismatch)" ] ],
          [ [ :code, "ArgumentError" ], [ :code, "Pgbus::SerializationError" ], [ :md, "`Serializer#locate_global_id`" ] ]
        ]
      )
      md <<~'MD'
        A handful of error classes that bypass `Pgbus::Error` entirely today
        (`PgmqSchema::VersionNotFoundError`, `Streams::SignedName::InvalidSignedName`,
        `Generators::ConfigConverter::Error`, and others) are also expected to be
        re-parented underneath it. Classes that reject a malformed *argument
        shape* — `CapsuleDSL::ParseError`, `Streams::Cursor::InvalidCursor`,
        `Streams::StreamNameTooLong` — are expected to stay `ArgumentError`
        subclasses by design, since that's what `ArgumentError` means.

        **Deprecation path:** if you `rescue ArgumentError` around
        `Pgbus.configure` or a job's config access today, that rescue will stop
        catching config errors once this lands — switch to
        `rescue Pgbus::Error` (which will catch both the old and new shape
        during any transition window #282 defines).

        ### Planned: config renames and dead-surface removal (tracked by #283)

        Renames ship as a deprecated alias in 1.0.0 (old name still works, logs a
        warning once) with removal in a future 2.0 — nothing breaks *at* 1.0.0
        except surface confirmed to have zero real-world callers:
      MD
      DocsUI::Table(
        [ "Today", "Planned 1.0.0 change", "Deprecation path" ],
        [
          [ [ :code, "skip_recurring" ], [ :md, "Renamed to `recurring_enabled` (positive polarity)." ], "Old name aliases to the new one and warns once; removed in 2.0." ],
          [ [ :code, "dashboard_filter_parameters" ], [ :md, "Renamed to `web_filter_parameters` (unify on the `web_` prefix)." ], "Old name aliases and warns once; removed in 2.0." ],
          [ [ :code, "dashboard_filter_sensitive" ], [ :md, "Renamed to `web_filter_sensitive`." ], "Old name aliases and warns once; removed in 2.0." ],
          [ [ :code, "lock_ttl:" ], [ :md, "Removed from `ensures_uniqueness` — validated but never read by anything." ], [ :md, "Passing it raises `ArgumentError` naming the removal and this page." ] ],
          [ [ :code, "pgbus:add_job_locks" ], [ :md, "Generator removed; `Pgbus::JobLock` model removed (zero references today)." ], "No replacement — remove any reference before upgrading." ]
        ]
      )
      md <<~'MD'
        Also planned: `Pgbus.publish` / `Pgbus.publish_later` top-level
        shortcuts (symmetric with `Pgbus.stream`), and a `config.drain_timeout`
        replacing the `Worker::DRAIN_TIMEOUT = 30` constant.

        Watch this section for the final shape — it will be rewritten with
        exact before/after code and a checked-off deprecation timeline once
        #282 and #283 merge.
      MD
    end
  end
end
