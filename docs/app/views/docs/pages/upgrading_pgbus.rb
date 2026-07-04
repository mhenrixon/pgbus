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
        Every pgbus upgrade — patch, minor, or major — follows the same six-step
        procedure: update the gem, review and apply the migration generator,
        migrate the database, check the vendored PGMQ schema, deploy, then verify
        with `pgbus doctor`. The steps below are generic; per-version sections list
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
    DocsUI::Section("0.9.8 → 1.0.0", description: "The 1.0 API freeze: error hierarchy, config renames, and dead-surface removal.") do
      DocsUI::Callout(:warning, title: "1.0.0 not yet released") do
        plain "Everything below is "
        strong { "implemented and lands in 1.0.0" }
        plain " — the unified error hierarchy (issue "
        a(href: "https://github.com/mhenrixon/pgbus/issues/282", class: "link") { "#282" }
        plain ") and the config renames, dead-surface removals, and new shortcuts (issue "
        a(href: "https://github.com/mhenrixon/pgbus/issues/283", class: "link") { "#283" }
        plain "). It describes real behavior on the unreleased 1.0 line; only the release itself hasn't happened yet."
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

        ### Breaking: unified error hierarchy (#282)

        Every operational error pgbus raises now descends from `Pgbus::Error`, so
        `rescue Pgbus::Error` catches them all. Four call sites that previously
        raised bare stdlib errors changed:
      MD
      DocsUI::Table(
        [ "Raised before", "Raises now", "Where" ],
        [
          [ [ :code, "ArgumentError" ], [ :code, "Pgbus::ConfigurationError" ], "Configuration#validate! and its setters" ],
          [ [ :code, "RuntimeError" ], [ :code, "Pgbus::ExecutionPoolError" ], [ :code, "AsyncPool" ] ],
          [ [ :code, "RuntimeError" ], [ :code, "Pgbus::EnqueueError" ], [ :md, "`ActiveJob` adapter (`perform_all_later` msg_id mismatch)" ] ],
          [ [ :code, "ArgumentError" ], [ :code, "Pgbus::SerializationError" ], [ :md, "`Serializer#locate_global_id`" ] ]
        ]
      )
      md <<~'MD'
        Four error classes that bypassed `Pgbus::Error` entirely
        (`PgmqSchema::VersionNotFoundError`, `Streams::SignedName::InvalidSignedName`
        and `MissingSecret`, `Generators::ConfigConverter::Error`) are now
        re-parented underneath it. Three classes that reject a malformed *argument
        shape* — `CapsuleDSL::ParseError`, `Streams::Cursor::InvalidCursor`,
        `Streams::StreamNameTooLong` — deliberately stay `ArgumentError`
        subclasses, since that's what `ArgumentError` means. The policy
        ("argument-shape errors are `ArgumentError`, operational errors are
        `Pgbus::Error`") is documented at the top of `lib/pgbus.rb`.

        **The one breaking change for existing code:** if you `rescue ArgumentError`
        around `Pgbus.configure` or a boot-time config read, that rescue no longer
        catches config errors — `Configuration#validate!` and its setters now raise
        `Pgbus::ConfigurationError`, which is **not** an `ArgumentError`. Switch to:
      MD
      DocsUI::Code(<<~'RUBY', filename: "config/initializers/pgbus.rb")
        begin
          Pgbus.configure { |c| c.visibility_timeout = 0 }
        rescue Pgbus::Error => e   # was: rescue ArgumentError
          Rails.logger.error("pgbus config invalid: #{e.message}")
          raise
        end
      RUBY
      md <<~'MD'
        Error *messages* are unchanged, so any `rescue ... => e` that only reads
        `e.message` keeps working. Only the rescued *class* changed.

        ### Config renames and dead-surface removal (#283)

        Renames ship as a **deprecated alias** in 1.0.0 — the old name still
        works but logs a warning once — with removal in a future 2.0. Nothing
        breaks *at* 1.0.0 except surface confirmed to have zero real-world
        callers (verified in the API-freeze audit):
      MD
      DocsUI::Table(
        [ "Old", "1.0.0", "Path" ],
        [
          [ [ :code, "skip_recurring" ], [ :md, "Renamed to `recurring_enabled` (positive polarity — `true` means run)." ], "Old name aliases (inverting the boolean) and warns once; removed in 2.0." ],
          [ [ :code, "dashboard_filter_parameters" ], [ :md, "Renamed to `web_filter_parameters` (unify on the `web_` prefix)." ], "Old name aliases and warns once; removed in 2.0." ],
          [ [ :code, "dashboard_filter_sensitive" ], [ :md, "Renamed to `web_filter_sensitive`." ], "Old name aliases and warns once; removed in 2.0." ],
          [ [ :code, "recurring_tasks_file" ], [ :md, "Deprecated in favor of `recurring_tasks_files` (plural)." ], "Setting both now warns once (the singular was silently ignored before); a lone singular still works." ],
          [ [ :code, "lock_ttl:" ], [ :md, "Removed from `ensures_uniqueness` — validated but never read by anything." ], [ :md, "Passing it raises `ArgumentError` naming the removal and this page." ] ],
          [ [ :code, "pgbus:add_job_locks" ], [ :md, "Generator removed; `Pgbus::JobLock` model removed (zero references)." ], [ :md, "No replacement — new installs use `pgbus:add_uniqueness_keys`; `pgbus:migrate_job_locks` still retires the legacy table." ] ],
          [ [ :code, "with_pgbus_durable" ], [ :md, "Removed (internal streams helper, zero callers)." ], [ :md, "Use `with_pgbus_broadcast_opts(durable:)`." ] ]
        ]
      )
      md <<~'MD'
        **New in 1.0.0:**

        - `Pgbus.publish` / `Pgbus.publish_later` — top-level shortcuts for
          `Pgbus::EventBus::Publisher.publish` / `.publish_later`, symmetric with
          `Pgbus.stream`. The long form still works.
        - `config.drain_timeout` (default 30s) replaces the hardcoded
          `Worker::DRAIN_TIMEOUT` constant — raise it if your jobs legitimately
          run longer than the graceful-shutdown window.
        - `pgbus doctor` now warns when `allowed_global_id_models` is `nil`
          (allow-all) in production. The default is unchanged for upgrade
          continuity, but set an explicit allowlist — it is security-relevant.

        `log_format=` no longer overwrites a custom logger's formatter.
        `streams_presence_*`, `group_mode`, and `streams_falcon_streaming_body`
        are marked **experimental** and are exempt from the 1.0 stability
        promise.
      MD
    end
  end
end
