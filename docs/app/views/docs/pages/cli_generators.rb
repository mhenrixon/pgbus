# frozen_string_literal: true

# Reference for the pgbus CLI and the Rails generators — what each command and
# generator does, and when you need it.
class Views::Docs::Pages::CliGenerators < DocsUI::Page
  title "CLI & generators"
  eyebrow "Reference"

  def lead = "The pgbus command-line interface and every Rails generator, at a glance."

  def content
    cli
    cli_flags
    core_generators
    feature_generators
    tuning_generators
  end

  private

  def cli
    DocsUI::Section("The CLI") do
      DocsUI::Table(
        [ "Command", "Does" ],
        [
          [ [ :code, "pgbus start" ], "Boot the supervisor (workers, dispatcher, scheduler, consumers)." ],
          [ [ :code, "pgbus status" ], "Show running processes." ],
          [ [ :code, "pgbus queues" ], "List queues with depth and metrics." ],
          [ [ :code, "pgbus version" ], "Print the version." ],
          [ [ :code, "pgbus help" ], "Show help." ]
        ]
      )
    end
  end

  def cli_flags
    DocsUI::Section("start flags", description: "Split roles and capsules across processes.") do
      DocsUI::Table(
        [ "Flag", "Does" ],
        [
          [ [ :code, "--workers-only" ], "Run only worker processes." ],
          [ [ :code, "--scheduler-only" ], "Run only the recurring-task scheduler." ],
          [ [ :code, "--dispatcher-only" ], "Run only the maintenance dispatcher." ],
          [ [ :code, "--capsule NAME" ], "Boot a single named capsule." ],
          [ [ :code, "--execution-mode async" ], "Run jobs as fibers instead of threads." ]
        ]
      )
      DocsUI::Callout(:note) do
        plain "The role flags are mutually exclusive, and the auto-tuned "
        code { "pool_size" }
        plain " follows the role — a scheduler-only process opens only the connections it needs. See "
        a(href: "/docs/running-workers", class: "link") { "Running workers" }
        plain "."
      end
    end
  end

  def core_generators
    DocsUI::Section("Core generators") do
      md <<~'MD'
        Every generator accepts `--database=pgbus` to route its migration to
        `db/pgbus_migrate/` for a [separate database](/docs/separate-database).
      MD
      DocsUI::Table(
        [ "Generator", "Does" ],
        [
          [ [ :code, "pgbus:install" ], "Full setup — the metadata migrations, the batches table, and a starter initializer." ],
          [ [ :code, "pgbus:update" ], "Convert a legacy YAML config and add any missing migrations, detecting a separate DB automatically." ],
          [ [ :code, "pgbus:upgrade_pgmq" ], "Upgrade the embedded PGMQ schema to the latest vendored version." ]
        ]
      )
    end
  end

  def feature_generators
    DocsUI::Section("Feature generators", description: "Add the table for an optional feature.") do
      DocsUI::Table(
        [ "Generator", "Adds" ],
        [
          [ [ :code, "pgbus:add_recurring" ], [ :md, "Recurring-task tables + a starter `recurring.yml`." ] ],
          [ [ :code, "pgbus:add_outbox" ], "The transactional-outbox table." ],
          [ [ :code, "pgbus:add_presence" ], "The stream-presence table." ],
          [ [ :code, "pgbus:add_queue_states" ], "The queue pause/resume + circuit-breaker state table." ],
          [ [ :code, "pgbus:add_uniqueness_keys" ], "The job-uniqueness lock table." ],
          [ [ :code, "pgbus:add_job_stats" ], "The job-stats table for the Insights dashboard." ],
          [ [ :code, "pgbus:add_stream_stats" ], "The stream-stats table (opt-in metrics)." ]
        ]
      )
      DocsUI::Callout(:note) do
        plain "A few migration-maintenance generators also ship for existing installs: "
        code { "add_failed_events_index" }
        plain ", "
        code { "add_job_stats_latency" }
        plain ", "
        code { "add_job_stats_queue_index" }
        plain ", and "
        code { "migrate_job_locks" }
        plain ". The "
        code { "pgbus:update" }
        plain " generator adds whichever of these your database is missing."
      end
    end
  end

  def tuning_generators
    DocsUI::Section("Tuning generators", description: "Re-apply table settings a schema load drops.") do
      DocsUI::Table(
        [ "Generator", "Does" ],
        [
          [ [ :code, "pgbus:tune_autovacuum" ], "Apply aggressive autovacuum settings to the high-churn queue tables." ],
          [ [ :code, "pgbus:tune_fillfactor" ], "Tune table fillfactor for existing installations." ]
        ]
      )
      DocsUI::Callout(:tip) do
        plain "Run these after "
        code { "db:schema:load" }
        plain ", which drops "
        code { "ALTER TABLE" }
        plain " settings. See "
        a(href: "/docs/performance-tuning", class: "link") { "Performance & tuning" }
        plain "."
      end
    end
  end
end
