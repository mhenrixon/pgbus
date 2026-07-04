# frozen_string_literal: true

# Keeping a busy pgbus install fast: autovacuum tuning for the high-churn queue
# tables, archive retention, and the health metrics that tell you when tuning is
# needed.
class Views::Docs::Pages::PerformanceTuning < DocsUI::Page
  title "Performance & tuning"
  eyebrow "Operations"

  def lead = "Tune autovacuum for the high-churn queue tables, size archive retention, and watch the health metrics."

  def content
    autovacuum
    archive
    health_metrics
  end

  private

  def autovacuum
    DocsUI::Section("Autovacuum tuning", description: "Queue tables churn far faster than the Postgres defaults expect.") do
      md <<~'MD'
        PGMQ queue tables see heavy insert/delete churn — a message is inserted,
        read (an UPDATE), and archived (a DELETE) in seconds. PostgreSQL's default
        autovacuum is too conservative for that, so dead tuples accumulate and
        indexes bloat. pgbus applies aggressive per-table tuning automatically:
        new queues get it at creation, the install migration tunes the default
        queue, and `pgbus:update` detects untuned tables.
      MD
      md <<~'MD'
        `db:schema:load` drops `ALTER TABLE` settings, so re-apply after a schema
        load with the generator:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:tune_autovacuum                  # generate the migration
        rails generate pgbus:tune_autovacuum --database=pgbus # for a separate database
      SHELL
      DocsUI::Callout(:warning) do
        plain "A long-running transaction pins the MVCC horizon and stops vacuum from "
        plain "cleaning any dead tuple created while it's open — the most common cause of "
        plain "queue-table bloat. Watch "
        code { "pgbus_oldest_transaction_age_seconds" }
        plain "."
      end
    end
  end

  def archive
    DocsUI::Section("Archive retention", description: "Archive tables grow unbounded without it.") do
      md <<~'MD'
        Archived (successfully-processed) messages accumulate in the `a_<queue>`
        tables. The dispatcher compacts them hourly; size the window to your volume:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.archive_retention = 3.days # default 7.days; nil disables cleanup
        end
      RUBY
      DocsUI::Callout(:tip) do
        plain "High-volume queues (>100 msg/s) want a shorter window (1–3 days); "
        plain "audit-sensitive queues may need 30+. SSE stream retention is separate — "
        plain "see "
        a(href: "/docs/streams", class: "link") { "Real-time streams" }
        plain "."
      end
    end
  end

  def health_metrics
    DocsUI::Section("The metrics that tell you when to tune") do
      md <<~'MD'
        The dashboard's Queue Health panel and the Prometheus gauges surface the
        signals that predict trouble before it becomes an incident:
      MD
      DocsUI::Table(
        [ "Metric", "Watch for" ],
        [
          [ [ :code, "pgbus_table_dead_tuples" ], "Dead tuples climbing — vacuum isn't keeping up." ],
          [ [ :code, "pgbus_table_bloat_ratio" ], "Dead / (dead + live) rising toward 1." ],
          [ [ :code, "pgbus_table_last_vacuum_age_seconds" ], "Long gaps since the last vacuum." ],
          [ [ :code, "pgbus_oldest_transaction_age_seconds" ], "A long transaction pinning the MVCC horizon." ],
          [ [ :code, "pgbus_worker_pool_utilization" ], "Busy / capacity near 1 — you need more threads." ]
        ]
      )
      DocsUI::Callout(:note) do
        plain "This page covers running-system tuning. The gem's benchmark harness "
        plain "and allocation budgets — for contributors optimizing hot paths — live in "
        plain "the repo's "
        code { "docs/performance.md" }
        plain "."
      end
    end
  end
end
