# frozen_string_literal: true

# Migrating from SolidQueue: both are PostgreSQL ActiveJob adapters, so the move
# is mostly config. Covers the worker-config mapping and what pgbus adds.
class Views::Docs::Pages::FromSolidQueue < DocsUI::Page
  title "From SolidQueue"
  eyebrow "Migrate"

  def lead = "Both are PostgreSQL ActiveJob adapters — swap the config and gain LISTEN/NOTIFY, DLQs, and recycling."

  def content
    overview
    swap
    workers
    mapping
    gains
  end

  private

  def overview
    DocsUI::Section("What changes") do
      md <<~'MD'
        SolidQueue and pgbus share an architecture — a supervisor/worker model,
        `FOR UPDATE SKIP LOCKED` polling, forked processes. Both are pure ActiveJob
        adapters, so your jobs work unchanged. pgbus adds LISTEN/NOTIFY for instant
        wake-up (SolidQueue only polls), dead-letter queues, worker recycling, and an
        event bus, and stores messages in PGMQ rather than custom tables.

        **Effort:** low.
      MD
    end
  end

  def swap
    DocsUI::Section("Swap the gem and adapter") do
      DocsUI::Code(<<~RUBY, filename: "Gemfile")
        # Remove
        gem "solid_queue"
        gem "mission_control-jobs" # if used

        # Add
        gem "pgbus"
      RUBY
      DocsUI::Code(<<~SHELL, lexer: :shell)
        bundle install && rails generate pgbus:install && rails db:migrate
      SHELL
      DocsUI::Code(<<~RUBY, filename: "config/application.rb")
        config.active_job.queue_adapter = :pgbus # was :solid_queue
      RUBY
      DocsUI::Callout(:note) do
        plain "If you ran SolidQueue in a separate database via "
        code { "config.solid_queue.connects_to" }
        plain ", the pgbus equivalent is "
        code { "config.connects_to" }
        plain " — see "
        a(href: "/docs/separate-database", class: "link") { "Separate database" }
        plain "."
      end
    end
  end

  def workers
    DocsUI::Section("Convert the worker config") do
      md <<~'MD'
        SolidQueue's `processes: N` forks N identical workers; in pgbus you list the
        same worker entry N times (one per process), or use the capsule DSL:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          # SolidQueue: queues "critical", threads 5, processes 2
          c.workers = "critical: 5; critical: 5; default, low: 3"
          c.max_jobs_per_worker = 10_000
          c.max_memory_mb       = 512
        end
      RUBY
    end
  end

  def mapping
    DocsUI::Section("Configuration mapping") do
      DocsUI::Table(
        [ "SolidQueue", "pgbus", "Notes" ],
        [
          [ [ :code, "polling_interval" ], [ :code, "polling_interval" ], "Defaults to 0.1s; LISTEN/NOTIFY makes it a fallback." ],
          [ [ :code, "threads" ], [ :code, "threads" ], "Same concept." ],
          [ [ :code, "processes: N" ], "Repeat the worker entry", "One entry per forked process." ],
          [ [ :code, 'queues: "a,b"' ], [ :code, "queues: [a, b]" ], "Array, not a comma string." ],
          [ [ :code, 'queues: "*"' ], "List queues explicitly", "PGMQ queues are explicit." ]
        ]
      )
      DocsUI::Callout(:tip) do
        plain "SolidQueue's "
        code { "limits_concurrency" }
        plain " has a near-identical pgbus API (auto-included, no explicit require). See "
        a(href: "/docs/concurrency-uniqueness", class: "link") { "Concurrency & uniqueness" }
        plain "; recurring.yml maps to "
        a(href: "/docs/recurring-tasks", class: "link") { "Recurring tasks" }
        plain "."
      end
    end
  end

  def gains
    DocsUI::Section("What you gain") do
      md <<~'MD'
        - **LISTEN/NOTIFY** — instant wake-up instead of polling latency.
        - **Dead-letter queues** — SolidQueue has none; pgbus routes exhausted jobs
          to a `_dlq` queue.
        - **Worker recycling** — memory, job-count, and lifetime limits.
        - **An event bus** — pub/sub with topic routing, on the same infrastructure.
      MD
    end
  end
end
