# frozen_string_literal: true

# What pgbus is, why it exists, and how it compares to the other Rails job
# backends. The comparison table is the payoff — it's the fastest way to see
# where pgbus sits.
class Views::Docs::Pages::Overview < DocsUI::Page
  title "Overview"
  eyebrow "Getting started"

  def lead
    "pgbus is PostgreSQL-native job processing and an event bus for Rails, built on PGMQ. " \
      "Jobs, events, and live updates all run in the database you already have."
  end

  def content
    what_it_is
    what_you_get
    comparison
    where_next
  end

  private

  def what_it_is
    DocsUI::Section("One database, no Redis", description: "The whole point.") do
      md <<~'MD'
        Most Rails job backends bolt on infrastructure: Sidekiq needs Redis, and
        turbo-rails broadcasts need Action Cable (and Redis again). pgbus needs
        **one PostgreSQL** — the database your app already runs — and layers jobs,
        a pub/sub event bus, and Server-Sent-Event streams on top of
        [PGMQ](https://github.com/pgmq/pgmq), a lightweight message-queue
        extension.

        That means one thing to provision, one thing to back up, one thing to
        monitor. Enqueues participate in your database transactions. And the
        durability guarantees are Postgres's, not a separate broker's.
      MD

      DocsUI::Callout(:note) do
        plain "PGMQ can run as a PostgreSQL extension or as vendored, embedded SQL "
        plain "(no extension required). See "
        a(href: "/docs/installation", class: "link") { "Installation" }
        plain " for the schema modes."
      end
    end
  end

  def what_you_get
    DocsUI::Section("What you get") do
      md <<~'MD'
        - **A drop-in ActiveJob adapter** — set the adapter to `:pgbus` and your
          existing jobs enqueue through PGMQ, with dead-letter routing and retry
          backoff you didn't have to write.
        - **An event bus** — publish once; AMQP-style topic patterns (`orders.#`,
          `payments.*`) fan out to idempotent subscribers, deduplicated by event id.
        - **Workers that recycle** — `max_jobs_per_worker`, `max_memory_mb`, and
          `max_worker_lifetime` retire a worker before it leaks, fixing the
          memory-bloat problem other backends leave to you.
        - **Reliability primitives** — dead-letter queues, a circuit breaker,
          job uniqueness, concurrency limits, priority queues, and a transactional
          outbox.
        - **Real-time streams** — a Turbo Streams transport over Postgres SSE, with
          no Action Cable and no lost messages on reconnect.
        - **A live dashboard** — queues, jobs, processes, failures, and dead
          letters, auto-refreshing over Turbo Frames (no WebSocket).
      MD
    end
  end

  def comparison
    DocsUI::Section("How it compares", description: "Against the common Rails job backends.") do
      DocsUI::Table(
        [ "Feature", "Sidekiq", "SolidQueue", "GoodJob", "pgbus" ],
        [
          [ "Infrastructure", "Redis", "PostgreSQL", "PostgreSQL", [ :code, "PostgreSQL (PGMQ)" ] ],
          [ "ActiveJob adapter", "Yes", "Yes", "Yes", "Yes" ],
          [ "LISTEN/NOTIFY", "N/A", "Polling only", "Yes", "Yes" ],
          [ "Dead letter queues", "Retries only", "No", "No", "Yes" ],
          [ "Worker recycling", "No", "No", "No", "Yes" ],
          [ "Event bus", "No", "No", "No", "Yes" ],
          [ "Idempotent events", "No", "No", "No", "Yes" ],
          [ "Concurrency controls", "Enterprise", [ :code, "limits_concurrency" ], "Yes", [ :code, "Pgbus::Concurrency" ] ],
          [ "Recurring / cron jobs", [ :code, "sidekiq-cron" ], [ :code, "recurring.yml" ], "Yes", [ :md, "Built in — [recurring tasks](/docs/recurring-tasks)" ] ],
          [ "Batches", "Pro", "No", [ :code, "GoodJob::Batch" ], [ :code, "Pgbus::Batch" ] ],
          [ "Web dashboard", "Yes", "Mission Control", "Yes", [ :code, "Pgbus::Engine" ] ],
          [ "Turbo Streams transport", "Cable (Redis)", "Cable", "Cable", "Built-in SSE" ],
          [ "Lost messages on reconnect", "Yes", "Yes", "Yes", "No (msg_id cursor)" ],
          [ "Transactional broadcasts", "No", "No", "No", "Yes (until commit)" ]
        ]
      )

      DocsUI::Callout(:tip) do
        plain "Coming from another backend? The migration guides cover what changes "
        plain "and what stays the same: "
        a(href: "/docs/from-sidekiq", class: "link") { "Sidekiq" }
        plain ", "
        a(href: "/docs/from-solid-queue", class: "link") { "SolidQueue" }
        plain ", "
        a(href: "/docs/from-good-job", class: "link") { "GoodJob" }
        plain "."
      end
    end
  end

  def where_next
    DocsUI::Section("Where to next") do
      md <<~'MD'
        - New here? Start with [Installation](/docs/installation), then the
          [Quick start](/docs/quick-start).
        - Want the mental model first? Read [Architecture](/docs/architecture).
        - Looking for a specific capability? The **Guide** covers the
          [ActiveJob adapter](/docs/active-job), the [event bus](/docs/event-bus),
          [retries & dead letters](/docs/retries-dead-letters), and more.
      MD
    end
  end
end
