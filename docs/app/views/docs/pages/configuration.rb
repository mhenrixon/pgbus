# frozen_string_literal: true

# How pgbus is configured: the initializer, the config groups that matter most,
# and the capsule DSL for defining workers. The exhaustive option list lives on
# the Configuration reference page.
class Views::Docs::Pages::Configuration < DocsUI::Page
  title "Configuration"
  eyebrow "Getting started"

  def lead = "One initializer drives everything — queues, retries, recycling, and worker capsules."

  def content
    initializer
    common_knobs
    capsules
    upgrading
  end

  private

  def initializer
    DocsUI::Section("The initializer", description: "Call Pgbus.configure once at boot.") do
      md <<~'MD'
        pgbus reads its settings from a `Pgbus.configure` block. Every option has
        a sensible default, so an empty block (or no initializer at all) gives you
        a working setup on your primary database.
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.queue_prefix       = "myapp"        # all queues → myapp_<name>
          c.max_retries        = 5              # failed reads before the DLQ
          c.visibility_timeout = 30.seconds
        end
      RUBY

      DocsUI::Callout(:note) do
        plain "Queue names are always prefixed: "
        code { "{queue_prefix}_{name}" }
        plain " (default "
        code { "pgbus_default" }
        plain "). Dead-letter queues append "
        code { "_dlq" }
        plain "."
      end
    end
  end

  def common_knobs
    DocsUI::Section("The knobs you'll reach for first") do
      md <<~'MD'
        These are the settings most apps touch. Durations accept an integer number
        of seconds or an `ActiveSupport::Duration` (`30.seconds`, `1.hour`).
      MD
      DocsUI::PropTable([
        [ [ :code, "queue_prefix" ], "String", [ :code, '"pgbus"' ], "Prefix for every PGMQ queue name." ],
        [ [ :code, "max_retries" ], "Integer", [ :code, "5" ], "Failed reads before a message routes to the dead-letter queue." ],
        [ [ :code, "visibility_timeout" ], "Duration", [ :code, "30" ], "How long a read message stays invisible before it can be retried." ],
        [ [ :code, "workers" ], "String / Array", [ :code, "default: 5" ], "Worker capsule definitions — see the capsule DSL below." ],
        [ [ :code, "max_jobs_per_worker" ], "Integer, nil", [ :code, "nil" ], "Recycle a worker after N jobs." ],
        [ [ :code, "max_memory_mb" ], "Integer, nil", [ :code, "nil" ], "Recycle a worker when RSS exceeds N MB." ],
        [ [ :code, "max_worker_lifetime" ], "Duration, nil", [ :code, "nil" ], "Recycle a worker after N seconds." ],
        [ [ :code, "idempotency_ttl" ], "Duration, nil", [ :code, "7.days" ], "How long processed-event records are kept for dedup." ]
      ])
      md <<~'MD'
        The complete list — outbox, streams, metrics, health, and the rest — is on
        the [Configuration reference](/docs/configuration-reference).
      MD
    end
  end

  def capsules
    DocsUI::Section("Worker capsules", description: "Which queues each worker serves, and how many threads.") do
      md <<~'MD'
        A **capsule** is a group of worker threads bound to a set of queues. The
        shortest form is the string DSL — Sidekiq-style `queues: threads`,
        semicolons separating capsules:
      MD
      DocsUI::Code(<<~RUBY)
        # default + mailers on 10 threads; critical on its own 5 threads
        c.workers = "default, mailers: 10; critical: 5"
      RUBY
      md <<~'MD'
        When you need advanced options — a single active consumer for strict
        ordering, or a consumer priority — use named capsules:
      MD
      DocsUI::Code(<<~RUBY)
        c.capsule :ordered, queues: %w[ordered_events], threads: 1, single_active_consumer: true
      RUBY
      md <<~'MD'
        The ordering and priority options are covered in
        [Routing & ordering](/docs/routing-ordering); recycling is in
        [Running workers](/docs/running-workers).
      MD
    end
  end

  def upgrading
    DocsUI::Section("Upgrading an existing install") do
      md <<~'MD'
        `rails generate pgbus:update` does two things in one pass: it converts a
        legacy `config/pgbus.yml` to a Ruby initializer, and it inspects your live
        database and adds any missing pgbus migrations. It detects a separate
        database automatically, so you don't re-specify `--database=pgbus`.
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:update            # convert config + add missing migrations
        rails generate pgbus:update --dry-run  # print the plan, create nothing
      SHELL
    end
  end
end
