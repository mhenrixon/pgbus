# frozen_string_literal: true

# Migrating from GoodJob: both are PostgreSQL-native with LISTEN/NOTIFY. The
# main difference is advisory locks + good_jobs vs PGMQ + visibility timeouts.
class Views::Docs::Pages::FromGoodJob < DocsUI::Page
  title "From GoodJob"
  eyebrow "Migrate"

  def lead = "Both are PostgreSQL-native with LISTEN/NOTIFY — swap advisory locks and good_jobs for PGMQ."

  def content
    overview
    swap
    concurrency_and_cron
    gains
    gotchas
  end

  private

  def overview
    DocsUI::Section("What changes") do
      md <<~'MD'
        GoodJob and pgbus are both PostgreSQL-native with LISTEN/NOTIFY. The
        architectural difference: GoodJob uses advisory locks and a `good_jobs`
        table; pgbus uses PGMQ — a dedicated message-queue extension — with
        visibility timeouts. Both are pure ActiveJob adapters, so jobs move over
        unchanged.

        **Effort:** low for standard ActiveJob; medium if you rely on GoodJob's
        concurrency controls, batches, or cron.
      MD
    end
  end

  def swap
    DocsUI::Section("Swap the gem and adapter") do
      DocsUI::Code(<<~RUBY, filename: "Gemfile")
        # Remove
        gem "good_job"

        # Add
        gem "pgbus"
      RUBY
      DocsUI::Code(<<~SHELL, lexer: :shell)
        bundle install && rails generate pgbus:install && rails db:migrate
      SHELL
      DocsUI::Code(<<~RUBY, filename: "config/application.rb")
        config.active_job.queue_adapter = :pgbus # was :good_job
      RUBY
    end
  end

  def concurrency_and_cron
    DocsUI::Section("Concurrency and cron") do
      md <<~'MD'
        GoodJob's `good_job_control_concurrency_with` maps to pgbus's
        `limits_concurrency`; GoodJob's `config.good_job.cron` maps to pgbus's
        recurring tasks. Both DSLs are auto-included — no explicit require:
      MD
      DocsUI::Code(<<~'RUBY')
        # GoodJob: good_job_control_concurrency_with(total_limit: 1, key: -> { ... })
        # pgbus:
        class ProcessOrderJob < ApplicationJob
          limits_concurrency to: 1, key: ->(order_id) { "ProcessOrder-#{order_id}" }
        end
      RUBY
      DocsUI::Callout(:tip) do
        plain "See "
        a(href: "/docs/concurrency-uniqueness", class: "link") { "Concurrency & uniqueness" }
        plain " and "
        a(href: "/docs/recurring-tasks", class: "link") { "Recurring tasks" }
        plain " for the full APIs; "
        a(href: "/docs/batches", class: "link") { "Batches" }
        plain " replaces GoodJob::Batch."
      end
    end
  end

  def gains
    DocsUI::Section("What you gain") do
      md <<~'MD'
        - **Dead-letter queues** — GoodJob retries in place; pgbus routes exhausted
          jobs to a `_dlq` queue for inspection.
        - **Worker recycling** — memory, job-count, and lifetime limits.
        - **An event bus** and **Postgres-SSE streams** — on the same database.
      MD
    end
  end

  def gotchas
    DocsUI::Section("Gotchas") do
      md <<~'MD'
        - **Locking model** — GoodJob's advisory locks are held for the duration of a
          job; PGMQ uses a visibility timeout that expires and re-delivers. A job
          that outlives its `visibility_timeout` can be re-read, so size the timeout
          above your longest job (or extend it in-job).
        - **Table cleanup** — after jobs drain, drop the `good_jobs` tables; pgbus
          doesn't read them.
      MD
    end
  end
end
