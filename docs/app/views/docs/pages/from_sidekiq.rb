# frozen_string_literal: true

# Migrating from Sidekiq: remove Redis, switch the adapter, convert native
# workers, and map Sidekiq features to their pgbus equivalents.
class Views::Docs::Pages::FromSidekiq < DocsUI::Page
  title "From Sidekiq"
  eyebrow "Migrate"

  def lead = "Drop Redis, switch the adapter, and map Sidekiq's Pro/Enterprise features onto pgbus."

  def content
    overview
    swap
    convert_workers
    api_map
    middleware
    gains
    gotchas
  end

  private

  def overview
    DocsUI::Section("What changes") do
      md <<~'MD'
        Sidekiq brokers through Redis; pgbus brokers through PostgreSQL (PGMQ). The
        migration removes Redis and gives you dead-letter queues, worker recycling,
        and an event bus on your existing database.

        **Effort:** low if you use ActiveJob exclusively; medium-high if you have
        native Sidekiq workers or lean on Pro/Enterprise features.
      MD
    end
  end

  def swap
    DocsUI::Section("Swap the gem and adapter") do
      DocsUI::Code(<<~RUBY, filename: "Gemfile")
        # Remove
        gem "sidekiq"
        gem "sidekiq-cron"        # if used
        gem "sidekiq-unique-jobs" # if used

        # Add
        gem "pgbus"
      RUBY
      DocsUI::Code(<<~SHELL, lexer: :shell)
        bundle install && rails generate pgbus:install && rails db:migrate
      SHELL
      DocsUI::Code(<<~RUBY, filename: "config/application.rb")
        config.active_job.queue_adapter = :pgbus # was :sidekiq
      RUBY
    end
  end

  def convert_workers
    DocsUI::Section("Convert native workers", description: "ActiveJob jobs work unchanged; native workers need a rewrite.") do
      md <<~'MD'
        If every job already inherits `ApplicationJob`, you're done — they run
        unchanged. A native `Sidekiq::Job` becomes a standard ActiveJob:
      MD
      DocsUI::Code(<<~RUBY)
        # Before — native Sidekiq worker
        class HardWorker
          include Sidekiq::Job
          sidekiq_options queue: :critical, retry: 5
          def perform(user_id, action) = ...
        end

        # After — ActiveJob
        class HardWorker < ApplicationJob
          queue_as :critical
          retry_on StandardError, wait: :polynomially_longer, attempts: 5
          def perform(user_id, action) = ...
        end
      RUBY
    end
  end

  def api_map
    DocsUI::Section("API mapping") do
      DocsUI::Table(
        [ "Sidekiq", "ActiveJob / pgbus" ],
        [
          [ [ :code, "perform_async(args)" ], [ :code, "perform_later(args)" ] ],
          [ [ :code, "perform_at(time, args)" ], [ :code, ".set(wait_until: time).perform_later(args)" ] ],
          [ [ :code, "perform_in(duration, args)" ], [ :code, ".set(wait: duration).perform_later(args)" ] ],
          [ [ :code, "sidekiq_options queue:" ], [ :code, "queue_as" ] ],
          [ [ :code, "sidekiq_options retry: N" ], [ :code, "retry_on StandardError, attempts: N" ] ],
          [ [ :code, "sidekiq_retries_exhausted" ], [ :md, "`discard_on` + `after_discard`" ] ]
        ]
      )
    end
  end

  def middleware
    DocsUI::Section("Middleware → ActiveJob callbacks") do
      md <<~'MD'
        Sidekiq middleware becomes ActiveJob callbacks. Server middleware maps to
        `before_perform` / `around_perform` / `after_perform`; client middleware to
        the `*_enqueue` callbacks:
      MD
      DocsUI::Code(<<~'RUBY')
        class ApplicationJob < ActiveJob::Base
          around_perform do |job, block|
            Rails.logger.info("Starting #{job.class.name}")
            block.call
            Rails.logger.info("Finished #{job.class.name}")
          end
        end
      RUBY
    end
  end

  def gains
    DocsUI::Section("What you gain", description: "And where the Pro/Enterprise features land.") do
      DocsUI::Table(
        [ "Sidekiq feature", "pgbus equivalent" ],
        [
          [ "Batches (Pro)", [ :md, "[`Pgbus::Batch`](/docs/batches)" ] ],
          [ "Concurrency (Enterprise)", [ :md, "[`limits_concurrency`](/docs/concurrency-uniqueness)" ] ],
          [ "Unique jobs (Enterprise)", [ :md, "[`ensures_uniqueness`](/docs/concurrency-uniqueness)" ] ],
          [ "Cron (sidekiq-cron)", [ :md, "[Recurring tasks](/docs/recurring-tasks) (fugit)" ] ],
          [ "Sidekiq Web", [ :md, "[Dashboard](/docs/dashboard)" ] ]
        ]
      )
      md <<~'MD'
        Plus what Sidekiq never had: dead-letter queues, worker recycling, an event
        bus, and no Redis.
      MD
    end
  end

  def gotchas
    DocsUI::Section("Gotchas") do
      md <<~'MD'
        - **Argument serialization** — Sidekiq passes raw JSON; ActiveJob uses
          GlobalID for Active Record objects. Passing ids (`user.id`) behaves the
          same; passing records (`user`) serializes via GlobalID automatically.
        - **`Sidekiq::CurrentAttributes.persist("Current")`** — becomes
          `config.current_attributes = :auto` (or an explicit list / per-class
          `only:`/`except:`); see [Current attributes](/docs/active-job). The
          context is restored around the whole `perform_now`, including
          `retry_on` / `discard_on` blocks.
        - **`Sidekiq.redis { ... }`** — if you used Sidekiq's Redis for custom
          locks or caching, move to PostgreSQL advisory locks or another store.
        - **Queue priority** — Sidekiq uses queue weights; pgbus processes queues in
          the order listed, so put higher-priority queues first (or use
          [priority queues](/docs/routing-ordering)). For per-tenant weighting use
          [`config.fair_share`](/docs/routing-ordering) — a weighted, work-conserving
          interleave across tenants inside a queue, no per-tenant queues needed.
      MD
    end
  end
end
