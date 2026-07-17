# frozen_string_literal: true

# The drop-in ActiveJob adapter: how jobs enqueue, run, and follow a message
# through PGMQ. The message-lifecycle diagram anchors the mental model.
class Views::Docs::Pages::ActiveJob < DocsUI::Page
  title "ActiveJob adapter"
  eyebrow "Guide"

  def lead = "Set the adapter to :pgbus and your existing jobs run on PGMQ, unchanged."

  def content
    setup
    enqueueing
    lifecycle
    what_you_keep
  end

  private

  def setup
    DocsUI::Section("Set the adapter") do
      md <<~'MD'
        pgbus is a standard ActiveJob queue adapter. Point Rails at it and every
        job in your app enqueues through PGMQ:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/application.rb")
        config.active_job.queue_adapter = :pgbus
      RUBY
      DocsUI::Callout(:note) do
        plain "Because it's a standard adapter, nothing about your job classes changes — "
        plain "no base-class swap, no per-job include. "
        code { "queue_as" }
        plain ", "
        code { "retry_on" }
        plain ", and "
        code { "discard_on" }
        plain " all work as usual."
      end
    end
  end

  def enqueueing
    DocsUI::Section("Enqueue and schedule") do
      md <<~'MD'
        Your existing jobs work with no edits:
      MD
      DocsUI::Code(<<~RUBY, filename: "app/jobs/order_confirmation_job.rb")
        class OrderConfirmationJob < ApplicationJob
          queue_as :mailers

          def perform(order)
            OrderMailer.confirmation(order).deliver_now
          end
        end

        OrderConfirmationJob.perform_later(order)                    # enqueue now
        OrderConfirmationJob.set(wait: 5.minutes).perform_later(order) # scheduled
      RUBY
      md <<~'MD'
        A scheduled job is sent with a PGMQ delay, so it stays invisible until its
        time arrives — no separate scheduler poll table.
      MD
    end
  end

  def lifecycle
    DocsUI::Section("What happens to a message", description: "Enqueue, read, execute, archive — or dead-letter.") do
      md <<~'MD'
        Each job becomes one PGMQ message. The adapter serializes it to JSON and
        sends it to the queue; a worker claims it under a visibility timeout, runs
        it inside the Rails executor, and archives it on success. On failure the
        visibility timeout expires and the message is retried — until `read_ct`
        crosses `max_retries`, when it routes to the dead-letter queue.
      MD
      render Components::Diagrams::MessageLifecycle.new
      md <<~'MD'
        The retry backoff and dead-letter details are on
        [Retries & dead letters](/docs/retries-dead-letters).
      MD
    end
  end

  def what_you_keep
    DocsUI::Section("Serialization and safety") do
      md <<~'MD'
        Payloads are **JSON only** — pgbus never uses `Marshal`, so a malicious or
        corrupt payload can't deserialize into arbitrary Ruby. GlobalID arguments
        (an Active Record record) resolve through the same
        `config.allowed_global_id_models` allowlist that EventBus payloads use:
        when the allowlist is set, a crafted `_aj_globalid` job argument whose
        model is not listed raises `Pgbus::SerializationError` before Rails'
        unrestricted `GlobalID::Locator` runs. Leave the config `nil` (default)
        for allow-all; set `[]` to deny all. Apps that enqueue ActiveStorage
        analyze/purge/transform jobs need `ActiveStorage::Blob` (and related
        models) on the allowlist.
      MD
      DocsUI::Callout(:tip) do
        plain "Need at-most-once semantics or a concurrency cap? See "
        a(href: "/docs/concurrency-uniqueness", class: "link") { "Concurrency & uniqueness" }
        plain " — "
        code { "ensures_uniqueness" }
        plain " and "
        code { "limits_concurrency" }
        plain " layer straight onto a job class."
      end
    end
  end
end
