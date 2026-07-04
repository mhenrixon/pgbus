# frozen_string_literal: true

# Publishing events atomically with a database write, so a rollback never leaves
# a phantom event and a commit never loses one.
class Views::Docs::Pages::Outbox < DocsUI::Page
  title "Transactional outbox"
  eyebrow "Guide"

  def lead = "Publish inside a transaction — the event commits with your data, or rolls back with it."

  def content
    problem
    setup
    usage
    how_it_works
  end

  private

  def problem
    DocsUI::Section("The dual-write problem") do
      md <<~'MD'
        Write a row and publish an event and you have two systems to keep in sync.
        If the publish happens before commit, a rollback leaves a **phantom event**
        for data that never landed. If it happens after commit, a crash in between
        **loses the event**. The outbox closes that gap: the event is written to an
        outbox table in the same transaction as your data, and a poller publishes
        it afterward.
      MD
      render Components::Diagrams::OutboxFlow.new
    end
  end

  def setup
    DocsUI::Section("Enable it") do
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:add_outbox                  # add the outbox migration
        rails generate pgbus:add_outbox --database=pgbus # for a separate database
        rails db:migrate
      SHELL
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.outbox_enabled       = true
          config.outbox_poll_interval = 1.0   # seconds
          config.outbox_batch_size    = 100
          config.outbox_retention     = 1.day # Duration also accepted
        end
      RUBY
    end
  end

  def usage
    DocsUI::Section("Publish inside a transaction") do
      md <<~'MD'
        Call `Pgbus::Outbox.publish` (a queue) or `publish_event` (a topic) inside
        the transaction that writes your data. Both the row and the outbox entry
        commit together — or roll back together:
      MD
      DocsUI::Code(<<~RUBY)
        ActiveRecord::Base.transaction do
          order = Order.create!(params)

          # Committed atomically with the order. If the transaction rolls back,
          # so does the outbox entry — no phantom event.
          Pgbus::Outbox.publish("default", { order_id: order.id })

          # For the topic-based event bus:
          Pgbus::Outbox.publish_event("orders.created", { order_id: order.id })
        end
      RUBY
    end
  end

  def how_it_works
    DocsUI::Section("How the poller works") do
      md <<~'MD'
        The outbox poller is a supervised process. Each cycle it claims a batch of
        unpublished entries with `FOR UPDATE SKIP LOCKED` (so multiple pollers never
        double-publish), sends them to PGMQ, and marks them published. An entry that
        fails to publish is simply skipped and retried next cycle. Published entries
        are purged after `outbox_retention`.
      MD
      DocsUI::Callout(:tip) do
        plain "The outbox pairs naturally with idempotent event handlers: at-least-once "
        plain "delivery plus "
        code { "idempotent!" }
        plain " means a re-published entry is handled exactly once. See "
        a(href: "/docs/event-bus", class: "link") { "Event bus" }
        plain "."
      end
    end
  end
end
