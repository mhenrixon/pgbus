# frozen_string_literal: true

# Controlling which work runs first and in what order: priority sub-queues,
# consumer priority for active/standby workers, and single-active-consumer for
# strict ordering.
class Views::Docs::Pages::RoutingOrdering < DocsUI::Page
  title "Routing & ordering"
  eyebrow "Guide"

  def lead = "Priority sub-queues, active/standby consumer priority, and single-active-consumer for strict order."

  def content
    priority_queues
    consumer_priority
    single_active
  end

  private

  def priority_queues
    DocsUI::Section("Priority queues", description: "High-priority work processed first.") do
      md <<~'MD'
        Enable priority levels and each logical queue gains sub-queues (`_p0`
        highest through `_p2`). A worker drains `_p0` before it touches `_p1`, and
        `_p1` before `_p2`:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.priority_levels  = 3 # creates _p0, _p1, _p2 per logical queue
          config.default_priority = 1 # jobs without a priority go to _p1
        end
      RUBY
      md <<~'MD'
        Set a job's priority with Active Job's built-in `queue_with_priority`:
      MD
      DocsUI::Code(<<~RUBY)
        class CriticalAlertJob < ApplicationJob
          queue_as :default
          queue_with_priority 0 # highest
        end

        class ReportJob < ApplicationJob
          queue_as :default
          queue_with_priority 2 # lowest
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "With "
        code { "priority_levels" }
        plain " unset (the default), priority queues are off and each logical queue is a single queue."
      end
    end
  end

  def consumer_priority
    DocsUI::Section("Consumer priority", description: "Active/standby workers on the same queues.") do
      md <<~'MD'
        When several workers serve the same queues, higher-priority workers process
        first; lower-priority ones back off (3× the polling interval) while a
        higher-priority peer is active, and resume automatically when it goes
        stale.
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure do |c|
          c.capsule :primary,  queues: %w[default], threads: 10, consumer_priority: 10
          c.capsule :fallback, queues: %w[default], threads: 5,  consumer_priority: 0
        end
      RUBY
      md <<~'MD'
        Priority lives in heartbeat metadata; workers discover higher-priority
        peers by reading the `pgbus_processes` table.
      MD
    end
  end

  def single_active
    DocsUI::Section("Single active consumer", description: "Strict ordering via advisory locks.") do
      md <<~'MD'
        For a queue that must be processed in order, mark its capsules
        `single_active_consumer: true`. Only one worker reads the queue at a time —
        others skip it and work elsewhere. It uses non-blocking PostgreSQL
        session-level advisory locks, which auto-release on connection close, so a
        standby takes over within one polling tick if the primary dies:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure do |c|
          c.capsule :ordered_primary, queues: %w[ordered_events], threads: 1, single_active_consumer: true
          c.capsule :ordered_standby, queues: %w[ordered_events], threads: 1, single_active_consumer: true
        end
      RUBY
      DocsUI::Callout(:warning) do
        plain "Single active consumer serializes the queue to one worker thread — "
        plain "throughput is bounded by that one consumer. Use it only where ordering truly matters."
      end
    end
  end
end
