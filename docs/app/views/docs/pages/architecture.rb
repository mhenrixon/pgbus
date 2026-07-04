# frozen_string_literal: true

# The moving parts and how a job flows through them. The diagram is the anchor;
# the layer list and the job lifecycle fill in the detail.
class Views::Docs::Pages::Architecture < DocsUI::Page
  title "Architecture"
  eyebrow "Guide"

  def lead = "A supervisor forks workers that move messages through PGMQ's tables; the dashboard reads the same tables."

  def content
    picture
    layers
    processes
    lifecycle
  end

  private

  def picture
    DocsUI::Section("The shape of it") do
      md <<~'MD'
        Everything an app does — enqueue a job, publish an event, broadcast an
        update — goes through one door, `Pgbus::Client`, into PGMQ's queue tables.
        A supervisor forks workers that read and execute; a dispatcher runs the
        periodic maintenance; the dashboard reads the very same tables.
      MD
      render Components::Diagrams::ArchitectureFlow.new
    end
  end

  def layers
    DocsUI::Section("The layers", description: "From configuration up to the dashboard.") do
      md <<~'MD'
        pgbus is organized as a stack — each layer depends only on the ones below
        it. You interact with the top; the invariants live at the bottom.
      MD
      DocsUI::Table(
        [ "Layer", "What it does" ],
        [
          [ "Dashboard", "The mounted Rails engine — queues, jobs, failures, dead letters." ],
          [ "CLI", [ :code, "pgbus start" ] ],
          [ "Process model", "Supervisor, worker, dispatcher, consumer, outbox poller." ],
          [ "Event bus", "Publisher, subscriber registry, topic routing, idempotent handlers." ],
          [ "ActiveJob", "The adapter + executor that serialize and run jobs." ],
          [ "Client", [ :code, "Pgbus::Client" ] ],
          [ "Configuration", "Every knob, resolved once at boot." ]
        ]
      )
      DocsUI::Callout(:note) do
        plain "The invariant that keeps the stack honest: "
        strong { "all PGMQ access goes through Pgbus::Client" }
        plain " — queue prefixing, visibility timeouts, and dead-letter routing are enforced there, not scattered across callers."
      end
    end
  end

  def processes
    DocsUI::Section("The processes a supervisor manages") do
      md <<~'MD'
        `pgbus start` boots a supervisor (a fork manager). It supervises:
      MD
      DocsUI::Table(
        [ "Process", "Responsibility" ],
        [
          [ "Worker", "Reads a queue (or wakes via LISTEN/NOTIFY), executes jobs, recycles itself." ],
          [ "Dispatcher", "Maintenance: idempotency cleanup, archive compaction, stale-process reaping, the circuit breaker." ],
          [ "Scheduler", "Enqueues recurring tasks on their cron schedule." ],
          [ "Consumer", "Reads event-bus topic queues and runs handlers." ],
          [ "Outbox poller", "Moves committed outbox rows into PGMQ (when the outbox is enabled)." ]
        ]
      )
    end
  end

  def lifecycle
    DocsUI::Section("How a job flows through the system") do
      md <<~'MD'
        1. **Enqueue** — Active Job serializes the job to JSON; pgbus sends it to
           the appropriate PGMQ queue through `Pgbus::Client`.
        2. **Read** — a worker polls the queue (or wakes instantly via
           LISTEN/NOTIFY) and claims the message with a visibility timeout, so no
           other worker takes it.
        3. **Execute** — the job is deserialized and run inside the Rails executor.
        4. **Archive or retry** — on success the message is archived to the
           `a_<queue>` table; on failure the visibility timeout expires and the
           message becomes available again. PGMQ's `read_ct` counts delivery
           attempts.
        5. **Dead letter** — when `read_ct` exceeds `max_retries`, the message
           moves to the `<queue>_dlq` queue for inspection instead of retrying
           forever.
      MD
      md <<~'MD'
        The message lifecycle has its own picture in
        [Retries & dead letters](/docs/retries-dead-letters).
      MD
    end
  end
end
