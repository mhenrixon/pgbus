# frozen_string_literal: true

# Coordinating a fan-out of jobs and firing a callback when they all finish.
class Views::Docs::Pages::Batches < DocsUI::Page
  title "Batches"
  eyebrow "Guide"

  def lead = "Enqueue a group of jobs and run a callback when the whole batch completes."

  def content
    creating
    open_batches
    callbacks
    configured_callbacks
    how_it_works
  end

  private

  def creating
    DocsUI::Section("Create and enqueue a batch") do
      md <<~'MD'
        A batch tracks a group of related jobs. Enqueue the jobs inside
        `batch.enqueue` and each is tagged with the batch id:
      MD
      DocsUI::Code(<<~RUBY)
        batch = Pgbus::Batch.new(
          on_finish: BatchFinishedJob,
          on_success: BatchSucceededJob,
          on_failure: BatchFailedJob,
          description: "Import users",
          properties: { initiated_by: current_user.id }
        )

        batch.enqueue do
          users.each { |user| ImportUserJob.perform_later(user.id) }
        end
      RUBY
    end
  end

  def open_batches
    DocsUI::Section("Open batches") do
      md <<~'MD'
        A batch stays open until it finishes. Call `enqueue` again to add
        another stage — `total_jobs` grows and the callbacks wait for the new
        jobs too:
      MD
      DocsUI::Code(<<~RUBY)
        batch = Pgbus::Batch.new(on_finish: BatchFinishedJob)
        batch.enqueue { ExtractJob.perform_later }
        batch.enqueue { TransformJob.perform_later }  # same batch, total_jobs == 2
      RUBY
      md <<~'MD'
        A job running inside a batch reaches its own batch through `batch` and
        can add siblings the same way. Membership stays explicit: only jobs
        enqueued inside an `enqueue` block join the batch, so a fan-out from a
        batched job does not silently extend it.
      MD
      DocsUI::Code(<<~RUBY, filename: "app/jobs/extract_job.rb")
        class ExtractJob < ApplicationJob
          def perform
            rows = extract
            batch.enqueue do
              rows.each { |row| TransformJob.perform_later(row.id) }
            end
          end
        end
      RUBY
      md <<~'MD'
        `Pgbus::Batch.find(batch_id)` returns the same handle from anywhere —
        with `description`, `properties`, `status`, `total_jobs`,
        `completed_jobs`, `failed_jobs`, `pending_jobs`, `progress_percentage`
        and `finished?`. Adding to a batch that has already finished raises
        `Pgbus::Batch::AlreadyFinished` — at `perform_later`, before the job
        is sent, even if the handle you hold is stale.
      MD
      DocsUI::Callout(:warn) do
        plain "Breaking change (pre-1.0): "
        code { "Pgbus::Batch.find" }
        plain " used to return the raw attributes Hash. Read the values off the handle, or query "
        code { "Pgbus::BatchEntry" }
        plain " directly for a row."
      end
    end
  end

  def configured_callbacks
    DocsUI::Section("Configured callback jobs") do
      md <<~'MD'
        A callback can be a configured ActiveJob instance instead of a bare
        class, so it runs on the queue — and with the delay — you choose:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus::Batch.new(
          on_finish: BatchFinishedJob.new.set(queue: :critical, wait: 5.minutes)
        )
      RUBY
      md <<~'MD'
        `.set` options resolve when the batch is created, and the serialized job
        is stored on the batch row. At fire time it is enqueued on its
        configured queue with `callback_batch_id` pointing at the finished
        batch, so the callback reads the batch through `batch` rather than
        through a properties argument:
      MD
      DocsUI::Code(<<~'RUBY', filename: "app/jobs/batch_finished_job.rb")
        class BatchFinishedJob < ApplicationJob
          def perform(*)
            Rails.logger.info "#{batch.completed_jobs}/#{batch.total_jobs} done"
            User.find(batch.properties["initiated_by"])
          end
        end
      RUBY
      md <<~'MD'
        A callback is never a member of the batch it reports on — its own
        `batch_id` is nil, so enqueueing it can never keep the batch open.
      MD
      DocsUI::Callout(:note) do
        plain "Existing installs need "
        code { "rails generate pgbus:add_batch_callback_jobs" }
        plain " (or "
        code { "pgbus:update" }
        plain ") for the jsonb callback columns. Bare callback classes keep the "
        code { "perform_later(properties)" }
        plain " signature, deprecated at 1.0."
      end
    end
  end

  def callbacks
    DocsUI::Section("Callbacks") do
      DocsUI::Table(
        [ "Callback", "Fired when" ],
        [
          [ [ :code, "on_finish" ], "The batch finished (no outstanding execution rows remain), including after a dispatcher sweep repair." ],
          [ [ :code, "on_success" ], "The batch finished with zero failed jobs." ],
          [ [ :code, "on_failure" ], "The batch finished with at least one dead-lettered job. (`on_discard:` is a deprecated alias until 1.0.)" ]
        ]
      )
      md <<~'MD'
        A callback job receives the batch `properties` hash as its argument:
      MD
      DocsUI::Code(<<~RUBY, filename: "app/jobs/batch_finished_job.rb")
        class BatchFinishedJob < ApplicationJob
          def perform(properties)
            user = User.find(properties["initiated_by"])
            ImportMailer.complete(user).deliver_later
          end
        end
      RUBY
    end
  end

  def how_it_works
    DocsUI::Section("How batches work") do
      md <<~'MD'
        1. `Batch.new(...)` creates a row in `pgbus_batches` with
           `status: "pending"`.
        2. `batch.enqueue { ... }` tags each enqueued job with the batch id and,
           in one transaction *before* the message is sent, increments
           `total_jobs` and inserts a `pgbus_batch_executions` row (identity is
           the ActiveJob `job_id`). The increment is guarded on an unfinished
           batch — that guard is what raises `AlreadyFinished`. A
           `perform_all_later` counts once for the whole bulk.
        3. As each job is archived or dead-lettered, the executor deletes that
           execution row and bumps `completed_jobs` / `failed_jobs`.
           `total_jobs == outstanding rows + completed + failed` holds at every
           commit point.
        4. The batch finishes when no execution rows remain and the counters
           add up (single-winner update). A dispatcher sweep repairs crash
           windows — a worker that dies between archive and row-delete, an
           enqueue that dies between insert and send, a `pending` batch whose
           block never returned. An unsent row is only un-counted once the
           sweep has checked the queue and DLQ for its message; a batch is
           only "stalled" after `config.batch_stall_threshold` (default
           5 minutes) without a new execution row.
        5. The dispatcher cleans up finished batches older than
           `config.batch_retention` (default 7 days).
      MD
      DocsUI::Callout(:note) do
        plain "Existing installs need "
        code { "rails generate pgbus:add_batch_executions" }
        plain " (or "
        code { "pgbus:update" }
        plain "). Fresh "
        code { "pgbus:install" }
        plain " already includes the executions table."
      end
    end
  end
end
