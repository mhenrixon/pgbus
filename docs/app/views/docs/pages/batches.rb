# frozen_string_literal: true

# Coordinating a fan-out of jobs and firing a callback when they all finish.
class Views::Docs::Pages::Batches < DocsUI::Page
  title "Batches"
  eyebrow "Guide"

  def lead = "Enqueue a group of jobs and run a callback when the whole batch completes."

  def content
    creating
    callbacks
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
          on_discard: BatchFailedJob,
          description: "Import users",
          properties: { initiated_by: current_user.id }
        )

        batch.enqueue do
          users.each { |user| ImportUserJob.perform_later(user.id) }
        end
      RUBY
    end
  end

  def callbacks
    DocsUI::Section("Callbacks") do
      DocsUI::Table(
        [ "Callback", "Fired when" ],
        [
          [ [ :code, "on_finish" ], "All jobs completed (success or discard)." ],
          [ [ :code, "on_success" ], "All jobs completed successfully (zero discarded)." ],
          [ [ :code, "on_discard" ], "At least one job was dead-lettered." ]
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
        2. `batch.enqueue { ... }` tags each enqueued job with the batch id in its
           payload.
        3. As each job completes or is dead-lettered, the executor atomically
           updates the batch counters.
        4. When `completed + discarded == total`, the status flips to `"finished"`
           and the callback jobs are enqueued.
        5. The dispatcher cleans up finished batches older than 7 days.
      MD
      DocsUI::Callout(:note) do
        plain "The "
        code { "pgbus_batches" }
        plain " table comes from the base "
        code { "pgbus:install" }
        plain " migration — no separate generator is needed for batches."
      end
    end
  end
end
