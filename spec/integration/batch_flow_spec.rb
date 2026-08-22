# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# End-to-end coverage of batches: real jobs enqueued inside a batch, real
# per-job completion signals, and a real callback job landing on a PGMQ queue
# only after the final job completes. Previously batch_spec.rb stubbed
# BatchEntry entirely (receive_message_chain), so the finish-detection and
# callback-enqueue paths never ran against a database.
RSpec.describe "Batch flow (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:callback_queue) { "batch_callbacks" }
  let(:work_queue) { "batch_work" }

  # Worker jobs go to their own queue so reads of the callback queue only ever
  # see callback jobs — never the batch's member jobs.
  let(:worker_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def perform(*); end
    end
  end

  let(:on_finish_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BatchFlowSpec::OnFinishJob"
      def perform(*); end
    end
  end

  let(:on_success_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BatchFlowSpec::OnSuccessJob"
      def perform(*); end
    end
  end

  let(:on_discard_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BatchFlowSpec::OnDiscardJob"
      def perform(*); end
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("BatchFlowSpec", Module.new)
    stub_const("BatchFlowSpec::OnFinishJob", on_finish_job)
    stub_const("BatchFlowSpec::OnSuccessJob", on_success_job)
    stub_const("BatchFlowSpec::OnDiscardJob", on_discard_job)
    client.ensure_queue(callback_queue)
    client.ensure_queue(work_queue)
  end

  # Drain the callback queue and return the ActiveJob class names of every
  # message on it, deleting each as it is read so a re-read never sees it again.
  def drain_callback_job_classes
    classes = []
    while (message = client.read_message(callback_queue, vt: 30))
      classes << JSON.parse(message.message).fetch("job_class")
      client.delete_message(callback_queue, message.msg_id.to_i)
    end
    classes
  end

  # The class name of the single callback job currently on the queue, or nil.
  def next_callback_job_class
    drain_callback_job_classes.first
  end

  def enqueue_batch(**callbacks)
    batch = Pgbus::Batch.new(**callbacks)
    batch.enqueue do
      worker_job.perform_later
      worker_job.perform_later
    end
    batch
  end

  describe "on_success path" do
    it "fires the callback exactly once, only on the final completion" do
      batch = enqueue_batch(on_finish: on_finish_job, on_success: on_success_job)

      # Two jobs enqueued means total_jobs == 2 and status == processing.
      record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(record.total_jobs).to eq(2)
      expect(record.status).to eq("processing")

      # First completion: not finished yet, no callback enqueued.
      Pgbus::Batch.job_completed(batch.batch_id)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("processing")

      # Final completion: batch finishes and callbacks enqueue.
      Pgbus::Batch.job_completed(batch.batch_id)
      finished = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(finished.status).to eq("finished")
      expect(finished.finished_at).not_to be_nil

      # on_finish + on_success both land on the callback queue (all succeeded).
      expect(drain_callback_job_classes).to contain_exactly(
        "BatchFlowSpec::OnFinishJob",
        "BatchFlowSpec::OnSuccessJob"
      )
    end

    it "is idempotent — an extra completion signal fires no second callback" do
      batch = enqueue_batch(on_finish: on_finish_job)

      2.times { Pgbus::Batch.job_completed(batch.batch_id) }
      # Drain the single expected on_finish callback.
      expect(next_callback_job_class).to eq("BatchFlowSpec::OnFinishJob")

      # A stray extra completion after finish must not enqueue anything more.
      Pgbus::Batch.job_completed(batch.batch_id)
      expect(next_callback_job_class).to be_nil
    end
  end

  describe "bulk enqueue via perform_all_later (issue #413)" do
    it "counts bulk-enqueued jobs into the batch and tags their payloads" do
      batch = Pgbus::Batch.new(on_finish: on_finish_job)
      batch.enqueue do
        ActiveJob.perform_all_later([worker_job.new, worker_job.new])
      end

      record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(record.total_jobs).to eq(2)
      expect(record.status).to eq("processing")

      # Every bulk payload on the work queue carries the batch id.
      batch_ids = []
      while (message = client.read_message(work_queue, vt: 30))
        batch_ids << JSON.parse(message.message)[Pgbus::Batch::METADATA_KEY]
        client.delete_message(work_queue, message.msg_id.to_i)
      end
      expect(batch_ids).to eq([batch.batch_id, batch.batch_id])

      # The batch finishes only after BOTH bulk jobs complete.
      Pgbus::Batch.job_completed(batch.batch_id)
      expect(next_callback_job_class).to be_nil
      Pgbus::Batch.job_completed(batch.batch_id)
      expect(next_callback_job_class).to eq("BatchFlowSpec::OnFinishJob")
    end
  end

  describe "on_discard path" do
    it "fires on_discard instead of on_success when a job is discarded" do
      batch = enqueue_batch(on_success: on_success_job, on_discard: on_discard_job)

      # One completes, one is discarded (dead-lettered) — batch still finishes.
      Pgbus::Batch.job_completed(batch.batch_id)
      Pgbus::Batch.job_discarded(batch.batch_id)

      finished = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(finished.status).to eq("finished")
      expect(finished.discarded_jobs).to eq(1)

      expect(drain_callback_job_classes).to contain_exactly("BatchFlowSpec::OnDiscardJob")
    end
  end
end
