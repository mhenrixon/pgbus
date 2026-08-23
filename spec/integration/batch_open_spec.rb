# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Batches v2 (issue #415): re-callable enqueue, the `batch` accessor on jobs and
# callbacks, and configured ActiveJob instances as callbacks — all against a
# real database and real PGMQ queues.
RSpec.describe "Open batches (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:work_queue) { "open_batch_work" }
  let(:callback_queue) { "open_batch_callbacks" }

  let(:worker_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      include Pgbus::ActiveJob::BatchId

      def self.name = "OpenBatchSpec::WorkerJob"
      def perform(*); end
    end
  end

  let(:callback_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      include Pgbus::ActiveJob::BatchId

      def self.name = "OpenBatchSpec::CallbackJob"
      def perform(*); end
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("OpenBatchSpec", Module.new)
    stub_const("OpenBatchSpec::WorkerJob", worker_job)
    stub_const("OpenBatchSpec::CallbackJob", callback_job)
    client.ensure_queue(work_queue)
    client.ensure_queue(callback_queue)
  end

  def complete_next(batch_id)
    row = Pgbus::BatchExecution.where(batch_id: batch_id).order(:id).first
    raise "no execution row for #{batch_id}" unless row

    Pgbus::Batch.job_completed(batch_id, job_id: row.job_id)
  end

  def queue_depth(queue)
    physical = Pgbus.configuration.queue_name(queue)
    ActiveRecord::Base.connection.select_value("SELECT count(*) FROM pgmq.q_#{physical}").to_i
  end

  def drain_callbacks
    classes = []
    while (message = client.read_message(callback_queue, vt: 30))
      classes << JSON.parse(message.message)
      client.delete_message(callback_queue, message.msg_id.to_i)
    end
    classes
  end

  describe "re-callable #enqueue" do
    it "adds to total_jobs instead of creating a second record" do
      batch = Pgbus::Batch.new
      batch.enqueue { worker_job.perform_later }

      batch.enqueue do
        worker_job.perform_later
        worker_job.perform_later
      end

      record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(record.total_jobs).to eq(3)
      expect(record.status).to eq("processing")
      expect(Pgbus::BatchEntry.where(batch_id: batch.batch_id).count).to eq(1)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(3)
    end

    it "is a no-op when the re-opened block enqueues nothing" do
      batch = Pgbus::Batch.new
      batch.enqueue { worker_job.perform_later }

      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).total_jobs).to eq(1)
    end

    it "raises AlreadyFinished once the batch has finished" do
      batch = Pgbus::Batch.new
      batch.enqueue { worker_job.perform_later }
      complete_next(batch.batch_id)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("finished")

      expect { batch.enqueue { worker_job.perform_later } }
        .to raise_error(Pgbus::Batch::AlreadyFinished)
    end

    # The guard is per job, at perform_later, before the message is sent —
    # even when the handle's own view of the batch is stale (issue #423).
    it "raises at perform_later and sends nothing when the batch finished under a stale handle" do
      batch = Pgbus::Batch.new
      batch.enqueue { worker_job.perform_later }
      stale = Pgbus::Batch.find(batch.batch_id)
      complete_next(batch.batch_id)
      depth_before = queue_depth(work_queue)

      expect { stale.enqueue { worker_job.perform_later } }
        .to raise_error(Pgbus::Batch::AlreadyFinished)

      expect(queue_depth(work_queue)).to eq(depth_before)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(0)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).total_jobs).to eq(1)
    end

    it "keeps total_jobs consistent when a re-opened block raises mid-way" do
      batch = Pgbus::Batch.new
      batch.enqueue { worker_job.perform_later }

      expect do
        batch.enqueue do
          worker_job.perform_later
          raise "boom"
        end
      end.to raise_error("boom")

      record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(record.total_jobs).to eq(2)
      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(2)

      complete_next(batch.batch_id)
      complete_next(batch.batch_id)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("finished")
    end

    it "counts a bulk perform_all_later with a single total_jobs update" do
      batch = Pgbus::Batch.new
      updates = 0
      subscriber = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
        updates += 1 if payload[:sql].include?("total_jobs = total_jobs +")
      end

      batch.enqueue { ActiveJob.perform_all_later([worker_job.new, worker_job.new, worker_job.new]) }

      ActiveSupport::Notifications.unsubscribe(subscriber)
      expect(updates).to eq(1)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).total_jobs).to eq(3)
    end

    it "keeps the batch open until the jobs added in the second block finish" do
      batch = Pgbus::Batch.new(on_finish: callback_job)
      batch.enqueue { worker_job.perform_later }
      batch.enqueue { worker_job.perform_later }

      complete_next(batch.batch_id)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("processing")

      complete_next(batch.batch_id)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("finished")
      expect(drain_callbacks.map { |p| p["job_class"] }).to eq(["OpenBatchSpec::CallbackJob"])
    end
  end

  describe ".find" do
    it "returns a handle a running job can add siblings through" do
      batch = Pgbus::Batch.new(description: "stage one", properties: { "tenant" => "acme" })
      batch.enqueue { worker_job.perform_later }

      handle = Pgbus::Batch.find(batch.batch_id)

      expect(handle).to be_a(Pgbus::Batch)
      expect(handle.description).to eq("stage one")
      expect(handle.properties).to eq({ "tenant" => "acme" })
      expect(handle.total_jobs).to eq(1)
      expect(handle.pending_jobs).to eq(1)
      expect(handle).not_to be_finished

      handle.enqueue { worker_job.perform_later }

      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).total_jobs).to eq(2)
    end
  end

  describe "the batch accessor inside a running job" do
    it "reaches the job's own batch through the payload metadata" do
      batch = Pgbus::Batch.new
      batch.enqueue { worker_job.perform_later }

      message = client.read_message(work_queue, vt: 30)
      payload = JSON.parse(message.message)
      job = ActiveJob::Base.deserialize(payload)
      job.batch_id = payload[Pgbus::Batch::METADATA_KEY]

      expect(job.batch).to be_a(Pgbus::Batch)
      expect(job.batch.batch_id).to eq(batch.batch_id)
    end
  end

  describe "configured callback instances" do
    it "enqueues on the configured queue and reports on the finished batch" do
      configured = callback_job.new.set(queue: callback_queue)
      batch = Pgbus::Batch.new(on_finish: configured, properties: { "tenant" => "acme" })
      batch.enqueue { worker_job.perform_later }

      complete_next(batch.batch_id)

      payload = drain_callbacks.first
      expect(payload["job_class"]).to eq("OpenBatchSpec::CallbackJob")
      expect(payload["queue_name"]).to eq(callback_queue)
      expect(payload["callback_batch_id"]).to eq(batch.batch_id)
      expect(payload["batch_id"]).to be_nil
      expect(payload).not_to have_key(Pgbus::Batch::METADATA_KEY)

      callback = ActiveJob::Base.deserialize(payload)
      expect(callback.batch.properties).to eq({ "tenant" => "acme" })
      expect(callback.batch.completed_jobs).to eq(1)
    end

    it "still fires a bare callback class with the properties hash" do
      batch = Pgbus::Batch.new(on_finish: callback_job, properties: { "tenant" => "acme" })
      batch.enqueue { worker_job.perform_later }

      complete_next(batch.batch_id)

      payload = drain_callbacks.first
      expect(payload["job_class"]).to eq("OpenBatchSpec::CallbackJob")
      expect(payload["arguments"].first).to include("tenant" => "acme")
    end
  end
end
