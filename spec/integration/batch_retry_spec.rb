# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #424: a retry_on re-enqueue must stay in its batch. Before, the
# original message signalled completion the moment perform_now returned (after
# retry_on had re-enqueued), and the retry message carried no batch tag — so
# on_success could fire while the retry was still pending, and a retry that
# later dead-lettered never fired on_failure.
RSpec.describe "Batch + retry_on lifecycle (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:work_queue) { "batch_retry_work" }
  let(:callback_queue) { "batch_retry_callbacks" }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }

  let(:on_success_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BatchRetrySpec::OnSuccessJob"
      def perform(*); end
    end
  end

  let(:on_failure_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BatchRetrySpec::OnFailureJob"
      def perform(*); end
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("BatchRetrySpec", Module.new)
    stub_const("BatchRetrySpec::OnSuccessJob", on_success_job)
    stub_const("BatchRetrySpec::OnFailureJob", on_failure_job)
    client.ensure_queue(work_queue)
    client.ensure_queue(callback_queue)
  end

  # Fails on the first N attempts, then succeeds.
  def build_retrying_job(attempts, fail_times:)
    queue = work_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      include Pgbus::ActiveJob::BatchId

      retry_on RuntimeError, attempts: 5, wait: 0
      def self.name = "BatchRetrySpec::FlakyJob"
      define_method(:perform) do |*|
        attempts << executions
        raise "boom" if attempts.size <= fail_times
      end
    end
  end

  def drain_callbacks
    classes = []
    while (message = client.read_message(callback_queue, vt: 30))
      classes << JSON.parse(message.message).fetch("job_class")
      client.delete_message(callback_queue, message.msg_id.to_i)
    end
    classes
  end

  def execute_next
    message = client.read_message(work_queue, vt: 30)
    raise "no message on #{work_queue}" unless message

    [executor.execute(message, work_queue), message]
  end

  it "keeps the batch open across a retry and finishes on the retry's success" do
    attempts = []
    stub_const("BatchRetrySpec::FlakyJob", build_retrying_job(attempts, fail_times: 1))
    batch = Pgbus::Batch.new(on_success: on_success_job, on_failure: on_failure_job)
    batch.enqueue { BatchRetrySpec::FlakyJob.perform_later }
    row = Pgbus::BatchExecution.find_by!(batch_id: batch.batch_id)
    original_msg_id = row.msg_id

    # Attempt 1: raises, retry_on re-enqueues, perform_now returns normally.
    result, = execute_next
    expect(result).to eq(:success)
    expect(attempts).to eq([1])

    record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
    expect(record.status).to eq("processing")
    expect(record.completed_jobs).to eq(0)
    rows = Pgbus::BatchExecution.where(batch_id: batch.batch_id)
    expect(rows.count).to eq(1)
    expect(rows.first.msg_id).not_to eq(original_msg_id) # re-pointed at the retry message
    expect(drain_callbacks).to be_empty

    # Attempt 2: succeeds.
    result, message = execute_next
    expect(result).to eq(:success)
    expect(message.msg_id.to_i).to eq(rows.first.msg_id)
    expect(attempts).to eq([1, 2])

    record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
    expect(record).to have_attributes(status: "finished", completed_jobs: 1, failed_jobs: 0, total_jobs: 1)
    expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(0)
    expect(drain_callbacks).to eq(["BatchRetrySpec::OnSuccessJob"])
  end

  it "fires on_failure, not on_success, when the retried job eventually dead-letters" do
    attempts = []
    stub_const("BatchRetrySpec::FlakyJob", build_retrying_job(attempts, fail_times: 100))
    batch = Pgbus::Batch.new(on_success: on_success_job, on_failure: on_failure_job)
    batch.enqueue { BatchRetrySpec::FlakyJob.perform_later }

    # retry_on attempts: 5 → four re-enqueues then the 5th attempt raises out
    # of perform_now; that message then fails until read_ct exceeds max_retries.
    4.times { expect(execute_next.first).to eq(:success) }
    expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("processing")

    # Last message: raise out → :failed; force redelivery until dead-lettered.
    loop do
      message = client.read_message(work_queue, vt: 30)
      break unless message

      result = executor.execute(message, work_queue)
      break if result == :dead_lettered

      client.set_visibility_timeout(work_queue, message.msg_id.to_i, vt: 0)
    end

    record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
    expect(record).to have_attributes(status: "finished", completed_jobs: 0, failed_jobs: 1)
    expect(drain_callbacks).to eq(["BatchRetrySpec::OnFailureJob"])
  end
end
