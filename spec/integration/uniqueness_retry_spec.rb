# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Reproduces the retry_on × :until_executed key-lifecycle bug (issue #333).
#
# The uniqueness key is released only on success or DLQ. With retry_on, ActiveJob
# re-enqueues the job from INSIDE perform_now (after incrementing executions),
# BEFORE the executor releases the key — so the retry re-enqueue hit the still-held
# key, was rejected as a duplicate (JobNotUnique under on_conflict: :reject), and
# the original message dead-lettered. A retry is the same logical job re-acquiring
# its OWN key and must be allowed.
RSpec.describe "Uniqueness + retry_on lifecycle (integration)", :integration do
  before do
    Pgbus::UniquenessKey.delete_all
    ActiveJob::Base.queue_adapter = :pgbus
  end

  # A job that fails on its first execution and succeeds on the second, guarded
  # by :until_executed uniqueness with an explicit per-arg key.
  def build_retrying_job(attempts_tracker)
    Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness

      ensures_uniqueness strategy: :until_executed, key: ->(id) { "retry-order-#{id}" }, on_conflict: :reject

      retry_on RuntimeError, attempts: 3, wait: 0

      define_method(:perform) do |order_id|
        attempts_tracker << order_id
        raise "boom" if attempts_tracker.size == 1 # fail only the first attempt
      end
    end
  end

  it "runs a retried :until_executed job to success without JobNotUnique or DLQ, and cleans up the key" do
    attempts = []
    job_class = build_retrying_job(attempts)
    stub_const("RetryOrderJob", job_class)

    client = Pgbus.client
    executor = Pgbus::ActiveJob::Executor.new(client: client)

    # Enqueue — acquires the uniqueness key at enqueue time.
    RetryOrderJob.perform_later(77)
    expect(Pgbus::UniquenessKey.locked?("retry-order-77")).to be(true)

    # Drive attempts. Each execute() runs one perform; retry_on re-enqueues the
    # job on the first (failing) attempt, then it succeeds on the second read.
    5.times do
      messages = client.read_batch("default", qty: 5, vt: 30)
      break if messages.empty?

      messages.each { |m| executor.execute(m, "default") }
    end

    # The job ran twice (failed once, succeeded once) and was NOT lost to DLQ.
    expect(attempts).to eq([77, 77])

    # No orphaned uniqueness key after successful completion.
    expect(Pgbus::UniquenessKey.locked?("retry-order-77")).to be(false)

    # Nothing landed in the dead-letter queue.
    dlq_metrics =
      begin
        client.metrics("default_dlq")
      rescue StandardError
        nil
      end
    dlq_len = dlq_metrics.respond_to?(:queue_length) ? dlq_metrics.queue_length : dlq_metrics&.fetch(:queue_length, 0)
    expect(dlq_len.to_i).to eq(0)
  ensure
    ActiveJob::Base.queue_adapter = :test if defined?(ActiveJob::Base)
  end
end
