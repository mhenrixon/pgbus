# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Reproduces the :while_executing retry lock-out (issue #423, F5).
#
# The :while_executing lock is acquired at execution start. Before this fix it
# was released only on success or DLQ, so a failed attempt left its own row in
# pgbus_uniqueness_keys — and the retry of the SAME message then conflicted
# with that row at acquire_execution_lock, was :skipped, and so on every read
# until read_ct exceeded max_retries and the job dead-lettered without ever
# getting a second real attempt.
RSpec.describe ":while_executing + failed attempt (integration)", :integration do
  before do
    Pgbus::UniquenessKey.delete_all
    ActiveJob::Base.queue_adapter = :pgbus
  end

  after { ActiveJob::Base.queue_adapter = :test }

  def build_flaky_job(attempts_tracker)
    Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness

      ensures_uniqueness strategy: :while_executing, key: ->(id) { "flaky-#{id}" }

      define_method(:perform) do |order_id|
        attempts_tracker << order_id
        raise "boom" if attempts_tracker.size == 1
      end
    end
  end

  it "lets the same message retry to success after a failed attempt releases the lock" do
    attempts = []
    stub_const("FlakyWhileExecutingJob", build_flaky_job(attempts))
    client = Pgbus.client
    executor = Pgbus::ActiveJob::Executor.new(client: client)

    FlakyWhileExecutingJob.perform_later(5)
    message = client.read_message("default", vt: 30)
    expect(executor.execute(message, "default")).to eq(:failed)

    # A failed attempt is no longer "executing" — the lock must be gone.
    expect(Pgbus::UniquenessKey.locked?("flaky-5")).to be(false)

    # Make the message visible again and retry it.
    client.set_visibility_timeout("default", message.msg_id.to_i, vt: 0)
    retry_message = client.read_message("default", vt: 30)
    expect(retry_message.msg_id).to eq(message.msg_id)

    expect(executor.execute(retry_message, "default")).to eq(:success)
    expect(attempts).to eq([5, 5])
    expect(Pgbus::UniquenessKey.locked?("flaky-5")).to be(false)
  end

  it "lets the same message re-acquire a lock left behind by a crashed attempt" do
    attempts = []
    stub_const("FlakyWhileExecutingJob", build_flaky_job(attempts))
    attempts << :primed # make the first real attempt succeed
    client = Pgbus.client
    executor = Pgbus::ActiveJob::Executor.new(client: client)

    FlakyWhileExecutingJob.perform_later(6)
    message = client.read_message("default", vt: 30)
    # Simulate a process kill mid-execution: the row from THIS message remains.
    Pgbus::UniquenessKey.acquire!("flaky-6", queue_name: "default", msg_id: message.msg_id.to_i)

    expect(executor.execute(message, "default")).to eq(:success)
    expect(attempts).to eq([:primed, 6])
    expect(Pgbus::UniquenessKey.locked?("flaky-6")).to be(false)
  end

  it "still skips when a different message holds the lock" do
    attempts = []
    stub_const("FlakyWhileExecutingJob", build_flaky_job(attempts))
    client = Pgbus.client
    executor = Pgbus::ActiveJob::Executor.new(client: client)

    FlakyWhileExecutingJob.perform_later(7)
    message = client.read_message("default", vt: 30)
    Pgbus::UniquenessKey.acquire!("flaky-7", queue_name: "default", msg_id: message.msg_id.to_i + 1_000)

    expect(executor.execute(message, "default")).to eq(:skipped)
    expect(attempts).to be_empty
  end
end
