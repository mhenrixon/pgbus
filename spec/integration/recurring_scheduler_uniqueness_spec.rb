# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Recurring scheduler uniqueness (integration)", :integration do
  let(:config) { Pgbus.configuration }
  let(:schedule) { Pgbus::Recurring::Schedule.new(config: config) }
  let(:run_at) { Time.utc(2026, 4, 1, 2, 0, 0) }

  let(:unique_job_class) do
    Class.new do
      include Pgbus::Uniqueness

      def self.name
        "UniqueRecurringJob"
      end

      ensures_uniqueness strategy: :until_executed, lock_ttl: 3600
    end
  end

  before do
    stub_const("UniqueRecurringJob", unique_job_class)

    config.recurring_tasks = {
      "unique_cleanup" => {
        "class" => "UniqueRecurringJob",
        "schedule" => "0 2 * * *",
        "queue" => "maintenance"
      }
    }
  end

  describe "first enqueue acquires lock and sends message" do
    it "creates a job lock row and enqueues the message" do
      task = schedule.tasks.first

      schedule.enqueue_task(task, run_at: run_at)

      # Lock should exist in the database
      lock = Pgbus::JobLock.find_by(lock_key: "UniqueRecurringJob")
      expect(lock).to be_present
      expect(lock.state).to eq("queued")
      expect(lock.job_class).to eq("UniqueRecurringJob")
      expect(lock.job_id).to eq("recurring-unique_cleanup")

      # Message should be in the queue
      messages = Pgbus.client.read_batch("maintenance", qty: 10)
      expect(messages.size).to eq(1)

      payload = JSON.parse(messages.first.message)
      expect(payload["job_class"]).to eq("UniqueRecurringJob")
      expect(payload[Pgbus::Uniqueness::METADATA_KEY]).to eq("UniqueRecurringJob")
      expect(payload[Pgbus::Uniqueness::STRATEGY_KEY]).to eq("until_executed")
      expect(payload[Pgbus::Uniqueness::TTL_KEY]).to eq(3600)
    end
  end

  describe "duplicate enqueue is rejected when lock is held" do
    it "skips the second enqueue while the first lock is active" do
      task = schedule.tasks.first

      # First enqueue — should succeed
      schedule.enqueue_task(task, run_at: run_at)

      # Second enqueue with a different run_at — should be skipped due to lock
      second_run_at = Time.utc(2026, 4, 2, 2, 0, 0)
      schedule.enqueue_task(task, run_at: second_run_at)

      # Only one message should be in the queue
      messages = Pgbus.client.read_batch("maintenance", qty: 10)
      expect(messages.size).to eq(1)

      # Only one lock should exist
      locks = Pgbus::JobLock.where(lock_key: "UniqueRecurringJob")
      expect(locks.count).to eq(1)
    end
  end

  describe "enqueue succeeds after lock is released" do
    it "allows a new enqueue once the previous lock is gone" do
      task = schedule.tasks.first

      # First enqueue
      schedule.enqueue_task(task, run_at: run_at)

      # Simulate job completion — release the lock
      Pgbus::JobLock.release!("UniqueRecurringJob")

      # Second enqueue should now succeed
      second_run_at = Time.utc(2026, 4, 2, 2, 0, 0)
      schedule.enqueue_task(task, run_at: second_run_at)

      # Two messages should be in the queue (first is invisible due to VT, read again)
      # Read with VT=0 to see all
      messages = Pgbus.client.read_batch("maintenance", qty: 10, vt: 0)
      expect(messages.size).to eq(2)

      # New lock should exist
      lock = Pgbus::JobLock.find_by(lock_key: "UniqueRecurringJob")
      expect(lock).to be_present
    end
  end

  describe "expired lock is cleaned up on next acquire" do
    it "replaces an expired lock and enqueues successfully" do
      task = schedule.tasks.first

      # Manually create an expired lock (TTL in the past)
      Pgbus::JobLock.acquire!(
        "UniqueRecurringJob",
        job_class: "UniqueRecurringJob",
        job_id: "old-run",
        state: "executing",
        ttl: -1
      )

      # Enqueue should succeed because acquire! cleans up expired locks first
      schedule.enqueue_task(task, run_at: run_at)

      messages = Pgbus.client.read_batch("maintenance", qty: 10)
      expect(messages.size).to eq(1)

      lock = Pgbus::JobLock.find_by(lock_key: "UniqueRecurringJob")
      expect(lock.job_id).to eq("recurring-unique_cleanup")
      expect(lock.state).to eq("queued")
    end
  end

  describe "non-unique job class is not affected" do
    let(:plain_schedule) do
      plain_job = Class.new do
        def self.name
          "PlainRecurringJob"
        end
      end
      stub_const("PlainRecurringJob", plain_job)

      config.recurring_tasks = {
        "plain_task" => {
          "class" => "PlainRecurringJob",
          "schedule" => "0 3 * * *",
          "queue" => "default"
        }
      }

      Pgbus::Recurring::Schedule.new(config: config)
    end

    it "enqueues without acquiring a lock" do
      task = plain_schedule.tasks.first
      plain_schedule.enqueue_task(task, run_at: run_at)

      expect(Pgbus::JobLock.count).to eq(0)

      messages = Pgbus.client.read_batch("default", qty: 10)
      expect(messages.size).to eq(1)

      payload = JSON.parse(messages.first.message)
      expect(payload).not_to have_key(Pgbus::Uniqueness::METADATA_KEY)
    end
  end

  describe "uniqueness metadata in payload" do
    it "includes lock key, strategy, and TTL for downstream executor" do
      task = schedule.tasks.first

      schedule.enqueue_task(task, run_at: run_at)

      messages = Pgbus.client.read_batch("maintenance", qty: 1)
      payload = JSON.parse(messages.first.message)

      expect(payload[Pgbus::Uniqueness::METADATA_KEY]).to eq("UniqueRecurringJob")
      expect(payload[Pgbus::Uniqueness::STRATEGY_KEY]).to eq("until_executed")
      expect(payload[Pgbus::Uniqueness::TTL_KEY]).to eq(3600)
    end
  end

  describe "concurrent scheduler ticks" do
    it "only one tick wins the lock when racing" do
      results = Concurrent::Array.new
      barrier = Concurrent::CyclicBarrier.new(5)

      threads = 5.times.map do |i|
        Thread.new do
          # Each thread gets its own schedule instance to avoid shared state
          thread_schedule = Pgbus::Recurring::Schedule.new(config: config)
          thread_task = thread_schedule.tasks.first
          thread_run_at = Time.utc(2026, 4, 1, 2, 0, i)

          barrier.wait
          thread_schedule.enqueue_task(thread_task, run_at: thread_run_at)
          results << :done
        end
      end

      threads.each(&:join)

      # Only one lock should exist
      locks = Pgbus::JobLock.where(lock_key: "UniqueRecurringJob")
      expect(locks.count).to eq(1)

      # Only one message should have been enqueued
      messages = Pgbus.client.read_batch("maintenance", qty: 10, vt: 0)
      expect(messages.size).to eq(1)
    end
  end
end
