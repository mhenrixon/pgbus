# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Recurring scheduler with :while_executing strategy (integration)", :integration do
  let(:config) { Pgbus.configuration }
  let(:schedule) { Pgbus::Recurring::Schedule.new(config: config) }
  let(:run_at) { Time.utc(2026, 4, 1, 2, 0, 0) }

  let(:while_executing_job_class) do
    Class.new do
      include Pgbus::Uniqueness

      def self.name
        "WhileExecutingJob"
      end

      ensures_uniqueness strategy: :while_executing, lock_ttl: 3600
    end
  end

  before do
    stub_const("WhileExecutingJob", while_executing_job_class)

    config.recurring_tasks = {
      "while_exec_task" => {
        "class" => "WhileExecutingJob",
        "schedule" => "*/5 * * * *",
        "queue" => "default"
      }
    }
  end

  describe "scheduler does not acquire locks for :while_executing jobs" do
    it "enqueues without creating a job lock" do
      task = schedule.tasks.first

      schedule.enqueue_task(task, run_at: run_at)

      # No lock should exist — :while_executing locks are only acquired at execution time
      expect(Pgbus::UniquenessKey.count).to eq(0)

      # Message should be in the queue
      messages = Pgbus.client.read_batch("default", qty: 10)
      expect(messages.size).to eq(1)

      payload = JSON.parse(messages.first.message)
      expect(payload["job_class"]).to eq("WhileExecutingJob")
    end

    it "does not inject uniqueness metadata into the payload" do
      task = schedule.tasks.first

      schedule.enqueue_task(task, run_at: run_at)

      messages = Pgbus.client.read_batch("default", qty: 1)
      payload = JSON.parse(messages.first.message)

      # while_executing strategy should NOT have uniqueness metadata injected
      # by the scheduler — the executor handles lock acquisition at runtime
      expect(payload).not_to have_key(Pgbus::Uniqueness::METADATA_KEY)
      expect(payload).not_to have_key(Pgbus::Uniqueness::STRATEGY_KEY)
      expect(payload).not_to have_key(Pgbus::Uniqueness::TTL_KEY)
    end

    it "allows multiple enqueues since no lock prevents it" do
      task = schedule.tasks.first

      schedule.enqueue_task(task, run_at: run_at)
      schedule.enqueue_task(task, run_at: Time.utc(2026, 4, 1, 2, 5, 0))

      messages = Pgbus.client.read_batch("default", qty: 10, vt: 0)
      expect(messages.size).to eq(2)

      expect(Pgbus::UniquenessKey.count).to eq(0)
    end
  end
end
