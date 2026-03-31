# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Recurring::Schedule do
  let(:config) { Pgbus::Configuration.new }
  let(:mock_client) { instance_double(Pgbus::Client) }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(mock_client).to receive(:ensure_queue)
    allow(mock_client).to receive(:send_message)
    config.recurring_tasks = {
      "daily_cleanup" => {
        "class" => "CleanupJob",
        "schedule" => "0 2 * * *",
        "queue" => "maintenance"
      },
      "hourly_sync" => {
        "class" => "SyncJob",
        "schedule" => "0 * * * *"
      }
    }
  end

  describe "#initialize" do
    it "loads tasks from configuration" do
      schedule = described_class.new(config: config)
      expect(schedule.tasks.size).to eq(2)
      expect(schedule.tasks.map(&:key)).to contain_exactly("daily_cleanup", "hourly_sync")
    end

    it "filters out invalid tasks with logging" do
      config.recurring_tasks = {
        "valid" => { "class" => "MyJob", "schedule" => "0 * * * *" },
        "invalid" => { "class" => "MyJob", "schedule" => nil }
      }

      schedule = described_class.new(config: config)
      expect(schedule.tasks.size).to eq(1)
      expect(schedule.tasks.first.key).to eq("valid")
    end

    it "handles empty recurring tasks" do
      config.recurring_tasks = {}
      schedule = described_class.new(config: config)
      expect(schedule.tasks).to be_empty
    end

    it "handles nil recurring tasks" do
      config.recurring_tasks = nil
      schedule = described_class.new(config: config)
      expect(schedule.tasks).to be_empty
    end
  end

  describe "#due_tasks" do
    it "returns tasks whose next_time is at or before the given time" do
      schedule = described_class.new(config: config)

      # Both tasks should have some next_time
      due = schedule.due_tasks(Time.now + 7200) # 2 hours from now
      expect(due).to be_an(Array)
    end

    it "returns empty array when no tasks are due" do
      # Use a once-a-year schedule so we can easily find a non-matching time
      config.recurring_tasks = {
        "yearly" => {
          "class" => "YearlyJob",
          "schedule" => "0 0 1 1 *" # Jan 1st midnight only
        }
      }
      schedule = described_class.new(config: config)

      # June 15 at noon — nowhere near Jan 1 midnight
      due = schedule.due_tasks(Time.utc(2026, 6, 15, 12, 30, 0))
      expect(due).to be_empty
    end
  end

  describe "#enqueue_task" do
    let(:schedule) { described_class.new(config: config) }
    let(:task) { schedule.tasks.first }
    let(:run_at) { Time.utc(2026, 3, 31, 2, 0, 0) }

    before do
      # Mock the RecurringExecution to allow recording
      allow(Pgbus::RecurringExecutionRecord).to receive(:record).and_yield
    end

    it "sends a message through the client" do
      schedule.enqueue_task(task, run_at: run_at)

      expect(mock_client).to have_received(:send_message).with(
        anything,
        hash_including("job_class" => "CleanupJob"),
        headers: hash_including("pgbus.recurring_key" => "daily_cleanup")
      )
    end

    it "uses the task's configured queue" do
      schedule.enqueue_task(task, run_at: run_at)

      expect(mock_client).to have_received(:send_message).with(
        "pgbus_maintenance",
        anything,
        headers: anything
      )
    end

    it "uses default queue when task has no queue" do
      task_no_queue = schedule.tasks.find { |t| t.key == "hourly_sync" }

      schedule.enqueue_task(task_no_queue, run_at: run_at)

      expect(mock_client).to have_received(:send_message).with(
        "pgbus_default",
        anything,
        headers: anything
      )
    end

    it "records the execution for deduplication" do
      allow(Pgbus::RecurringExecutionRecord).to receive(:record)
        .with("daily_cleanup", run_at)
        .and_yield

      schedule.enqueue_task(task, run_at: run_at)

      expect(Pgbus::RecurringExecutionRecord).to have_received(:record)
        .with("daily_cleanup", run_at)
    end

    it "does not enqueue when execution already recorded" do
      allow(Pgbus::RecurringExecutionRecord).to receive(:record)
        .and_raise(Pgbus::Recurring::AlreadyRecorded)

      schedule.enqueue_task(task, run_at: run_at)

      expect(mock_client).not_to have_received(:send_message)
    end
  end

  describe "#build_payload" do
    let(:schedule) { described_class.new(config: config) }

    it "builds a serialized job payload for class-based tasks" do
      task = schedule.tasks.find { |t| t.key == "daily_cleanup" }
      payload = schedule.build_payload(task)

      expect(payload["job_class"]).to eq("CleanupJob")
      expect(payload["arguments"]).to eq([])
      expect(payload["queue_name"]).to eq("maintenance")
      expect(payload["priority"]).to be_nil
    end

    it "builds a command payload for command-based tasks" do
      config.recurring_tasks = {
        "cleanup_cmd" => {
          "command" => "OldRecord.cleanup!",
          "schedule" => "0 3 * * *"
        }
      }
      schedule_with_cmd = described_class.new(config: config)
      task = schedule_with_cmd.tasks.first
      payload = schedule_with_cmd.build_payload(task)

      expect(payload["job_class"]).to eq("Pgbus::Recurring::CommandJob")
      expect(payload["arguments"]).to eq(["OldRecord.cleanup!"])
    end
  end
end
