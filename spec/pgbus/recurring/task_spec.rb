# frozen_string_literal: true

require "spec_helper"
require "fugit"

RSpec.describe Pgbus::Recurring::Task do
  describe ".from_configuration" do
    it "creates a task from YAML-style config" do
      task = described_class.from_configuration("daily_cleanup",
                                                class: "CleanupJob",
                                                schedule: "0 2 * * *",
                                                queue: "maintenance",
                                                args: [1000],
                                                priority: 5,
                                                description: "Daily cleanup task")

      expect(task.key).to eq("daily_cleanup")
      expect(task.class_name).to eq("CleanupJob")
      expect(task.schedule).to eq("0 2 * * *")
      expect(task.queue_name).to eq("maintenance")
      expect(task.arguments).to eq([1000])
      expect(task.priority).to eq(5)
      expect(task.description).to eq("Daily cleanup task")
    end

    it "accepts string keys for config hash" do
      task = described_class.from_configuration("sync",
                                                "class" => "SyncJob",
                                                "schedule" => "every hour")

      expect(task.class_name).to eq("SyncJob")
      expect(task.schedule).to eq("every hour")
    end

    it "defaults queue_name to nil when not specified" do
      task = described_class.from_configuration("task1",
                                                class: "MyJob",
                                                schedule: "every hour")

      expect(task.queue_name).to be_nil
    end

    it "defaults arguments to empty array" do
      task = described_class.from_configuration("task1",
                                                class: "MyJob",
                                                schedule: "every hour")

      expect(task.arguments).to eq([])
    end

    it "defaults priority to 0" do
      task = described_class.from_configuration("task1",
                                                class: "MyJob",
                                                schedule: "every hour")

      expect(task.priority).to eq(0)
    end

    it "supports command-based tasks" do
      task = described_class.from_configuration("cleanup_cmd",
                                                command: "OldRecord.where('created_at < ?', 30.days.ago).delete_all",
                                                schedule: "0 3 * * *")

      expect(task.command).to eq("OldRecord.where('created_at < ?', 30.days.ago).delete_all")
      expect(task.class_name).to be_nil
    end
  end

  describe "#valid?" do
    it "is valid with class and schedule" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "0 * * * *")

      expect(task).to be_valid
    end

    it "is valid with command and schedule" do
      task = described_class.from_configuration("t1",
                                                command: "puts 'hello'",
                                                schedule: "0 * * * *")

      expect(task).to be_valid
    end

    it "is invalid without schedule" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: nil)

      expect(task).not_to be_valid
      expect(task.errors).to include(/schedule/i)
    end

    it "is invalid without class or command" do
      task = described_class.from_configuration("t1",
                                                schedule: "0 * * * *")

      expect(task).not_to be_valid
      expect(task.errors).to include(/class.*command/i)
    end

    it "is invalid with unparseable schedule" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "not a valid cron")

      expect(task).not_to be_valid
      expect(task.errors).to include(/schedule/i)
    end

    it "rejects duration-style schedules (every 1.minute)" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "every 1.minute")

      expect(task).not_to be_valid
    end
  end

  describe "#parsed_schedule" do
    it "returns a Fugit::Cron for standard cron" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "0 2 * * *")

      expect(task.parsed_schedule).to be_a(Fugit::Cron)
    end

    it "parses natural language schedules" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "every day at 2am")

      expect(task.parsed_schedule).to be_a(Fugit::Cron)
    end

    it "returns nil for invalid schedules" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "garbage")

      expect(task.parsed_schedule).to be_nil
    end
  end

  describe "#next_time" do
    it "returns the next occurrence from now" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "0 * * * *")

      next_time = task.next_time
      expect(next_time).to be > Time.now
      expect(next_time.min).to eq(0)
    end

    it "returns next occurrence from a given time" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "0 12 * * *")

      from = Time.utc(2026, 1, 1, 0, 0, 0)
      next_time = task.next_time(from)
      expect(next_time.hour).to eq(12)
      expect(next_time.day).to eq(1)
    end
  end

  describe "#previous_time" do
    it "returns the previous occurrence" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "0 * * * *")

      prev_time = task.previous_time
      expect(prev_time).to be < Time.now
      expect(prev_time.min).to eq(0)
    end
  end

  describe "#human_schedule" do
    it "returns a human-readable schedule description" do
      task = described_class.from_configuration("t1",
                                                class: "MyJob",
                                                schedule: "0 2 * * *")

      expect(task.human_schedule).to be_a(String)
      expect(task.human_schedule).not_to be_empty
    end
  end

  describe "#job_class" do
    it "constantizes the class_name" do
      stub_const("CleanupJob", Class.new)
      task = described_class.from_configuration("t1",
                                                class: "CleanupJob",
                                                schedule: "0 * * * *")

      expect(task.job_class).to eq(CleanupJob)
    end

    it "returns nil for unknown class" do
      task = described_class.from_configuration("t1",
                                                class: "NonExistentJob",
                                                schedule: "0 * * * *")

      expect(task.job_class).to be_nil
    end
  end

  describe "#to_h" do
    it "returns a hash representation" do
      task = described_class.from_configuration("daily",
                                                class: "CleanupJob",
                                                schedule: "0 2 * * *",
                                                queue: "maintenance",
                                                args: [100],
                                                priority: 3,
                                                description: "Daily cleanup")

      h = task.to_h
      expect(h[:key]).to eq("daily")
      expect(h[:class_name]).to eq("CleanupJob")
      expect(h[:schedule]).to eq("0 2 * * *")
      expect(h[:queue_name]).to eq("maintenance")
      expect(h[:arguments]).to eq([100])
      expect(h[:priority]).to eq(3)
      expect(h[:description]).to eq("Daily cleanup")
    end
  end
end
