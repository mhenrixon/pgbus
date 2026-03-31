# frozen_string_literal: true

module Pgbus
  class RecurringExecution < ApplicationRecord
    self.table_name = "pgbus_recurring_executions"

    validates :task_key, presence: true
    validates :run_at, presence: true

    scope :for_task, ->(key) { where(task_key: key) }
    scope :recent, ->(limit = 50) { order(run_at: :desc).limit(limit) }
    scope :older_than, ->(time) { where("run_at < ?", time) }

    # Record a recurring execution with deduplication.
    # Uses the unique index on (task_key, run_at) to prevent duplicates.
    # Yields to the caller to perform the actual enqueue.
    def self.record(task_key, run_at)
      transaction do
        execution = create!(task_key: task_key, run_at: run_at)
        yield execution if block_given?
        execution
      end
    rescue ActiveRecord::RecordNotUnique
      raise Pgbus::Recurring::AlreadyRecorded,
            "Recurring task '#{task_key}' already recorded for #{run_at.iso8601}"
    end

    # Find the most recent execution for a task
    def self.last_execution(task_key)
      for_task(task_key).order(run_at: :desc).first
    end
  end
end
