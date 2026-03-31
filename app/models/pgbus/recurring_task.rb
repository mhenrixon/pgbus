# frozen_string_literal: true

module Pgbus
  class RecurringTask < BaseModel
    self.table_name = "pgbus_recurring_tasks"

    validates :key, presence: true, uniqueness: true
    validates :schedule, presence: true

    scope :static_tasks, -> { where(static: true) }
    scope :enabled, -> { where(enabled: true) }
    scope :disabled, -> { where(enabled: false) }

    # Sync static tasks from configuration.
    # Creates new tasks, updates existing ones, removes stale ones.
    def self.sync_from_config!(tasks_hash)
      transaction do
        task_keys = tasks_hash.keys

        # Upsert all configured tasks
        tasks_hash.each do |key, options|
          options = options.transform_keys(&:to_s)
          record = find_or_initialize_by(key: key)
          record.assign_attributes(
            class_name: options["class"],
            command: options["command"],
            schedule: options["schedule"],
            queue_name: options["queue"],
            arguments: options["args"],
            priority: options.fetch("priority", 0).to_i,
            description: options["description"],
            static: true
          )
          record.save!
        end

        # Remove static tasks no longer in config
        static_tasks.where.not(key: task_keys).delete_all
      end
    end
  end
end
