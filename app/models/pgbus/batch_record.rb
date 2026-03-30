# frozen_string_literal: true

module Pgbus
  class BatchRecord < ApplicationRecord
    self.table_name = "pgbus_batches"

    scope :finished, -> { where(status: "finished") }
    scope :stale, ->(before:) { finished.where("finished_at < ?", before) }

    # Atomic counter increment with transition detection.
    # Returns the row hash (with "just_finished" flag) or nil.
    def self.increment_counter!(batch_id, column)
      result = connection.exec_query(
        <<~SQL,
          UPDATE pgbus_batches
          SET #{column} = #{column} + 1,
              status = CASE
                WHEN completed_jobs + discarded_jobs + 1 = total_jobs THEN 'finished'
                ELSE status
              END,
              finished_at = CASE
                WHEN completed_jobs + discarded_jobs + 1 = total_jobs THEN NOW()
                ELSE finished_at
              END
          WHERE batch_id = $1
          RETURNING status, total_jobs, completed_jobs, discarded_jobs,
                    on_finish_class, on_success_class, on_discard_class, properties,
                    (completed_jobs + discarded_jobs = total_jobs) AS just_finished
        SQL
        "Pgbus Batch Counter",
        [batch_id]
      )

      result.first
    end
  end
end
