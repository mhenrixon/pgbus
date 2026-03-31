# frozen_string_literal: true

module Pgbus
  class BatchEntry < BaseModel
    self.table_name = "pgbus_batches"

    COUNTER_COLUMNS = %w[completed_jobs discarded_jobs].freeze

    scope :finished, -> { where(status: "finished") }
    scope :stale, ->(before:) { finished.where("finished_at < ?", before) }

    # Atomically increment the counter and detect if this update caused the
    # batch to finish. Uses row-level locking to prevent duplicate callbacks.
    # Returns { just_finished:, record: } or nil if batch not found.
    def self.increment_counter!(batch_id, column)
      raise ArgumentError, "Invalid column: #{column}" unless COUNTER_COLUMNS.include?(column)

      transaction do
        record = lock.find_by(batch_id: batch_id)
        return nil unless record

        record.increment!(column)

        just_finished = record.completed_jobs + record.discarded_jobs == record.total_jobs
        record.update!(status: "finished", finished_at: Time.current) if just_finished && record.status != "finished"

        { record: record, just_finished: just_finished && record.status == "finished" }
      end
    end
  end
end
