# frozen_string_literal: true

module Pgbus
  class BatchExecution < BusRecord
    self.table_name = "pgbus_batch_executions"

    # One row per outstanding batched job. Inserted before send_message so a
    # crash cannot produce an untracked in-flight job. unique_by: job_id makes
    # a retry re-enqueue of the same ActiveJob id a no-op.
    def self.insert_for!(batch_id:, job_id:)
      insert_all(
        [{ batch_id: batch_id, job_id: job_id, created_at: Time.current }],
        unique_by: :job_id
      )
    end

    def self.backfill!(job_id, msg_id:, queue_name:)
      where(job_id: job_id).update_all(msg_id: msg_id, queue_name: queue_name)
    end
  end
end
