# frozen_string_literal: true

require "securerandom"
require "json"

module Pgbus
  class Batch
    METADATA_KEY = "pgbus_batch_id"

    attr_reader :batch_id, :properties, :description,
                :on_finish, :on_success, :on_discard

    def initialize(on_finish: nil, on_success: nil, on_discard: nil, description: nil, properties: {})
      @batch_id = SecureRandom.uuid
      @on_finish = on_finish
      @on_success = on_success
      @on_discard = on_discard
      @description = description
      @properties = properties
      @job_count = 0
    end

    # Enqueue a group of jobs as a batch.
    # Jobs enqueued inside the block are tracked as part of this batch.
    def enqueue(&)
      create_record
      count_jobs(&)
      update_total
      self
    end

    # Record a completed job. Returns the batch status after update.
    def self.job_completed(batch_id)
      update_counter(batch_id, "completed_jobs")
    end

    # Record a discarded (dead-lettered) job. Returns the batch status after update.
    def self.job_discarded(batch_id)
      update_counter(batch_id, "discarded_jobs")
    end

    # Find a batch record by ID. Returns a hash or nil.
    def self.find(batch_id)
      return nil unless defined?(ActiveRecord::Base)

      result = ActiveRecord::Base.connection.exec_query(
        "SELECT * FROM pgbus_batches WHERE batch_id = $1",
        "Pgbus Batch Find",
        [batch_id]
      )
      result.first
    end

    # Delete finished batches older than the given threshold.
    def self.cleanup(older_than:)
      return 0 unless defined?(ActiveRecord::Base)

      result = ActiveRecord::Base.connection.exec_query(
        "DELETE FROM pgbus_batches WHERE status = 'finished' AND finished_at < $1 RETURNING id",
        "Pgbus Batch Cleanup",
        [older_than]
      )
      result.to_a.size
    end

    private

    def create_record
      return unless defined?(ActiveRecord::Base)

      ActiveRecord::Base.connection.exec_query(
        <<~SQL,
          INSERT INTO pgbus_batches
            (batch_id, description, on_finish_class, on_success_class, on_discard_class, properties, status)
          VALUES ($1, $2, $3, $4, $5, $6, 'pending')
        SQL
        "Pgbus Batch Create",
        [batch_id, description, on_finish&.name, on_success&.name, on_discard&.name, JSON.generate(properties)]
      )
    end

    def count_jobs(&)
      Thread.current[:pgbus_batch_id] = batch_id
      @job_count = 0

      yield

      @job_count = Thread.current[:pgbus_batch_job_count] || 0
    ensure
      Thread.current[:pgbus_batch_id] = nil
      Thread.current[:pgbus_batch_job_count] = nil
    end

    def update_total
      return unless defined?(ActiveRecord::Base)

      ActiveRecord::Base.connection.exec_query(
        "UPDATE pgbus_batches SET total_jobs = $1, status = 'processing' WHERE batch_id = $2",
        "Pgbus Batch Update Total",
        [@job_count, batch_id]
      )
    end

    class << self
      private

      def update_counter(batch_id, column)
        return nil unless defined?(ActiveRecord::Base)

        result = ActiveRecord::Base.connection.exec_query(
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
            RETURNING status, total_jobs, completed_jobs, discarded_jobs, on_finish_class, on_success_class, on_discard_class, properties
          SQL
          "Pgbus Batch Counter",
          [batch_id]
        )

        row = result.first
        return nil unless row

        fire_callbacks(row) if row["status"] == "finished"
        row
      end

      def fire_callbacks(row)
        properties = JSON.parse(row["properties"] || "{}")
        all_succeeded = row["discarded_jobs"].to_i.zero?

        enqueue_callback(row["on_finish_class"], properties) if row["on_finish_class"]
        enqueue_callback(row["on_success_class"], properties) if row["on_success_class"] && all_succeeded
        enqueue_callback(row["on_discard_class"], properties) if row["on_discard_class"] && !all_succeeded
      end

      def enqueue_callback(class_name, properties)
        job_class = class_name.constantize
        job_class.perform_later(properties)
      rescue NameError => e
        Pgbus.logger.error { "[Pgbus] Batch callback class not found: #{class_name}: #{e.message}" }
      end
    end
  end
end
