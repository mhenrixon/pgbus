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

    # Record a completed job. Returns the batch row after update.
    def self.job_completed(batch_id)
      update_counter(batch_id, "completed_jobs")
    end

    # Record a discarded (dead-lettered) job. Returns the batch row after update.
    def self.job_discarded(batch_id)
      update_counter(batch_id, "discarded_jobs")
    end

    # Find a batch record by ID. Returns a hash or nil.
    def self.find(batch_id)
      BatchEntry.find_by(batch_id: batch_id)&.attributes
    end

    # Delete finished batches older than the given threshold.
    def self.cleanup(older_than:)
      BatchEntry.stale(before: older_than).delete_all
    end

    private

    def create_record
      BatchEntry.create!(
        batch_id: batch_id,
        description: description,
        on_finish_class: on_finish&.name,
        on_success_class: on_success&.name,
        on_discard_class: on_discard&.name,
        properties: JSON.generate(properties),
        status: "pending"
      )
    end

    def count_jobs(&)
      previous_batch_id = Thread.current[:pgbus_batch_id]
      previous_count = Thread.current[:pgbus_batch_job_count]

      Thread.current[:pgbus_batch_id] = batch_id
      Thread.current[:pgbus_batch_job_count] = 0

      yield

      @job_count = Thread.current[:pgbus_batch_job_count] || 0
    ensure
      Thread.current[:pgbus_batch_id] = previous_batch_id
      Thread.current[:pgbus_batch_job_count] = previous_count
    end

    def update_total
      if @job_count.zero?
        # Finish empty batches immediately — no jobs to signal completion
        BatchEntry.where(batch_id: batch_id).update_all(
          total_jobs: 0,
          status: "finished",
          finished_at: Time.current
        )
        fire_empty_batch_callbacks
      else
        BatchEntry.where(batch_id: batch_id).update_all(total_jobs: @job_count, status: "processing")
      end
    end

    def fire_empty_batch_callbacks
      record = BatchEntry.find_by(batch_id: batch_id)
      return unless record

      properties = parse_properties(record.properties)
      self.class.send(:enqueue_callback, record.on_finish_class, properties) if record.on_finish_class
      self.class.send(:enqueue_callback, record.on_success_class, properties) if record.on_success_class
    end

    def parse_properties(props)
      JSON.parse(props.presence || "{}")
    rescue JSON::ParserError => e
      Pgbus.logger.error { "[Pgbus] Invalid batch properties JSON: #{e.message}" }
      {}
    end

    class << self
      private

      def update_counter(batch_id, column)
        result = BatchEntry.increment_counter!(batch_id, column)
        return nil unless result

        fire_callbacks(result[:record]) if result[:just_finished]
        result
      end

      def fire_callbacks(record)
        properties = begin
          JSON.parse(record.properties.presence || "{}")
        rescue JSON::ParserError => e
          Pgbus.logger.error { "[Pgbus] Invalid batch properties JSON: #{e.message}" }
          {}
        end
        all_succeeded = record.discarded_jobs.zero?

        enqueue_callback(record.on_finish_class, properties) if record.on_finish_class
        enqueue_callback(record.on_success_class, properties) if record.on_success_class && all_succeeded
        enqueue_callback(record.on_discard_class, properties) if record.on_discard_class && !all_succeeded
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
