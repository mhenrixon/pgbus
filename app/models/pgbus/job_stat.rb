# frozen_string_literal: true

module Pgbus
  class JobStat < Pgbus::ApplicationRecord
    self.table_name = "pgbus_job_stats"

    scope :since, ->(time) { where("created_at >= ?", time) }
    scope :successful, -> { where(status: "success") }
    scope :failed, -> { where(status: "failed") }
    scope :dead_lettered, -> { where(status: "dead_lettered") }

    # Record a job execution stat. Called by the executor after each job.
    def self.record!(job_class:, queue_name:, status:, duration_ms:)
      return unless table_exists?

      create!(
        job_class: job_class,
        queue_name: queue_name,
        status: status,
        duration_ms: duration_ms
      )
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus] Failed to record job stat: #{e.message}" }
    end

    def self.table_exists?
      return @table_exists if defined?(@table_exists)

      @table_exists = connection.table_exists?(table_name)
    rescue StandardError
      @table_exists = false
    end

    # Throughput: jobs per minute bucketed by minute for the last N minutes
    def self.throughput(minutes: 60)
      since(minutes.minutes.ago)
        .group("date_trunc('minute', created_at)")
        .order(Arel.sql("date_trunc('minute', created_at)"))
        .count
    end

    # Average duration by job class
    def self.avg_duration_by_class(minutes: 60)
      since(minutes.minutes.ago)
        .group(:job_class)
        .order(Arel.sql("AVG(duration_ms) DESC"))
        .average(:duration_ms)
    end

    # Success/fail/DLQ counts
    def self.status_counts(minutes: 60)
      since(minutes.minutes.ago).group(:status).count
    end

    # Top N slowest job classes by average duration
    def self.slowest_classes(limit: 10, minutes: 60)
      since(minutes.minutes.ago)
        .group(:job_class)
        .order(Arel.sql("AVG(duration_ms) DESC"))
        .limit(limit)
        .pluck(:job_class, Arel.sql("COUNT(*)"), Arel.sql("ROUND(AVG(duration_ms))"), Arel.sql("MAX(duration_ms)"))
        .map { |cls, count, avg, max| { job_class: cls, count: count.to_i, avg_ms: avg.to_i, max_ms: max.to_i } }
    end

    # Total stats summary
    def self.summary(minutes: 60)
      stats = since(minutes.minutes.ago)
      {
        total: stats.count,
        success: stats.successful.count,
        failed: stats.failed.count,
        dead_lettered: stats.dead_lettered.count,
        avg_duration_ms: stats.average(:duration_ms)&.round(1) || 0,
        max_duration_ms: stats.maximum(:duration_ms) || 0
      }
    end

    # Cleanup old stats
    def self.cleanup!(older_than:)
      where("created_at < ?", older_than).delete_all
    end
  end
end
