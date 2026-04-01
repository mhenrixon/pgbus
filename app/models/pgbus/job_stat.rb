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

    # Memoized — intentionally never invalidated at runtime. If the
    # pgbus_job_stats migration runs while the app is already running,
    # a restart is required for stat recording to begin.
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

    # Single-query aggregate summary using conditional counts.
    def self.summary(minutes: 60)
      row = since(minutes.minutes.ago).pick(
        Arel.sql("COUNT(*)"),
        Arel.sql("COUNT(*) FILTER (WHERE status = 'success')"),
        Arel.sql("COUNT(*) FILTER (WHERE status = 'failed')"),
        Arel.sql("COUNT(*) FILTER (WHERE status = 'dead_lettered')"),
        Arel.sql("ROUND(AVG(duration_ms)::numeric, 1)"),
        Arel.sql("MAX(duration_ms)")
      )

      {
        total: row[0].to_i,
        success: row[1].to_i,
        failed: row[2].to_i,
        dead_lettered: row[3].to_i,
        avg_duration_ms: row[4]&.to_f || 0,
        max_duration_ms: row[5].to_i
      }
    end

    # Cleanup old stats
    def self.cleanup!(older_than:)
      where("created_at < ?", older_than).delete_all
    end
  end
end
