# frozen_string_literal: true

module Pgbus
  # Thread-safe buffer that accumulates job stats in memory and flushes
  # them to the database in batches. This avoids one INSERT per job
  # execution, replacing it with periodic bulk inserts.
  class StatBuffer
    DEFAULT_FLUSH_SIZE = 100
    DEFAULT_FLUSH_INTERVAL = 5 # seconds

    attr_reader :flush_size, :flush_interval

    def initialize(flush_size: DEFAULT_FLUSH_SIZE, flush_interval: DEFAULT_FLUSH_INTERVAL)
      @flush_size = flush_size
      @flush_interval = flush_interval
      @buffer = []
      @mutex = Mutex.new
      @last_flush_at = monotonic_now
      @stopped = false
    end

    # Append a stat entry to the buffer. Flushes automatically when
    # the buffer reaches flush_size.
    def push(attrs)
      should_flush = false

      @mutex.synchronize do
        @buffer << attrs
        should_flush = @buffer.size >= @flush_size
      end

      flush if should_flush
    end

    # Flush buffered stats to the database. Safe to call from any thread.
    def flush
      entries = nil

      @mutex.synchronize do
        return if @buffer.empty?

        entries = @buffer.dup
        @buffer.clear
        @last_flush_at = monotonic_now
      end

      write_to_database(entries) if entries&.any?
    end

    # Flush if the interval has elapsed since the last flush.
    # Called by the dispatcher on its maintenance tick.
    def flush_if_due
      due = @mutex.synchronize { monotonic_now - @last_flush_at >= @flush_interval }
      flush if due
    end

    def stop
      @stopped = true
      flush
    end

    def size
      @mutex.synchronize { @buffer.size }
    end

    private

    def write_to_database(entries)
      return unless JobStat.table_exists?

      # Always attempt all 6 columns. If the latency migration hasn't
      # run, the first flush will fail, and we fall back to base columns.
      # This avoids relying on the memoized latency_columns? flag which
      # can be stuck at false if evaluated before the connection is ready.
      rows = entries.map do |e|
        { job_class: e[:job_class], queue_name: e[:queue_name],
          status: e[:status], duration_ms: e[:duration_ms],
          enqueue_latency_ms: e[:enqueue_latency_ms], retry_count: e[:retry_count] }
      end

      JobStat.insert_all(rows)
    rescue ActiveRecord::StatementInvalid, ActiveModel::UnknownAttributeError => e
      # Column doesn't exist — fall back to base columns only
      if e.message.include?("enqueue_latency_ms") || e.message.include?("retry_count")
        base_rows = entries.map do |entry|
          { job_class: entry[:job_class], queue_name: entry[:queue_name],
            status: entry[:status], duration_ms: entry[:duration_ms] }
        end
        JobStat.insert_all(base_rows)
      else
        Pgbus.logger.warn { "[Pgbus] Stat buffer flush failed: #{e.message}" }
      end
    rescue StandardError => e
      Pgbus.logger.warn { "[Pgbus] Stat buffer flush failed: #{e.message}" }
    end

    def monotonic_now
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end
end
