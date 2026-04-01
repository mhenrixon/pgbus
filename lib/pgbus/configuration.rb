# frozen_string_literal: true

require "logger"

module Pgbus
  class Configuration
    # Connection settings
    attr_accessor :database_url, :connection_params, :pool_size, :pool_timeout

    # Queue settings
    attr_accessor :default_queue, :queue_prefix

    # Worker settings
    attr_accessor :workers, :polling_interval, :visibility_timeout, :prefetch_limit

    # Worker recycling
    attr_accessor :max_jobs_per_worker, :max_memory_mb, :max_worker_lifetime

    # Dispatcher settings
    attr_accessor :dispatch_interval

    # Circuit breaker
    attr_accessor :circuit_breaker_enabled, :circuit_breaker_threshold,
                  :circuit_breaker_base_backoff, :circuit_breaker_max_backoff

    # Dead letter queue
    attr_accessor :max_retries, :dead_letter_queue_suffix

    # Priority queues
    attr_accessor :priority_levels, :default_priority

    # Archive compaction
    attr_accessor :archive_retention, :archive_compaction_interval, :archive_compaction_batch_size

    # Transactional outbox
    attr_accessor :outbox_enabled, :outbox_poll_interval, :outbox_batch_size, :outbox_retention

    # Event bus
    attr_accessor :idempotency_ttl

    # Logging
    attr_accessor :logger

    # LISTEN/NOTIFY
    attr_accessor :listen_notify, :notify_throttle_ms

    # PGMQ schema installation mode (:auto, :extension, :embedded)
    attr_reader :pgmq_schema_mode

    # Event consumers
    attr_accessor :event_consumers

    # Recurring jobs
    attr_accessor :recurring_tasks, :recurring_schedule_interval, :recurring_tasks_file,
                  :skip_recurring, :recurring_execution_retention

    # Multi-database support (optional separate database for pgbus tables)
    # Set to { database: { writing: :pgbus, reading: :pgbus } } to use a separate database.
    # Requires a matching entry in config/database.yml under the "pgbus" key.
    attr_accessor :connects_to

    # Web dashboard
    attr_accessor :web_auth, :web_refresh_interval, :web_per_page, :web_live_updates, :web_data_source

    def initialize
      @database_url = nil
      @connection_params = nil
      @pool_size = 5
      @pool_timeout = 5

      @default_queue = "default"
      @queue_prefix = "pgbus"

      @workers = [{ queues: %w[default], threads: 5 }]
      @polling_interval = 0.1
      @visibility_timeout = 30

      @prefetch_limit = nil

      @max_jobs_per_worker = nil
      @max_memory_mb = nil
      @max_worker_lifetime = nil

      @dispatch_interval = 1.0

      @circuit_breaker_enabled = true
      @circuit_breaker_threshold = 5
      @circuit_breaker_base_backoff = 30
      @circuit_breaker_max_backoff = 600

      @max_retries = 5
      @dead_letter_queue_suffix = "_dlq"

      @priority_levels = nil
      @default_priority = 1

      @archive_retention = 7 * 24 * 3600 # 7 days
      @archive_compaction_interval = 3600
      @archive_compaction_batch_size = 1000

      @outbox_enabled = false
      @outbox_poll_interval = 1.0
      @outbox_batch_size = 100
      @outbox_retention = 24 * 3600 # 1 day

      @idempotency_ttl = 7 * 24 * 3600 # 7 days

      @logger = defined?(Rails) ? Rails.logger : Logger.new($stdout)

      @listen_notify = true
      @notify_throttle_ms = 250

      @pgmq_schema_mode = :auto

      @event_consumers = nil

      @recurring_tasks = nil
      @recurring_schedule_interval = 1.0
      @recurring_tasks_file = nil
      @skip_recurring = false
      @recurring_execution_retention = 7 * 24 * 3600 # 7 days

      @connects_to = nil

      @web_auth = nil
      @web_refresh_interval = 5000
      @web_per_page = 25
      @web_live_updates = true
      @web_data_source = nil
    end

    def queue_name(name)
      "#{queue_prefix}_#{name}"
    end

    def dead_letter_queue_name(name)
      "#{queue_name(name)}#{dead_letter_queue_suffix}"
    end

    def priority_queue_name(name, priority)
      "#{queue_name(name)}_p#{priority}"
    end

    def priority_queue_names(name)
      return [queue_name(name)] unless priority_levels && priority_levels > 1

      (0...priority_levels).map { |p| priority_queue_name(name, p) }
    end

    VALID_PGMQ_SCHEMA_MODES = %i[auto extension embedded].freeze

    def pgmq_schema_mode=(mode)
      mode = mode.to_sym
      unless VALID_PGMQ_SCHEMA_MODES.include?(mode)
        raise ArgumentError, "Invalid pgmq_schema_mode: #{mode}. Must be one of: #{VALID_PGMQ_SCHEMA_MODES.join(", ")}"
      end

      @pgmq_schema_mode = mode
    end

    def validate!
      raise ArgumentError, "pool_size must be > 0" unless pool_size.is_a?(Numeric) && pool_size.positive?
      raise ArgumentError, "pool_timeout must be > 0" unless pool_timeout.is_a?(Numeric) && pool_timeout.positive?
      raise ArgumentError, "polling_interval must be > 0" unless polling_interval.is_a?(Numeric) && polling_interval.positive?
      raise ArgumentError, "visibility_timeout must be > 0" unless visibility_timeout.is_a?(Numeric) && visibility_timeout.positive?
      raise ArgumentError, "max_retries must be >= 0" unless max_retries.is_a?(Integer) && max_retries >= 0

      workers.each do |w|
        threads = w[:threads] || w["threads"] || 5
        raise ArgumentError, "worker threads must be > 0" unless threads.is_a?(Integer) && threads.positive?
      end

      raise ArgumentError, "prefetch_limit must be > 0" if prefetch_limit && !(prefetch_limit.is_a?(Integer) && prefetch_limit.positive?)

      if priority_levels && !(priority_levels.is_a?(Integer) && priority_levels >= 1 && priority_levels <= 10)
        raise ArgumentError, "priority_levels must be an integer between 1 and 10"
      end

      self
    end

    def connection_options
      if database_url
        database_url
      elsif connection_params
        connection_params
      elsif defined?(ActiveRecord::Base)
        if connects_to
          -> { Pgbus::ApplicationRecord.connection.raw_connection }
        else
          -> { ActiveRecord::Base.connection.raw_connection }
        end
      else
        raise ConfigurationError, "No database connection configured. " \
                                  "Set Pgbus.configuration.database_url, connection_params, or use with Rails."
      end
    end
  end
end
