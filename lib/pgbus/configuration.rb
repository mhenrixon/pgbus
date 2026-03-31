# frozen_string_literal: true

require "logger"

module Pgbus
  class Configuration
    # Connection settings
    attr_accessor :database_url, :connection_params, :pool_size, :pool_timeout

    # Queue settings
    attr_accessor :default_queue, :queue_prefix

    # Worker settings
    attr_accessor :workers, :polling_interval, :visibility_timeout

    # Worker recycling
    attr_accessor :max_jobs_per_worker, :max_memory_mb, :max_worker_lifetime

    # Dispatcher settings
    attr_accessor :dispatch_interval

    # Dead letter queue
    attr_accessor :max_retries, :dead_letter_queue_suffix

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

      @max_jobs_per_worker = nil
      @max_memory_mb = nil
      @max_worker_lifetime = nil

      @dispatch_interval = 1.0

      @max_retries = 5
      @dead_letter_queue_suffix = "_dlq"

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

    VALID_PGMQ_SCHEMA_MODES = %i[auto extension embedded].freeze

    def pgmq_schema_mode=(mode)
      mode = mode.to_sym
      unless VALID_PGMQ_SCHEMA_MODES.include?(mode)
        raise ArgumentError, "Invalid pgmq_schema_mode: #{mode}. Must be one of: #{VALID_PGMQ_SCHEMA_MODES.join(", ")}"
      end

      @pgmq_schema_mode = mode
    end

    def connection_options
      if database_url
        database_url
      elsif connection_params
        connection_params
      elsif defined?(ActiveRecord::Base)
        -> { ActiveRecord::Base.connection.raw_connection }
      else
        raise ConfigurationError, "No database connection configured. " \
                                  "Set Pgbus.configuration.database_url, connection_params, or use with Rails."
      end
    end
  end
end
