# frozen_string_literal: true

require "logger"

module Pgbus
  class Configuration
    # Connection settings
    attr_accessor :database_url, :connection_params, :pool_size, :pool_timeout

    # Queue settings
    attr_accessor :default_queue, :queue_prefix

    # Worker settings
    attr_accessor :polling_interval, :visibility_timeout, :prefetch_limit
    attr_reader :workers

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
    attr_accessor :idempotency_ttl, :allowed_global_id_models

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

    # Job stats
    attr_accessor :stats_retention, :stats_enabled

    # Web dashboard
    attr_accessor :web_auth, :web_refresh_interval, :web_per_page, :web_live_updates, :web_data_source,
                  :insights_default_minutes, :base_controller_class, :return_to_app_url

    def initialize
      @database_url = nil
      @connection_params = nil
      @pool_size = nil
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
      @allowed_global_id_models = nil # nil = allow all (for backwards compat)

      @logger = (defined?(Rails) && Rails.respond_to?(:logger) && Rails.logger) || Logger.new($stdout)

      @listen_notify = true
      @notify_throttle_ms = 250

      @pgmq_schema_mode = :auto

      @event_consumers = nil

      @recurring_tasks = nil
      @recurring_schedule_interval = 1.0
      @recurring_tasks_file = nil
      @skip_recurring = false
      @recurring_execution_retention = 7 * 24 * 3600 # 7 days

      @stats_enabled = true
      @stats_retention = 30 * 24 * 3600 # 30 days

      @connects_to = nil

      @web_auth = nil
      @web_refresh_interval = 5000
      @web_per_page = 25
      @web_live_updates = true
      @web_data_source = nil
      @insights_default_minutes = 30 * 24 * 60 # 30 days
      @base_controller_class = "::ActionController::Base"
      @return_to_app_url = nil
    end

    def queue_name(name)
      full = "#{queue_prefix}_#{name}"
      QueueNameValidator.validate!(full)
      full
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
      if pool_size && !(pool_size.is_a?(Numeric) && pool_size.positive?)
        raise ArgumentError, "pool_size must be a positive number or nil (auto-tune)"
      end

      raise ArgumentError, "pool_timeout must be > 0" unless pool_timeout.is_a?(Numeric) && pool_timeout.positive?
      raise ArgumentError, "polling_interval must be > 0" unless polling_interval.is_a?(Numeric) && polling_interval.positive?
      raise ArgumentError, "visibility_timeout must be > 0" unless visibility_timeout.is_a?(Numeric) && visibility_timeout.positive?
      raise ArgumentError, "max_retries must be >= 0" unless max_retries.is_a?(Integer) && max_retries >= 0

      Array(workers).each do |w|
        threads = w[:threads] || w["threads"] || 5
        raise ArgumentError, "worker threads must be > 0" unless threads.is_a?(Integer) && threads.positive?
      end

      raise ArgumentError, "prefetch_limit must be > 0" if prefetch_limit && !(prefetch_limit.is_a?(Integer) && prefetch_limit.positive?)

      if priority_levels && !(priority_levels.is_a?(Integer) && priority_levels >= 1 && priority_levels <= 10)
        raise ArgumentError, "priority_levels must be an integer between 1 and 10"
      end

      unless insights_default_minutes.is_a?(Integer) && insights_default_minutes.positive?
        raise ArgumentError, "insights_default_minutes must be a positive integer"
      end

      self
    end

    # Set the worker capsule list. Accepts:
    #
    #   String — parsed via Pgbus::Configuration::CapsuleDSL into capsules
    #            with auto-generated names (each capsule's :name is its
    #            first queue token).
    #
    #     c.workers "*: 5"
    #     c.workers "critical: 5; default, mailers: 10"
    #
    #   Array  — legacy explicit form. Each entry is a Hash with :queues
    #            and :threads (and optionally :name, :single_active_consumer,
    #            :consumer_priority, :prefetch_limit).
    #
    #     c.workers [{ queues: %w[default], threads: 5 }]
    #
    #   nil    — no workers configured (used when running scheduler-only or
    #            dispatcher-only processes).
    #
    # Raises ArgumentError for any other type.
    def workers=(value)
      @workers = case value
                 when nil
                   nil
                 when String
                   CapsuleDSL.parse(value).map { |entry| entry.merge(name: entry[:queues].first.to_s) }
                 when Array
                   value
                 else
                   raise ArgumentError,
                         "workers must be a String (DSL), Array (legacy form), or nil — got #{value.class}"
                 end
    end

    # Define a named capsule and append it to the workers list.
    #
    #   c.capsule :critical, queues: %w[critical], threads: 5
    #   c.capsule :gated, queues: %w[gated], threads: 1, single_active_consumer: true
    #
    # Names must be unique. Queues must not overlap with capsules already
    # defined (would cause double-processing). Composes with the string DSL —
    # +c.workers "..."+ followed by +c.capsule :name, ...+ appends the
    # named capsule to the list parsed from the string.
    def capsule(name, queues:, threads:, **)
      raise ArgumentError, "capsule queues must be a non-empty Array" unless queues.is_a?(Array) && queues.any?
      raise ArgumentError, "capsule threads must be a positive Integer" unless threads.is_a?(Integer) && threads.positive?

      normalized_name = name.to_s
      @workers ||= []

      raise ArgumentError, "capsule #{name.inspect} is already defined" if @workers.any? { |c| capsule_name(c) == normalized_name }

      validate_no_queue_overlap!(queues)

      @workers << { name: normalized_name, queues: queues, threads: threads, ** }
    end

    # Look up a capsule by its name. Accepts symbol or string. Returns the
    # matching Hash, or nil. Used by the CLI's --capsule selector.
    def capsule_named(name)
      return nil unless @workers

      key = name.to_s
      @workers.find { |c| capsule_name(c) == key }
    end

    # Returns the connection pool size to use for the PGMQ client.
    #
    # If +pool_size+ was explicitly set, returns that value unchanged. Otherwise
    # auto-derives from the configured worker and event_consumer thread counts:
    #
    #   sum(workers.threads) + sum(event_consumers.threads) + 2
    #
    # The +2 covers the dispatcher and scheduler, which each issue occasional
    # queries against the same connection pool.
    #
    # Auto-tune protects users from the common pitfall of running 15 worker
    # threads with a hand-set pool_size of 5 (resulting in ConnectionPool
    # timeouts under load). Setting pool_size explicitly is still supported
    # for advanced cases where you need a tighter or looser pool than the
    # default formula provides.
    POOL_SIZE_OVERHEAD = 2 # dispatcher + scheduler
    POOL_SIZE_WARN_THRESHOLD = 50

    def resolved_pool_size
      return pool_size if pool_size

      total = sum_thread_counts(workers, default_threads: 5, group: "worker") +
              sum_thread_counts(event_consumers, default_threads: 3, group: "event_consumer") +
              POOL_SIZE_OVERHEAD

      warn_if_oversized(total)
      total
    end

    def connection_options
      if database_url
        database_url
      elsif connection_params
        connection_params
      elsif defined?(ActiveRecord::Base)
        # Extract connection config from ActiveRecord so pgmq-ruby creates its
        # own dedicated PG connections. Sharing AR's raw_connection via a Proc
        # is NOT thread-safe: the ConnectionPool caches the PG::Connection from
        # whichever thread first called the Proc, then hands that same object to
        # other threads — causing nil results, segfaults, and data corruption
        # when AR and PGMQ issue concurrent queries on the same connection.
        extract_ar_connection_hash
      else
        raise ConfigurationError, "No database connection configured. " \
                                  "Set Pgbus.configuration.database_url, connection_params, or use with Rails."
      end
    end

    private

    # Sum the +:threads+ values across a list of worker/consumer entries.
    # Uses +default_threads+ when an entry omits the key. Rejects anything
    # that isn't a positive Integer with a clear error — silent coercion via
    # +to_i+ would let "abc" → 0 produce a critically under-sized pool with
    # no indication that something was wrong.
    # Read a capsule's name from either symbol or string key, normalized
    # to a string for comparison. Returns nil for unnamed (legacy) entries.
    def capsule_name(entry)
      raw = entry[:name] || entry["name"]
      raw&.to_s
    end

    # Validates that no queue in +new_queues+ would overlap with any
    # existing capsule. The wildcard '*' counts as overlapping with EVERY
    # other queue (and vice versa) because at runtime '*' is expanded to
    # all known queues. Raises ArgumentError on overlap.
    def validate_no_queue_overlap!(new_queues)
      existing = (@workers || []).flat_map { |c| c[:queues] || c["queues"] || [] }
      return if existing.empty?

      if existing.include?(CapsuleDSL::WILDCARD)
        raise ArgumentError,
              "an existing capsule already uses '*' (matches every queue) — " \
              "the new capsule's queues #{new_queues.inspect} would overlap with it"
      end

      if new_queues.include?(CapsuleDSL::WILDCARD)
        raise ArgumentError,
              "the new capsule uses '*' (matches every queue) but other capsules " \
              "are already defined with queues #{existing.inspect} — " \
              "the wildcard would overlap with all of them"
      end

      conflict = new_queues.find { |q| existing.include?(q) }
      return unless conflict

      raise ArgumentError,
            "queue #{conflict.inspect} is already assigned to another capsule — " \
            "each queue can only belong to one capsule"
    end

    def sum_thread_counts(entries, default_threads:, group:)
      return 0 unless entries

      entries.sum do |entry|
        threads = entry[:threads] || entry["threads"] || default_threads
        unless threads.is_a?(Integer) && threads.positive?
          raise ArgumentError,
                "#{group} threads must be a positive integer, got #{threads.inspect}"
        end
        threads
      end
    end

    def warn_if_oversized(size)
      return unless size > POOL_SIZE_WARN_THRESHOLD

      Pgbus.logger.warn do
        "[Pgbus] Auto-tuned pool_size is #{size} (over #{POOL_SIZE_WARN_THRESHOLD}). " \
          "Verify your worker thread counts are intentional. " \
          "Set Pgbus.configuration.pool_size explicitly to override."
      end
    end

    def extract_ar_connection_hash
      base = connects_to ? Pgbus::BusRecord : ActiveRecord::Base
      db_config = base.connection_db_config

      # Rails 7.1+ db_config.configuration_hash returns the full config
      config_hash = db_config.configuration_hash

      {
        host: config_hash[:host] || "localhost",
        port: (config_hash[:port] || 5432).to_i,
        dbname: config_hash[:database],
        user: config_hash[:username],
        password: config_hash[:password]
      }.compact
    rescue StandardError => e
      # Fallback to Proc path if AR config extraction fails (e.g., adapter
      # doesn't expose standard config keys). Log a warning since this path
      # is not thread-safe.
      Pgbus.logger.warn do
        "[Pgbus] Could not extract AR connection config (#{e.class}: #{e.message}). " \
          "Falling back to shared raw_connection — this is NOT thread-safe with multiple workers. " \
          "Set Pgbus.configuration.database_url explicitly for thread-safe operation."
      end
      if connects_to
        -> { Pgbus::BusRecord.connection.raw_connection }
      else
        -> { ActiveRecord::Base.connection.raw_connection }
      end
    end
  end
end
