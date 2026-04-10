# frozen_string_literal: true

require "logger"

module Pgbus
  class Configuration
    # Connection settings
    attr_accessor :database_url, :connection_params, :pool_size, :pool_timeout

    # Queue settings
    attr_accessor :default_queue, :queue_prefix

    # Worker settings
    attr_accessor :polling_interval, :prefetch_limit
    attr_reader :workers, :visibility_timeout # rubocop:disable Style/AccessorGrouping

    # Supervisor role selection.
    # nil = boot all roles (default behavior).
    # Array of role symbols = boot only the listed roles.
    # Set via the CLI flags --workers-only / --scheduler-only / --dispatcher-only,
    # or directly in an initializer for advanced cases.
    attr_reader :roles

    VALID_ROLES = %i[workers dispatcher scheduler consumers outbox].freeze

    # Worker recycling
    attr_accessor :max_jobs_per_worker, :max_memory_mb, :max_worker_lifetime

    # Dispatcher settings
    attr_accessor :dispatch_interval

    # Circuit breaker. Only `enabled` is user-facing — the trip threshold and
    # backoff curve are tuned via constants on Pgbus::CircuitBreaker because
    # they are implementation details that have never been worth exposing.
    attr_accessor :circuit_breaker_enabled

    # Dead letter queue
    attr_accessor :max_retries

    # Priority queues
    attr_accessor :priority_levels, :default_priority

    # Archive compaction. Only the user-facing retention window is configurable;
    # the loop interval and batch size are tuned via constants on
    # Pgbus::Process::Dispatcher.
    attr_reader :archive_retention

    # Transactional outbox
    attr_accessor :outbox_enabled, :outbox_poll_interval, :outbox_batch_size
    attr_reader :outbox_retention # rubocop:disable Style/AccessorGrouping

    # Event bus
    attr_accessor :allowed_global_id_models
    attr_reader :idempotency_ttl # rubocop:disable Style/AccessorGrouping

    # Logging
    attr_accessor :logger

    # LISTEN/NOTIFY. Only the on/off switch is user-facing — the throttle
    # interval is a Postgres-side tuning knob that lives as a constant on
    # Pgbus::Client (NOTIFY_THROTTLE_MS).
    attr_accessor :listen_notify

    # PGMQ schema installation mode (:auto, :extension, :embedded)
    attr_reader :pgmq_schema_mode

    # Event consumers
    attr_accessor :event_consumers

    # Recurring jobs
    attr_accessor :recurring_tasks, :recurring_schedule_interval, :recurring_tasks_file, :skip_recurring
    attr_reader :recurring_execution_retention # rubocop:disable Style/AccessorGrouping

    # Multi-database support (optional separate database for pgbus tables)
    # Set to { database: { writing: :pgbus, reading: :pgbus } } to use a separate database.
    # Requires a matching entry in config/database.yml under the "pgbus" key.
    attr_accessor :connects_to

    # Job stats
    attr_accessor :stats_enabled
    attr_reader :stats_retention # rubocop:disable Style/AccessorGrouping

    # Web dashboard
    attr_accessor :web_auth, :web_refresh_interval, :web_per_page, :web_live_updates, :web_data_source,
                  :insights_default_minutes, :base_controller_class, :return_to_app_url,
                  :metrics_enabled

    # Streams (turbo-rails replacement, SSE-based)
    attr_accessor :streams_enabled, :streams_queue_prefix, :streams_signed_name_secret,
                  :streams_default_retention, :streams_retention, :streams_heartbeat_interval,
                  :streams_max_connections, :streams_idle_timeout, :streams_listen_health_check_ms,
                  :streams_write_deadline_ms, :streams_falcon_streaming_body,
                  :streams_stats_enabled

    def initialize
      @database_url = nil
      @connection_params = nil
      @pool_size = nil
      @pool_timeout = 5

      @default_queue = "default"
      @queue_prefix = "pgbus"

      @workers = [{ queues: %w[default], threads: 5 }]
      @roles = nil
      @polling_interval = 0.1
      @visibility_timeout = 30

      @prefetch_limit = nil

      @max_jobs_per_worker = nil
      @max_memory_mb = nil
      @max_worker_lifetime = nil

      @dispatch_interval = 1.0

      @circuit_breaker_enabled = true

      @max_retries = 5

      @priority_levels = nil
      @default_priority = 1

      @archive_retention = 7 * 24 * 3600 # 7 days

      @outbox_enabled = false
      @outbox_poll_interval = 1.0
      @outbox_batch_size = 100
      @outbox_retention = 24 * 3600 # 1 day

      @idempotency_ttl = 7 * 24 * 3600 # 7 days
      @allowed_global_id_models = nil # nil = allow all (for backwards compat)

      @logger = (defined?(Rails) && Rails.respond_to?(:logger) && Rails.logger) || Logger.new($stdout)

      @listen_notify = true

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
      @metrics_enabled = true

      @streams_enabled = true
      @streams_queue_prefix = "pgbus_stream"
      @streams_signed_name_secret = nil
      @streams_default_retention = 5 * 60 # 5 minutes
      @streams_retention = {}
      @streams_heartbeat_interval = 15
      @streams_max_connections = 2_000
      @streams_idle_timeout = 3_600 # 1 hour
      # 250ms — this value plays two roles: (1) the TCP keepalive
      # interval for the streamer's PG LISTEN connection, and (2) the
      # upper bound on how long Dispatcher#handle_connect waits for
      # the Listener to acknowledge a synchronous ensure_listening
      # call. 5s was unbounded enough to drop messages on a
      # realistic subscribe burst; 250ms keeps the connect-path race
      # window tight while still leaving headroom over a typical
      # PG keepalive interval.
      @streams_listen_health_check_ms = 250
      @streams_write_deadline_ms = 5_000
      @streams_falcon_streaming_body = false
      # Opt-in: when true, the Dispatcher writes one row to
      # pgbus_stream_stats per broadcast/connect/disconnect. Default
      # off because stream event volume can be much higher than job
      # volume and the Insights surface is only useful if operators
      # actually look at it. Separate from #stats_enabled (which
      # gates pgbus_job_stats recording) on purpose — operators
      # usually want job stats on and stream stats off, or vice versa.
      @streams_stats_enabled = false
    end

    def queue_name(name)
      full = "#{queue_prefix}_#{name}"
      QueueNameValidator.validate!(full)
      full
    end

    def dead_letter_queue_name(name)
      "#{queue_name(name)}#{Pgbus::DEAD_LETTER_SUFFIX}"
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

      validate_streams!

      self
    end

    def validate_streams!
      unless streams_default_retention.is_a?(Numeric) && streams_default_retention >= 0
        raise ArgumentError, "streams_default_retention must be a non-negative number"
      end

      unless streams_max_connections.is_a?(Integer) && streams_max_connections.positive?
        raise ArgumentError, "streams_max_connections must be a positive integer"
      end

      unless streams_heartbeat_interval.is_a?(Numeric) && streams_heartbeat_interval.positive?
        raise ArgumentError, "streams_heartbeat_interval must be a positive number"
      end

      unless streams_idle_timeout.is_a?(Numeric) && streams_idle_timeout.positive?
        raise ArgumentError, "streams_idle_timeout must be a positive number"
      end

      unless streams_listen_health_check_ms.is_a?(Integer) && streams_listen_health_check_ms.positive?
        raise ArgumentError, "streams_listen_health_check_ms must be a positive integer"
      end

      unless streams_write_deadline_ms.is_a?(Integer) && streams_write_deadline_ms.positive?
        raise ArgumentError, "streams_write_deadline_ms must be a positive integer"
      end

      raise ArgumentError, "streams_retention must be a Hash" unless streams_retention.is_a?(Hash)
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
    #
    # NAMING SEMANTICS for the String form:
    #
    # The parser produces anonymous capsules (no :name). The setter then
    # auto-assigns a :name to capsules whose first queue would yield a
    # *unique* name across the parsed list AND is not the bare wildcard
    # (`*`). Anything else stays anonymous.
    #
    #   "critical: 5; default: 10"  -> two NAMED capsules ("critical", "default")
    #   "*: 5"                       -> one anonymous capsule (wildcard never names)
    #   "*: 3; *: 3; *: 3"           -> three anonymous capsules — legal,
    #                                   represents "3 forks all reading every
    #                                   queue", restoring the legacy YAML
    #                                   `5 × {queues: ["*"], threads: 3}` shape
    #   "default: 5; default: 3"     -> two anonymous capsules — same logic
    #
    # The point of the carve-out is the legacy "I want N forks of the same
    # worker pool" pattern: it must keep working since PGMQ tolerates it
    # natively (multiple processes reading the same queue with FOR UPDATE
    # SKIP LOCKED). The CLI's --capsule selector only matches NAMED
    # capsules, so anonymous duplicates can't be ambiguously addressed.
    def workers=(value)
      @workers = case value
                 when nil
                   nil
                 when String
                   parsed = CapsuleDSL.parse(value)
                   assign_auto_names(parsed)
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

    # Returns true if the given role should be booted by the supervisor.
    # When +roles+ is nil (the default), every role is enabled — this matches
    # the legacy single-process behavior. When +roles+ is set (e.g. via the
    # CLI's --workers-only / --scheduler-only / --dispatcher-only flags),
    # only the listed roles boot.
    #
    # Accepts symbol or string for case-insensitive comparison.
    def role_enabled?(role)
      return true if @roles.nil?

      @roles.include?(role.to_s.downcase.to_sym)
    end

    # Set the supervisor role filter. Accepts:
    #
    #   nil           — boot all roles (default)
    #   Symbol/String — wraps into a single-element array
    #   Array         — list of roles to boot
    #
    # Each role is normalized to a downcased symbol and validated against
    # VALID_ROLES. Unknown role names raise ArgumentError immediately so
    # typos like `[:workres]` fail loud at boot rather than leaving the
    # supervisor idling with no children.
    def roles=(value)
      if value.nil?
        @roles = nil
        return
      end

      normalized = Array(value).map { |r| r.to_s.downcase.to_sym }.uniq
      invalid = normalized - VALID_ROLES
      if invalid.any?
        raise ArgumentError,
              "invalid role(s) #{invalid.inspect} — valid roles are: #{VALID_ROLES.join(", ")}"
      end

      @roles = normalized
    end

    # Duration setters: each accepts either a Numeric (seconds) or an
    # ActiveSupport::Duration (e.g. 10.minutes, 7.days). Validation runs
    # immediately on assignment so misconfigurations crash at boot rather
    # than leaving stale state until a `validate!` call somewhere.
    #
    # Numeric values are stored unchanged (preserving Float for sub-second
    # values). Duration values are coerced to Integer seconds via .to_i.

    def visibility_timeout=(value)
      @visibility_timeout = coerce_duration!(value, :visibility_timeout)
    end

    def archive_retention=(value)
      @archive_retention = coerce_duration!(value, :archive_retention)
    end

    def outbox_retention=(value)
      @outbox_retention = coerce_duration!(value, :outbox_retention)
    end

    def idempotency_ttl=(value)
      @idempotency_ttl = coerce_duration!(value, :idempotency_ttl)
    end

    def stats_retention=(value)
      @stats_retention = coerce_duration!(value, :stats_retention)
    end

    def recurring_execution_retention=(value)
      @recurring_execution_retention = coerce_duration!(value, :recurring_execution_retention)
    end

    # Returns the connection pool size to use for the PGMQ client.
    #
    # If +pool_size+ was explicitly set, returns that value unchanged. Otherwise
    # auto-derives from the threads needed by the roles this process actually
    # runs (respects +Configuration#roles+ from --workers-only / --scheduler-only
    # / --dispatcher-only):
    #
    #   workers role     → sum(workers.threads)
    #   consumers role   → sum(event_consumers.threads)
    #   dispatcher role  → +1
    #   scheduler role   → +1
    #
    # A --scheduler-only deployment that has 50 worker threads configured
    # only needs 1 connection (for the scheduler), not 52.
    #
    # Auto-tune protects users from the common pitfall of running 15 worker
    # threads with a hand-set pool_size of 5 (resulting in ConnectionPool
    # timeouts under load). Setting pool_size explicitly is still supported
    # for advanced cases where you need a tighter or looser pool than the
    # default formula provides.
    POOL_SIZE_WARN_THRESHOLD = 50

    def resolved_pool_size
      return pool_size if pool_size

      total = 0
      total += sum_thread_counts(workers, default_threads: 5, group: "worker") if role_enabled?(:workers)
      total += sum_thread_counts(event_consumers, default_threads: 3, group: "event_consumer") if role_enabled?(:consumers)
      total += 1 if role_enabled?(:dispatcher)
      total += 1 if role_enabled?(:scheduler)

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

    # Coerce a duration setting value to a positive Numeric.
    #
    # Accepts an ActiveSupport::Duration (coerced to Integer seconds via .to_i)
    # or a Numeric (stored as-is, preserving Float for sub-second values).
    # Raises ArgumentError immediately for nil, zero, negative, or non-numeric
    # input — callers crash at boot rather than carrying silently-broken state.
    def coerce_duration!(value, name)
      # nil is a valid sentinel for "feature disabled" (e.g. archive_retention,
      # idempotency_ttl, recurring_execution_retention all use nil to skip the
      # corresponding maintenance task in the dispatcher).
      return nil if value.nil?

      # Check Duration FIRST because ActiveSupport overrides Numeric#is_a?
      # to return true for Integer, so a duration would otherwise be caught
      # by the Numeric branch and stored as-is (uncoerced).
      duration_class_loaded = defined?(ActiveSupport::Duration)
      return validate_positive_duration!(value.to_i, name) if duration_class_loaded && value.is_a?(ActiveSupport::Duration)

      # Plain Numeric (Integer, Float, Rational). Use class identity rather
      # than is_a? for the Duration exclusion because ActiveSupport overrides
      # is_a? — see comment above.
      if value.is_a?(Numeric) && (!defined?(ActiveSupport::Duration) || value.class != ActiveSupport::Duration)
        return validate_positive_duration!(value, name)
      end

      raise ArgumentError,
            "#{name} must be a Numeric (seconds), ActiveSupport::Duration, or nil to disable, got #{value.inspect}"
    end

    def validate_positive_duration!(numeric, name)
      raise ArgumentError, "#{name} must be a positive number, got #{numeric}" unless numeric.positive?

      numeric
    end

    # Read a capsule's name from either symbol or string key, normalized
    # to a string for comparison. Returns nil for unnamed (legacy) entries.
    def capsule_name(entry)
      raw = entry[:name] || entry["name"]
      raw&.to_s
    end

    # Auto-assign :name to parsed capsules where the first queue token would
    # yield a unique name and is not the bare wildcard. See the long comment
    # on +workers=+ for the why. Returns the same array with :name merged in
    # where applicable.
    def assign_auto_names(parsed_capsules)
      first_queue_counts = parsed_capsules.each_with_object(Hash.new(0)) do |capsule, h|
        h[capsule[:queues].first] += 1
      end

      parsed_capsules.map do |capsule|
        first = capsule[:queues].first
        nameable = first != CapsuleDSL::WILDCARD && first_queue_counts[first] == 1
        nameable ? capsule.merge(name: first.to_s) : capsule
      end
    end

    # Validates that the new capsule (added via +c.capsule :name, ...+) does
    # not overlap with any existing NAMED capsule. Anonymous capsules (parsed
    # from the string DSL with auto-naming skipped, e.g. wildcards or
    # would-collide first-queues) are intentionally invisible here — they
    # represent "N forks of the same pool" and are allowed to overlap with
    # each other and with named capsules.
    #
    # The wildcard '*' counts as overlapping with EVERY other queue (and
    # vice versa) because at runtime '*' is expanded to all known queues.
    # Raises ArgumentError on overlap.
    def validate_no_queue_overlap!(new_queues)
      existing_named = (@workers || []).select { |c| capsule_name(c) }
      return if existing_named.empty?

      existing_queues = existing_named.flat_map { |c| c[:queues] || c["queues"] || [] }
      return if existing_queues.empty?

      if existing_queues.include?(CapsuleDSL::WILDCARD)
        raise ArgumentError,
              "an existing named capsule already uses '*' (matches every queue) — " \
              "the new capsule's queues #{new_queues.inspect} would overlap with it"
      end

      if new_queues.include?(CapsuleDSL::WILDCARD)
        raise ArgumentError,
              "the new capsule uses '*' (matches every queue) but other named capsules " \
              "are already defined with queues #{existing_queues.inspect} — " \
              "the wildcard would overlap with all of them"
      end

      conflict = new_queues.find { |q| existing_queues.include?(q) }
      return unless conflict

      raise ArgumentError,
            "queue #{conflict.inspect} is already assigned to another named capsule — " \
            "named capsules cannot share queues"
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
