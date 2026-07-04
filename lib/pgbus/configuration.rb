# frozen_string_literal: true

require "logger"

module Pgbus
  class Configuration
    # Connection settings
    attr_accessor :database_url, :connection_params, :pool_size, :pool_timeout

    # Queue settings
    attr_accessor :default_queue, :queue_prefix

    # Worker settings
    attr_accessor :polling_interval, :prefetch_limit, :execution_mode
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

    # Liveness probe: supervisor kills a worker whose claim loop has not
    # advanced for longer than stall_threshold seconds (default 90).
    # read_timeout caps how long a single PGMQ read can block (default 30s),
    # so a dead socket raises instead of parking the loop forever.
    attr_accessor :stall_threshold, :read_timeout

    # Dispatcher settings
    attr_accessor :dispatch_interval

    # Circuit breaker. Only `enabled` is user-facing — the trip threshold and
    # backoff curve are tuned via constants on Pgbus::CircuitBreaker because
    # they are implementation details that have never been worth exposing.
    attr_accessor :circuit_breaker_enabled

    # Dead letter queue
    attr_accessor :max_retries

    # Retry backoff for the VT-based retry path (unhandled exceptions).
    # Jobs can override these per-class via Pgbus::RetryBackoff::JobMixin.
    attr_accessor :retry_backoff, :retry_backoff_max, :retry_backoff_jitter

    # Priority queues
    attr_accessor :priority_levels, :default_priority

    # Grouped reads (PGMQ v1.11.0+ FIFO grouping).
    # nil = disabled (default read_batch behavior).
    # :fifo = use read_grouped (drains oldest group first, throughput-optimized).
    # :round_robin = use read_grouped_rr (fair round-robin across groups).
    attr_reader :group_mode

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
    attr_reader :log_format # rubocop:disable Style/AccessorGrouping

    # Error reporting — array of callable objects invoked on caught exceptions.
    # Each receives (exception, context_hash) or (exception, context_hash, config).
    attr_accessor :error_reporters

    # LISTEN/NOTIFY. Only the on/off switch is user-facing — the throttle
    # interval is a Postgres-side tuning knob that lives as a constant on
    # Pgbus::Client (NOTIFY_THROTTLE_MS).
    attr_accessor :listen_notify

    # PGMQ schema installation mode (:auto, :extension, :embedded)
    attr_reader :pgmq_schema_mode

    # Event consumers
    attr_reader :event_consumers

    # Recurring jobs
    attr_accessor :recurring_tasks, :recurring_schedule_interval, :recurring_tasks_file, :skip_recurring
    attr_writer :recurring_tasks_files
    attr_reader :recurring_execution_retention # rubocop:disable Style/AccessorGrouping

    # Multi-database support (optional separate database for pgbus tables)
    # Set to { database: { writing: :pgbus, reading: :pgbus } } to use a separate database.
    # Requires a matching entry in config/database.yml under the "pgbus" key.
    attr_accessor :connects_to

    # Zombie message detection — logs a warning when a message is redelivered
    # (read_ct > 1) without any prior failure recorded in pgbus_failed_events.
    attr_accessor :zombie_detection

    # Job stats
    attr_accessor :stats_enabled, :stats_flush_size, :stats_flush_interval
    attr_reader :stats_retention # rubocop:disable Style/AccessorGrouping

    # Web dashboard
    attr_accessor :web_auth, :web_refresh_interval, :web_per_page, :web_live_updates, :web_data_source,
                  :insights_default_minutes, :base_controller_class, :return_to_app_url,
                  :metrics_enabled,
                  :dashboard_filter_parameters, :dashboard_filter_sensitive

    # HTTP health endpoints (liveness/readiness for orchestrators like
    # Kubernetes). `health_port` nil (default) leaves the standalone server in
    # the supervisor disabled — mount Pgbus::Web::HealthApp in the host Rails
    # app instead. When set, Supervisor#run serves /livez and /readyz over a
    # plain TCPServer bound to `health_bind` (default localhost).
    attr_accessor :health_port, :health_bind

    # Streams (turbo-rails replacement, SSE-based)
    attr_accessor :streams_enabled, :streams_path, :streams_queue_prefix, :streams_signed_name_secret,
                  :streams_default_retention, :streams_retention, :streams_heartbeat_interval,
                  :streams_max_connections, :streams_idle_timeout, :streams_listen_health_check_ms,
                  :streams_write_deadline_ms, :streams_falcon_streaming_body,
                  :streams_stats_enabled, :streams_test_mode,
                  :streams_orphan_sweep_interval, :streams_orphan_threshold,
                  :streams_durable_patterns,
                  :streams_presence_patterns, :streams_presence_member,
                  :streams_host, :streams_port, :streams_database_url
    attr_reader :streams_default_broadcast_mode # rubocop:disable Style/AccessorGrouping

    # NOTIFY-gated worker wakeups. When true, each Worker fork owns a
    # dedicated NotifyListener PG connection that LISTENs on its queues'
    # INSERT channels and wakes the loop on a real insert. Defaults to the
    # value of listen_notify. The worker_notify_* overrides mirror
    # streams_* so the LISTEN connection can bypass PgBouncer.
    attr_accessor :worker_notify_wakeup,
                  :worker_notify_host, :worker_notify_port, :worker_notify_database_url

    # AppSignal integration (auto-loaded when ::Appsignal is defined and this is true).
    # Set to false to opt out without uninstalling the appsignal gem.
    attr_accessor :appsignal_enabled, :appsignal_probe_enabled

    # Generic metrics adapter (Pgbus::Metrics). Consumes the same pgbus.*
    # instrumentation events as AppSignal and forwards them to a backend:
    #   nil (default) — install nothing, zero overhead
    #   :prometheus   — in-process registry, scraped via PrometheusExporter
    #   :statsd       — UDP datagrams to statsd_host:statsd_port
    #   a Pgbus::Metrics::Backend instance — a custom backend
    attr_accessor :metrics_backend, :statsd_host, :statsd_port

    # When true (default), Pgbus.configure and ConfigLoader.apply run
    # validate! after applying settings, so an invalid value fails loud at
    # boot instead of dormant until a worker path consumes it. Set false to
    # opt out (exotic setups that intentionally hold a transiently-invalid
    # config); explicit validate! still works.
    attr_accessor :eager_validation

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
      @execution_mode = :threads

      @max_jobs_per_worker = nil
      @max_memory_mb = nil
      @max_worker_lifetime = nil

      @stall_threshold = 90
      @read_timeout = 30

      @dispatch_interval = 1.0

      @circuit_breaker_enabled = true

      @max_retries = 5
      @retry_backoff = 5          # seconds — first VT-retry delay
      @retry_backoff_max = 300    # 5 minutes cap
      @retry_backoff_jitter = 0.15

      @priority_levels = nil
      @default_priority = 1
      @group_mode = nil

      @archive_retention = 7 * 24 * 3600 # 7 days

      @outbox_enabled = false
      @outbox_poll_interval = 1.0
      @outbox_batch_size = 100
      @outbox_retention = 24 * 3600 # 1 day

      @idempotency_ttl = 7 * 24 * 3600 # 7 days
      @allowed_global_id_models = nil # nil = allow all (for backwards compat)

      @logger = (defined?(Rails) && Rails.respond_to?(:logger) && Rails.logger) || Logger.new($stdout)
      @log_format = :text
      @error_reporters = []

      @listen_notify = true

      @worker_notify_wakeup = nil
      @worker_notify_host = nil
      @worker_notify_port = nil
      @worker_notify_database_url = nil

      @pgmq_schema_mode = :auto

      @event_consumers = nil

      @recurring_tasks = nil
      @recurring_schedule_interval = 1.0
      @recurring_tasks_file = nil
      @recurring_tasks_files = nil
      @skip_recurring = false
      @recurring_execution_retention = 7 * 24 * 3600 # 7 days

      @zombie_detection = true

      @stats_enabled = true
      @stats_retention = 30 * 24 * 3600 # 30 days
      # StatBuffer flush thresholds. Buffered stats are lost on SIGKILL, so a
      # smaller size/interval shrinks the residual loss window at the cost of
      # more frequent bulk inserts. Defaults mirror StatBuffer's own constants.
      @stats_flush_size = StatBuffer::DEFAULT_FLUSH_SIZE
      @stats_flush_interval = StatBuffer::DEFAULT_FLUSH_INTERVAL

      @connects_to = nil

      @web_auth = nil
      @web_refresh_interval = 5000
      @web_per_page = 25
      @web_live_updates = true
      @web_data_source = nil
      @insights_default_minutes = 60 # 1 hour
      @base_controller_class = "::ActionController::Base"
      @return_to_app_url = nil
      @metrics_enabled = true
      @dashboard_filter_parameters = nil # nil = auto-detect from Rails, then fall back to defaults
      @dashboard_filter_sensitive = true

      # HTTP health endpoints — nil port disables the standalone server.
      @health_port = nil
      @health_bind = "127.0.0.1"

      @streams_enabled = true
      @streams_path = nil
      @streams_queue_prefix = "pgbus_stream"
      # Streamer-only connection overrides. The Streamer's Listener owns a
      # dedicated long-lived `wait_for_notify` PG connection that can't go
      # through a PgBouncer in transaction mode (LISTEN/NOTIFY don't survive
      # transaction-pool COMMIT boundaries — see PlanetScale's docs). Setting
      # any of these overrides only the Streamer's connection options; the
      # worker, dispatcher, and client publish paths keep using the regular
      # `database_url` / `connection_params` (typically pooled).
      #
      #   streams_host          — override host only
      #   streams_port          — override port only (most common case:
      #                           pooler is on 6432, direct is 5432)
      #   streams_database_url  — full URL override; takes precedence over
      #                           the host/port surgicals when set
      @streams_host = nil
      @streams_port = nil
      @streams_database_url = nil
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
      @streams_test_mode = false
      @streams_default_broadcast_mode = :ephemeral
      @streams_orphan_sweep_interval = 3600    # 1 hour
      @streams_orphan_threshold = 86_400       # 24 hours
      @streams_durable_patterns = []
      # Streams matching these patterns get connection-driven presence:
      # auto-join on SSE connect, auto-leave on disconnect, touch on the
      # keepalive heartbeat (issue #169). Empty by default (opt-in).
      @streams_presence_patterns = []
      # Extracts a presence member { id:, metadata: } from a connection's
      # authorize-hook context. Defaults to nil, which uses the built-in
      # extractor (see #presence_member_for): a Hash with :member_id/:id,
      # or an object responding to #id.
      @streams_presence_member = nil

      # AppSignal: auto-on when the appsignal gem is loaded; probe runs in
      # the same process, so the operator can disable it independently.
      @appsignal_enabled = true
      @appsignal_probe_enabled = true

      # Generic metrics adapter: off by default (no subscription installed).
      @metrics_backend = nil
      @statsd_host = "127.0.0.1"
      @statsd_port = 8125

      @eager_validation = true
    end

    def queue_name(name)
      full = "#{queue_prefix}_#{name}"
      QueueNameValidator.normalize(full)
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

    # Returns the execution mode for a specific worker config hash,
    # falling back to the global execution_mode setting.
    def execution_mode_for(worker_config)
      mode = worker_config[:execution_mode] || execution_mode
      ExecutionPools.normalize_mode(mode)
    end

    VALID_LOG_FORMATS = %i[text json].freeze

    def log_format=(format)
      format = format.to_sym
      unless VALID_LOG_FORMATS.include?(format)
        raise Pgbus::ConfigurationError, "Invalid log_format: #{format}. Must be one of: #{VALID_LOG_FORMATS.join(", ")}"
      end

      @log_format = format
      @logger.formatter = case format
                          when :json then LogFormatter::JSON.new
                          when :text then LogFormatter::Text.new
                          end
    end

    VALID_BROADCAST_MODES = %i[ephemeral durable].freeze

    def streams_default_broadcast_mode=(mode)
      mode = mode.to_sym
      unless VALID_BROADCAST_MODES.include?(mode)
        raise Pgbus::ConfigurationError,
              "Invalid streams_default_broadcast_mode: #{mode}. Must be one of: #{VALID_BROADCAST_MODES.join(", ")}"
      end

      @streams_default_broadcast_mode = mode
    end

    VALID_GROUP_MODES = [nil, :fifo, :round_robin].freeze

    def group_mode=(mode)
      coerced = case mode
                when nil then nil
                when Symbol then mode
                when String then mode.to_sym
                else
                  raise Pgbus::ConfigurationError,
                        "Invalid group_mode type: #{mode.class}. Must be nil, String, or Symbol"
                end
      unless VALID_GROUP_MODES.include?(coerced)
        raise Pgbus::ConfigurationError, "Invalid group_mode: #{coerced.inspect}. Must be nil, :fifo, or :round_robin"
      end

      @group_mode = coerced
    end

    VALID_PGMQ_SCHEMA_MODES = %i[auto extension embedded].freeze

    def pgmq_schema_mode=(mode)
      mode = mode.to_sym
      unless VALID_PGMQ_SCHEMA_MODES.include?(mode)
        raise Pgbus::ConfigurationError, "Invalid pgmq_schema_mode: #{mode}. Must be one of: #{VALID_PGMQ_SCHEMA_MODES.join(", ")}"
      end

      @pgmq_schema_mode = mode
    end

    def validate!
      if pool_size && !(pool_size.is_a?(Numeric) && pool_size.positive?)
        raise Pgbus::ConfigurationError, "pool_size must be a positive number or nil (auto-tune)"
      end

      raise Pgbus::ConfigurationError, "pool_timeout must be > 0" unless pool_timeout.is_a?(Numeric) && pool_timeout.positive?
      raise Pgbus::ConfigurationError, "polling_interval must be > 0" unless polling_interval.is_a?(Numeric) && polling_interval.positive?
      unless visibility_timeout.is_a?(Numeric) && visibility_timeout.positive?
        raise Pgbus::ConfigurationError,
              "visibility_timeout must be > 0"
      end
      raise Pgbus::ConfigurationError, "max_retries must be >= 0" unless max_retries.is_a?(Integer) && max_retries >= 0
      raise Pgbus::ConfigurationError, "retry_backoff must be > 0" unless retry_backoff.is_a?(Numeric) && retry_backoff.positive?
      unless retry_backoff_max.is_a?(Numeric) && retry_backoff_max.positive?
        raise Pgbus::ConfigurationError,
              "retry_backoff_max must be > 0"
      end
      unless retry_backoff_jitter.is_a?(Numeric) && retry_backoff_jitter >= 0 && retry_backoff_jitter <= 1
        raise Pgbus::ConfigurationError, "retry_backoff_jitter must be between 0 and 1"
      end

      unless stall_threshold.nil? || (stall_threshold.is_a?(Numeric) && stall_threshold.positive?)
        raise Pgbus::ConfigurationError, "stall_threshold must be a positive number or nil to disable"
      end
      unless read_timeout.nil? || (read_timeout.is_a?(Numeric) && read_timeout.positive?)
        raise Pgbus::ConfigurationError, "read_timeout must be a positive number or nil to disable"
      end

      unless stats_flush_size.is_a?(Integer) && stats_flush_size.positive?
        raise Pgbus::ConfigurationError, "stats_flush_size must be a positive integer"
      end
      unless stats_flush_interval.is_a?(Numeric) && stats_flush_interval.positive?
        raise Pgbus::ConfigurationError, "stats_flush_interval must be a positive number"
      end

      unless health_port.nil? || (health_port.is_a?(Integer) && health_port.between?(1, 65_535))
        raise Pgbus::ConfigurationError, "health_port must be an integer between 1 and 65535 or nil to disable"
      end

      raise Pgbus::ConfigurationError, "health_bind must be a non-empty String" unless health_bind.is_a?(String) && !health_bind.empty?

      raise Pgbus::ConfigurationError, "statsd_host must be a non-empty String" unless statsd_host.is_a?(String) && !statsd_host.empty?

      unless statsd_port.is_a?(Integer) && statsd_port.between?(1, 65_535)
        raise Pgbus::ConfigurationError, "statsd_port must be an integer between 1 and 65535"
      end

      # Validate global execution_mode
      validate_execution_mode!(execution_mode)

      Array(workers).each do |w|
        threads = w[:threads] || 5
        raise Pgbus::ConfigurationError, "worker threads must be > 0" unless threads.is_a?(Integer) && threads.positive?

        # Validate per-worker execution_mode override if present
        mode = w[:execution_mode]
        validate_execution_mode!(mode) if mode
      end

      if prefetch_limit && !(prefetch_limit.is_a?(Integer) && prefetch_limit.positive?)
        raise Pgbus::ConfigurationError,
              "prefetch_limit must be > 0"
      end

      if priority_levels && !(priority_levels.is_a?(Integer) && priority_levels >= 1 && priority_levels <= 10)
        raise Pgbus::ConfigurationError, "priority_levels must be an integer between 1 and 10"
      end

      unless insights_default_minutes.is_a?(Integer) && insights_default_minutes.positive?
        raise Pgbus::ConfigurationError, "insights_default_minutes must be a positive integer"
      end

      validate_streams!
      validate_metrics_backend!

      self
    end

    def validate_streams!
      unless streams_default_retention.is_a?(Numeric) && streams_default_retention >= 0
        raise Pgbus::ConfigurationError, "streams_default_retention must be a non-negative number"
      end

      unless streams_max_connections.is_a?(Integer) && streams_max_connections.positive?
        raise Pgbus::ConfigurationError, "streams_max_connections must be a positive integer"
      end

      unless streams_heartbeat_interval.is_a?(Numeric) && streams_heartbeat_interval.positive?
        raise Pgbus::ConfigurationError, "streams_heartbeat_interval must be a positive number"
      end

      unless streams_idle_timeout.is_a?(Numeric) && streams_idle_timeout.positive?
        raise Pgbus::ConfigurationError, "streams_idle_timeout must be a positive number"
      end

      unless streams_listen_health_check_ms.is_a?(Integer) && streams_listen_health_check_ms.positive?
        raise Pgbus::ConfigurationError, "streams_listen_health_check_ms must be a positive integer"
      end

      unless streams_write_deadline_ms.is_a?(Integer) && streams_write_deadline_ms.positive?
        raise Pgbus::ConfigurationError, "streams_write_deadline_ms must be a positive integer"
      end

      raise Pgbus::ConfigurationError, "streams_retention must be a Hash" unless streams_retention.is_a?(Hash)

      if streams_orphan_sweep_interval && !(streams_orphan_sweep_interval.is_a?(Numeric) && streams_orphan_sweep_interval.positive?)
        raise Pgbus::ConfigurationError, "streams_orphan_sweep_interval must be a positive number or nil to disable"
      end

      unless streams_durable_patterns.is_a?(Array)
        raise Pgbus::ConfigurationError,
              "streams_durable_patterns must be an Array of strings/regex"
      end

      unless streams_presence_patterns.is_a?(Array)
        raise Pgbus::ConfigurationError,
              "streams_presence_patterns must be an Array of strings/regex"
      end

      if !streams_presence_member.nil? && !streams_presence_member.respond_to?(:call)
        raise Pgbus::ConfigurationError, "streams_presence_member must respond to #call (a Proc/lambda) or be nil"
      end

      return if streams_orphan_threshold.nil?
      return if streams_orphan_threshold.is_a?(Numeric) && streams_orphan_threshold.positive?

      raise Pgbus::ConfigurationError, "streams_orphan_threshold must be a positive number or nil to disable"
    end

    def validate_metrics_backend!
      value = metrics_backend
      return if value.nil?
      return if %i[prometheus statsd].include?(value)
      return if defined?(Pgbus::Metrics::Backend) && value.is_a?(Pgbus::Metrics::Backend)

      raise Pgbus::ConfigurationError,
            "metrics_backend must be nil, :prometheus, :statsd, or a " \
            "Pgbus::Metrics::Backend instance (got #{value.inspect})"
    end

    # Returns true if the given stream name should be durable based on
    # `streams_durable_patterns` (exact string or Regexp match) or the
    # `streams_default_broadcast_mode` fallback.
    def stream_durable?(name)
      patterns = streams_durable_patterns || []
      return true if patterns.any? { |p| p.is_a?(Regexp) ? p.match?(name) : p == name }

      streams_default_broadcast_mode == :durable
    end

    # Returns true if the given stream name should have connection-driven
    # presence based on `streams_presence_patterns` (exact string or
    # Regexp match). Presence is opt-in, so the default (no patterns) is
    # false. See issue #169.
    def stream_presence?(name)
      patterns = streams_presence_patterns || []
      patterns.any? { |p| p.is_a?(Regexp) ? p.match?(name) : p == name }
    end

    # Derives a presence member { id:, metadata: } from a connection's
    # authorize-hook context, or nil when no member can be derived (an
    # anonymous connection — presence is simply skipped). Uses
    # `streams_presence_member` when configured; otherwise the built-in
    # extractor handles the common shapes:
    #   - Hash with :member_id (or :id) and optional :metadata
    #   - an object responding to #id (e.g. a User model)
    # The id is always coerced to a String and metadata defaults to {}.
    def presence_member_for(context)
      return nil if context.nil?

      raw = streams_presence_member ? streams_presence_member.call(context) : default_presence_member(context)
      return nil unless raw.is_a?(Hash)

      id = raw[:id]
      return nil if id.nil? || id.to_s.empty?

      { id: id.to_s, metadata: raw[:metadata] || {} }
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
                   value.map { |entry| normalize_entry(entry, group: "worker") }
                 else
                   raise Pgbus::ConfigurationError,
                         "workers must be a String (DSL), Array (legacy form), or nil — got #{value.class}"
                 end
    end

    # Event consumer configs arrive with symbol keys (Ruby DSL) or string keys
    # (YAML via ConfigLoader). Normalize once here so every read site uses
    # symbol access only. nil passes through (no consumers configured).
    def event_consumers=(value)
      @event_consumers =
        case value
        when nil
          nil
        when Array
          value.map { |entry| normalize_entry(entry, group: "event_consumer") }
        else
          raise Pgbus::ConfigurationError,
                "event_consumers must be an Array or nil — got #{value.class}"
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
      raise Pgbus::ConfigurationError, "capsule queues must be a non-empty Array" unless queues.is_a?(Array) && queues.any?
      raise Pgbus::ConfigurationError, "capsule threads must be a positive Integer" unless threads.is_a?(Integer) && threads.positive?

      normalized_name = name.to_s
      @workers ||= []

      if @workers.any? { |c| capsule_name(c) == normalized_name }
        raise Pgbus::ConfigurationError, "capsule #{name.inspect} is already defined"
      end

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
    # VALID_ROLES. Unknown role names raise Pgbus::ConfigurationError
    # immediately so typos like `[:workres]` fail loud at boot rather than
    # leaving the supervisor idling with no children.
    def roles=(value)
      if value.nil?
        @roles = nil
        return
      end

      normalized = Array(value).map { |r| r.to_s.downcase.to_sym }.uniq
      invalid = normalized - VALID_ROLES
      if invalid.any?
        raise Pgbus::ConfigurationError,
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

    def recurring_tasks_files
      return @recurring_tasks_files if @recurring_tasks_files

      recurring_tasks_file ? [recurring_tasks_file] : nil
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

    # Connections needed per async worker: one for the reactor's serial
    # execution, one for polling, one for headroom. Fibers share connections
    # because only one runs at a time per reactor thread.
    ASYNC_POOL_CONNECTIONS = 3

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

    # Connection options the Streamer's dedicated LISTEN/NOTIFY PG connection
    # should use. Defaults to `connection_options` (same as workers and the
    # publish path). If any of `streams_database_url`, `streams_host`, or
    # `streams_port` is set, the Streamer's connection is reconfigured —
    # everything else keeps using the base options.
    #
    # The typical use is "workers go through PgBouncer, streamer goes direct":
    #
    #   c.connects_to = { database: { writing: :pgbus } }   # pooler
    #   c.streams_port = 5432                               # direct
    #
    # Precedence: streams_database_url > streams_host/port override > base.
    def streams_connection_options
      return streams_database_url if streams_database_url

      base = connection_options
      return base unless streams_host || streams_port

      case base
      when Hash
        result = base.dup
        result[:host] = streams_host if streams_host
        result[:port] = streams_port if streams_port
        result
      when String
        # libpq's conninfo parser takes later key=value pairs as overrides
        # for earlier ones, so we just append. Handles both URI form
        # (postgres://...) and key=value form.
        parts = [base]
        parts << "host=#{streams_host}" if streams_host
        parts << "port=#{streams_port}" if streams_port
        parts.join(" ")
      else
        base
      end
    end

    # Connection options for the Worker's dedicated NotifyListener connection.
    # Mirrors streams_connection_options: defaults to the base connection_options,
    # overridable via worker_notify_database_url / worker_notify_host /
    # worker_notify_port so the LISTEN connection can bypass PgBouncer.
    def worker_notify_connection_options
      return worker_notify_database_url if worker_notify_database_url

      base = connection_options
      return base unless worker_notify_host || worker_notify_port

      case base
      when Hash
        result = base.dup
        result[:host] = worker_notify_host if worker_notify_host
        result[:port] = worker_notify_port if worker_notify_port
        result
      when String
        parts = [base]
        parts << "host=#{worker_notify_host}" if worker_notify_host
        parts << "port=#{worker_notify_port}" if worker_notify_port
        parts.join(" ")
      else
        base
      end
    end

    # Resolved notify wakeup flag: defaults to listen_notify when nil.
    def worker_notify_wakeup?
      if @worker_notify_wakeup.nil?
        listen_notify
      else
        @worker_notify_wakeup
      end
    end

    private

    # Built-in presence-member extractor used when no custom
    # `streams_presence_member` is configured. Returns a { id:, metadata: }
    # Hash or nil; #presence_member_for normalizes the id/metadata.
    def default_presence_member(context)
      if context.is_a?(Hash)
        id = context[:member_id] || context[:id]
        return nil if id.nil?

        { id: id, metadata: context[:metadata] || {} }
      elsif context.respond_to?(:id)
        { id: context.id, metadata: {} }
      end
    end

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

      raise Pgbus::ConfigurationError,
            "#{name} must be a Numeric (seconds), ActiveSupport::Duration, or nil to disable, got #{value.inspect}"
    end

    def validate_positive_duration!(numeric, name)
      raise Pgbus::ConfigurationError, "#{name} must be a positive number, got #{numeric}" unless numeric.positive?

      numeric
    end

    # ExecutionPools.normalize_mode raises ArgumentError for an unknown mode —
    # that's the right shape for the factory's own callers, but a bad
    # execution_mode reaching here is a configuration failure, so re-raise it
    # as ConfigurationError to match the rest of validate! (issue #282).
    def validate_execution_mode!(mode)
      ExecutionPools.normalize_mode(mode)
    rescue ArgumentError => e
      raise Pgbus::ConfigurationError, e.message
    end

    # Symbolize the top-level keys of a worker/consumer config entry so every
    # downstream read site can use symbol access only. YAML (via ConfigLoader)
    # yields string keys; the Ruby DSL and +capsule+ builder yield symbols.
    # Entries are flat hashes with array/scalar values — no deep recursion.
    def normalize_entry(entry, group:)
      raise Pgbus::ConfigurationError, "#{group} entry must be a Hash, got #{entry.class}: #{entry.inspect}" unless entry.is_a?(Hash)

      entry.transform_keys(&:to_sym)
    end

    # Read a capsule's name (symbol key after normalization), as a string for
    # comparison. Returns nil for unnamed (legacy) entries.
    def capsule_name(entry)
      entry[:name]&.to_s
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

      existing_queues = existing_named.flat_map { |c| c[:queues] || [] }
      return if existing_queues.empty?

      if existing_queues.include?(CapsuleDSL::WILDCARD)
        raise Pgbus::ConfigurationError,
              "an existing named capsule already uses '*' (matches every queue) — " \
              "the new capsule's queues #{new_queues.inspect} would overlap with it"
      end

      if new_queues.include?(CapsuleDSL::WILDCARD)
        raise Pgbus::ConfigurationError,
              "the new capsule uses '*' (matches every queue) but other named capsules " \
              "are already defined with queues #{existing_queues.inspect} — " \
              "the wildcard would overlap with all of them"
      end

      conflict = new_queues.find { |q| existing_queues.include?(q) }
      return unless conflict

      raise Pgbus::ConfigurationError,
            "queue #{conflict.inspect} is already assigned to another named capsule — " \
            "named capsules cannot share queues"
    end

    def sum_thread_counts(entries, default_threads:, group:)
      return 0 unless entries

      entries.sum do |entry|
        threads = entry[:threads] || default_threads
        unless threads.is_a?(Integer) && threads.positive?
          raise Pgbus::ConfigurationError,
                "#{group} threads must be a positive integer, got #{threads.inspect}"
        end

        if async_execution_mode?(entry)
          ASYNC_POOL_CONNECTIONS
        else
          threads
        end
      end
    end

    def async_execution_mode?(entry)
      execution_mode_for(entry) == :async
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
