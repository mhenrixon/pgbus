# frozen_string_literal: true

require "json"
require "socket"
require "timeout"
require_relative "client/read_after"
require_relative "client/ensure_stream_queue"
require_relative "client/notify_stream"
require_relative "client/connection_health"

module Pgbus
  class Client
    include ReadAfter
    include EnsureStreamQueue
    include NotifyStream

    attr_reader :pgmq, :config, :connection_health

    PGMQ_REQUIRE_MUTEX = Mutex.new
    private_constant :PGMQ_REQUIRE_MUTEX

    # Throttle window for PGMQ's enable_notify_insert trigger. Postgres
    # NOTIFYs are coalesced into one wake-up per window, so a value of 250ms
    # means: at most 4 broadcasts/sec per queue, regardless of insert rate.
    # The trigger is a Postgres-level concern; exposing it as a setting
    # never came up in practice and changing it on the fly would require
    # re-running the trigger DDL on every queue.
    NOTIFY_THROTTLE_MS = 250

    def initialize(config = Pgbus.configuration)
      # Define the PGMQ module before requiring the gem so that Zeitwerk's
      # eager_load (called inside pgmq.rb) can resolve the constant.
      # Without this, Ruby 4.0 + Zeitwerk 2.7.5 raises NameError because
      # eager_load runs const_get(:Client) on PGMQ before the module is defined.
      PGMQ_REQUIRE_MUTEX.synchronize do
        Object.const_set(:PGMQ, Module.new) unless defined?(::PGMQ)
        require "pgmq"
      end
      @config = config
      conn_opts = config.connection_options
      @shared_connection = conn_opts.is_a?(Proc)

      if @shared_connection
        # When using the Rails lambda path (-> { AR::Base.connection.raw_connection }),
        # the Proc returns the same underlying PG::Connection that ActiveRecord uses.
        # PG::Connection (libpq) is not thread-safe — concurrent access causes
        # segfaults and result corruption. Force pool_size=1 and serialize all
        # operations through a mutex.
        @pgmq = PGMQ::Client.new(conn_opts, pool_size: 1, pool_timeout: config.pool_timeout)
        @pgmq_mutex = Mutex.new
      else
        # With a String URL or Hash params, pgmq-ruby creates its own dedicated
        # PG::Connection per pool slot — no shared state with ActiveRecord.
        # Use the resolved pool size (auto-tuned from worker thread counts
        # unless explicitly set) and let pgmq-ruby's connection_pool handle
        # concurrency internally (no mutex needed).
        #
        # Bound reads with libpq-native mechanisms baked into the connection
        # options (issue #198): a server-side statement_timeout for a slow query,
        # plus client-side tcp_user_timeout + keepalives for a dead/hung socket.
        # Both raise clean PG errors — no Ruby Timeout, no Thread#raise. Only
        # safe on this dedicated-connection branch — never on the shared-AR Proc
        # path, where statement_timeout would leak into application queries.
        conn_opts = apply_connection_bounds(conn_opts)
        @pgmq = PGMQ::Client.new(conn_opts, pool_size: config.resolved_pool_size, pool_timeout: config.pool_timeout)
        @pgmq_mutex = nil
      end

      @queues_created = Concurrent::Map.new
      @stream_indexes_created = Concurrent::Map.new
      @queue_strategy = QueueFactory.for(config)
      @schema_ensured = false
      @connection_health = ConnectionHealth.new(
        on_open: method(:log_circuit_open),
        on_close: method(:log_circuit_close)
      )
      # Snapshot whether libpq's baked-in read bounds fully cover a hung socket
      # on this host/connection, so the read path can skip the Ruby Timeout
      # last resort. Computed once: @shared_connection, config.read_timeout
      # (which apply_connection_bounds also snapshots), the platform, and the
      # linked libpq version are all fixed for a Client's lifetime.
      @libpq_read_bounds_effective = libpq_read_bounds_effective?
      warn_shared_connection_read_bounds
    end

    # Actively open a database connection and run `SELECT 1` so a bad
    # database_url / connection_params surfaces at boot instead of on the
    # first operation. PGMQ::Client's pool is lazy — nothing touches the
    # database at init — so without this the supervisor forks children that
    # crash-loop against an unreachable DB. Called from Supervisor#run before
    # any queue bootstrap or forking.
    #
    # Raises Pgbus::ConfigurationError (not a transient PGMQ error) because a
    # failure here means the operator's connection config is wrong: the message
    # carries the underlying error plus which config source was in use.
    def verify_connection!
      synchronized do
        @pgmq.with_connection { |conn| conn.exec("SELECT 1") }
      end
      true
    rescue PGMQ::Errors::ConnectionError, PG::Error => e
      raise ConfigurationError, "Database connection failed via #{connection_source}: #{e.message}"
    end

    # Lightweight liveness probe used by the doctor: open a raw connection and
    # run `SELECT 1`. Unlike verify_connection! (which wraps failures as
    # ConfigurationError for the supervisor boot path), ping lets the raw
    # PG/PGMQ error propagate so the caller can render the underlying reason.
    # Returns true on success; a bad connection raises rather than returning
    # false — the caller renders the underlying reason — so this is a probe,
    # not a boolean predicate, hence no `?` suffix.
    def ping # rubocop:disable Naming/PredicateMethod
      with_raw_connection { |conn| conn.exec("SELECT 1") }
      true
    end

    # The logical queue names pgbus expects to exist based on the configuration
    # (default queue + worker capsules + recurring tasks). Public wrapper around
    # collect_configured_queues so the doctor can diff configured-vs-existing
    # queues without reaching into PGMQ or config internals directly.
    def configured_queues
      collect_configured_queues
    end

    # Whether the given logical queue currently has a live PGMQ insert-NOTIFY
    # trigger with pgbus's throttle interval on every physical table it maps to.
    # Uses the same physical-name resolution as bootstrap (@queue_strategy), so
    # a priority queue's _p0.._pN sub-tables — where the trigger actually lives —
    # are all checked, not the bare prefixed name that priority mode never
    # creates. Returns false when any physical table lacks the trigger or the
    # check can't run.
    def notify_enabled?(queue_name)
      names = @queue_strategy.physical_queue_names(queue_name)
      names.all? { |physical| notify_trigger_current?(physical, NOTIFY_THROTTLE_MS) }
    end

    # The physical PGMQ queue table names a logical queue maps to — one for a
    # standard queue, or the _p0.._pN sub-queues when priority is enabled. This
    # is the SAME resolution the bootstrap path uses (@queue_strategy), so a
    # caller diffing configured-vs-existing queues compares the exact names PGMQ
    # actually holds rather than the bare prefixed name.
    def physical_queue_names(logical_name)
      @queue_strategy.physical_queue_names(logical_name)
    end

    # Whether the PGMQ schema itself is present (the pgmq.meta table exists),
    # independent of pgbus's own version-tracking table. Lets a caller tell
    # "PGMQ installed via the extension / before version tracking" (schema
    # present, no tracking row) apart from "PGMQ not installed at all".
    def pgmq_installed?
      with_raw_connection do |conn|
        result = conn.exec(
          "SELECT 1 FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'meta' LIMIT 1"
        )
        result.ntuples.positive?
      end
    end

    # The most recently recorded installed PGMQ schema version string (e.g.
    # "1.5.0"), read from the pgbus_pgmq_schema_versions tracking table. Returns
    # nil when nothing is tracked yet or the table does not exist — the same
    # logic the `pgbus:pgmq:status` rake task uses, kept here so the doctor and
    # the rake task share one raw-SQL path (never SQL outside the Client).
    def pgmq_schema_version
      with_raw_connection do |conn|
        result = conn.exec(
          "SELECT version FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
        )
        row = result.first
        row && row["version"]
      end
    rescue ActiveRecord::StatementInvalid => e
      raise unless undefined_table_error?(e)

      nil
    rescue StandardError => e
      raise unless defined?(PG::UndefinedTable) && e.is_a?(PG::UndefinedTable)

      nil
    end

    def ensure_queue(name)
      ensure_pgmq_schema
      @queue_strategy.physical_queue_names(name).each { |pq| ensure_single_queue(pq) }
    end

    def ensure_all_queues
      queue_names = collect_configured_queues
      Pgbus.logger.info { "[Pgbus] Bootstrapping #{queue_names.size} queue(s): #{queue_names.join(", ")}" }
      queue_names.each { |name| ensure_queue(name) }
    end

    def ensure_dead_letter_queue(name)
      dlq_name = config.dead_letter_queue_name(name)
      return if @queues_created[dlq_name]

      @queues_created.compute_if_absent(dlq_name) do
        synchronized do
          @pgmq.create(dlq_name)
          tune_autovacuum(dlq_name)
        end
        true
      end
    end

    def send_message(queue_name, payload, headers: nil, delay: 0, priority: nil)
      target = @queue_strategy.target_queue(queue_name, priority)
      Instrumentation.instrument("pgbus.client.send_message", queue: target) do
        with_stale_connection_retry do
          ensure_queue(queue_name)
          synchronized { @pgmq.produce(target, serialize(payload), headers: headers && serialize(headers), delay: delay) }
        end
      end
    end

    def send_batch(queue_name, payloads, headers: nil, delay: 0)
      full_name = config.queue_name(queue_name)
      serialized, serialized_headers = serialize_batch(payloads, headers)
      Instrumentation.instrument("pgbus.client.send_batch", queue: full_name, size: payloads.size) do
        with_stale_connection_retry do
          ensure_queue(queue_name)
          synchronized { @pgmq.produce_batch(full_name, serialized, headers: serialized_headers, delay: delay) }
        end
      end
    end

    def read_message(queue_name, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_message", queue: full_name) do
          with_stale_connection_retry do
            synchronized { with_read_timeout { @pgmq.read(full_name, vt: vt || config.visibility_timeout) } }
          end
        end
      end
    end

    def read_batch(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_batch", queue: full_name, qty: qty) do
          with_stale_connection_retry do
            synchronized { with_read_timeout { @pgmq.read_batch(full_name, vt: vt || config.visibility_timeout, qty: qty) } }
          end
        end
      end
    end

    # Read from priority sub-queues, highest priority (p0) first.
    # Returns [priority_queue_name, messages] pairs.
    def read_batch_prioritized(queue_name, qty:, vt: nil)
      # Non-priority fast path delegates to read_batch, which is already gated
      # by the connection-health breaker — no extra guard needed here.
      unless @queue_strategy.priority?
        return (read_batch(queue_name, qty: qty, vt: vt) || []).map do |m|
          [config.queue_name(queue_name), m]
        end
      end

      # The priority loop issues its own reads, so gate the whole loop: an open
      # breaker fails fast before any sub-queue is touched, and the loop as a
      # unit records one success/failure with the latch.
      guarded_read do
        remaining = qty
        results = []

        config.priority_queue_names(queue_name).each do |pq_name|
          break if remaining <= 0

          msgs = Instrumentation.instrument("pgbus.client.read_batch", queue: pq_name, qty: remaining) do
            with_stale_connection_retry do
              synchronized { with_read_timeout { @pgmq.read_batch(pq_name, vt: vt || config.visibility_timeout, qty: remaining) } }
            end
          end || []

          msgs.each { |m| results << [pq_name, m] }
          remaining -= msgs.size
        end

        results
      end
    end

    def read_with_poll(queue_name, qty:, vt: nil, max_poll_seconds: 5, poll_interval_ms: 100)
      full_name = config.queue_name(queue_name)
      guarded_read do
        with_stale_connection_retry do
          synchronized do
            @pgmq.read_with_poll(
              full_name,
              vt: vt || config.visibility_timeout,
              qty: qty,
              max_poll_seconds: max_poll_seconds,
              poll_interval_ms: poll_interval_ms
            )
          end
        end
      end
    end

    # Read from multiple queues in a single SQL query (UNION ALL).
    # Each returned message includes a queue_name field identifying its source.
    # queue_names should be logical names (prefix is added automatically).
    #
    # `qty` is the per-queue cap (pgmq-ruby semantics), so without `limit:` the
    # caller receives up to `queue_count * qty` messages. Pass `limit:` to cap
    # the total across all queues — required when feeding a fixed-size pool,
    # otherwise the pool can overflow on multi-queue reads (issue #123).
    def read_multi(queue_names, qty:, vt: nil, limit: nil)
      full_names = queue_names.map { |q| config.queue_name(q) }
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_multi", queues: full_names, qty: qty, limit: limit) do
          with_stale_connection_retry do
            synchronized do
              with_read_timeout do
                @pgmq.read_multi(full_names, vt: vt || config.visibility_timeout, qty: qty, limit: limit)
              end
            end
          end
        end
      end
    end

    # Delete a message. Pass prefixed: false when queue_name is already
    # the full PGMQ queue name (e.g. from priority sub-queues or dashboard).
    def delete_message(queue_name, msg_id, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.delete(name, msg_id) }
      end
    end

    # Archive a message. Pass prefixed: false when queue_name is already
    # the full PGMQ queue name.
    def archive_message(queue_name, msg_id, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.archive(name, msg_id) }
      end
    end

    # Batch archive — moves multiple messages to the archive table in one call.
    def archive_batch(queue_name, msg_ids, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.archive_batch(name, msg_ids) }
      end
    end

    # Batch delete — permanently removes multiple messages in one call.
    def delete_batch(queue_name, msg_ids, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.delete_batch(name, msg_ids) }
      end
    end

    # Set visibility timeout. Pass prefixed: false when queue_name is already
    # the full PGMQ queue name.
    def set_visibility_timeout(queue_name, msg_id, vt:, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.set_vt(name, msg_id, vt: vt) }
      end
    end

    # Open a PGMQ transaction. The caller block may run twice if the first
    # attempt hits a pre-flight stale-connection error — safe because no SQL
    # was sent on the first attempt (the connection was dead before the BEGIN).
    def transaction(&block)
      with_stale_connection_retry do
        synchronized { @pgmq.transaction(&block) }
      end
    end

    def move_to_dead_letter(queue_name, message)
      dlq_name = config.dead_letter_queue_name(queue_name)
      full_queue = config.queue_name(queue_name)

      with_stale_connection_retry do
        ensure_dead_letter_queue(queue_name)
        synchronized do
          @pgmq.transaction do |txn|
            txn.produce(dlq_name, message.message, headers: message.headers)
            txn.delete(full_queue, message.msg_id.to_i)
          end
        end
      end
    end

    def metrics(queue_name = nil)
      with_stale_connection_retry do
        synchronized do
          if queue_name
            @pgmq.metrics(config.queue_name(queue_name))
          else
            @pgmq.metrics_all
          end
        end
      end
    end

    # Snapshot of the PGMQ connection pool: {size:, available:, pool_timeout:}.
    #
    # Reads pgmq-ruby's own pool counters (@pgmq.stats -> {size:, available:})
    # and adds the configured pool_timeout so alerting has the full picture:
    # how many connections exist, how many are free right now, and how long a
    # checkout waits before raising a pool-timeout error. Works on both the
    # dedicated-pool path and the shared-Proc path (where size is 1).
    #
    # Purely observational — wrapped in a rescue that returns {} so a probe or
    # heartbeat reading the pool can never break job processing. Not routed
    # through with_stale_connection_retry: reading in-memory counters touches no
    # socket, and a failing read must degrade to {} rather than retry.
    def pool_stats
      @pgmq.stats.merge(pool_timeout: config.pool_timeout)
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus::Client] pool_stats unavailable: #{e.class}: #{e.message}" }
      {}
    end

    def list_queues
      with_stale_connection_retry do
        synchronized { @pgmq.list_queues }
      end
    end

    def purge_queue(queue_name, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.purge_queue(name) }
      end
    end

    def drop_queue(queue_name, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      result = with_stale_connection_retry do
        synchronized { @pgmq.drop_queue(name) }
      end
      @queues_created.delete(name)
      result
    end

    # Check whether a message exists in the given queue.
    #
    # Pass either +msg_id+ for a fast primary-key lookup, or +uniqueness_key+
    # to scan the queue for any message whose payload carries that key in the
    # +pgbus_uniqueness_key+ JSONB field. The latter is used by the dispatcher
    # reaper to determine if a uniqueness lock with msg_id=0 (placeholder)
    # still has a corresponding queue message.
    #
    # +queue_name+ may be either a logical name (e.g. "default") or an already
    # prefixed physical name (e.g. "pgbus_default"). The client normalizes both.
    #
    # Returns:
    #   true  — the message definitely exists in the queue
    #   false — the message definitely does not exist
    #   nil   — could not determine (e.g. queue table missing or unknown error).
    #           Callers MUST treat nil as "exists" for safety.
    def message_exists?(queue_name, msg_id: nil, uniqueness_key: nil)
      has_msg_id = !msg_id.nil?
      has_uniqueness_key = !uniqueness_key.nil?
      raise ArgumentError, "pass exactly one of msg_id or uniqueness_key" unless has_msg_id ^ has_uniqueness_key

      full_name = resolve_full_queue_name(queue_name)
      sanitized = QueueNameValidator.sanitize!(full_name)

      synchronized do
        with_raw_connection do |conn|
          if has_msg_id
            msg_id_present?(conn, sanitized, msg_id.to_i)
          else
            uniqueness_key_present?(conn, sanitized, uniqueness_key)
          end
        end
      end
    rescue ActiveRecord::StatementInvalid => e
      raise unless undefined_table_error?(e)

      nil
    rescue StandardError => e
      raise unless defined?(PG::UndefinedTable) && e.is_a?(PG::UndefinedTable)

      nil
    end

    def purge_archive(queue_name, older_than:, batch_size: 1000)
      full_name = config.queue_name(queue_name)
      sanitized = QueueNameValidator.sanitize!(full_name)
      total = 0

      sql = "DELETE FROM pgmq.a_#{sanitized} " \
            "WHERE ctid = ANY(ARRAY(SELECT ctid FROM pgmq.a_#{sanitized} WHERE enqueued_at < $1 LIMIT $2))"

      loop do
        deleted = synchronized do
          with_raw_connection do |conn|
            conn.exec_params(sql, [older_than, batch_size]).cmd_tuples
          end
        end
        total += deleted
        break if deleted < batch_size
      end

      total
    end

    # --- Grouped reads (PGMQ v1.11.0+) ---

    def read_grouped(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_grouped", queue: full_name, qty: qty) do
          with_stale_connection_retry do
            synchronized { with_read_timeout { @pgmq.read_grouped(full_name, vt: vt || config.visibility_timeout, qty: qty) } }
          end
        end
      end
    end

    def read_grouped_rr(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_grouped_rr", queue: full_name, qty: qty) do
          with_stale_connection_retry do
            synchronized { with_read_timeout { @pgmq.read_grouped_rr(full_name, vt: vt || config.visibility_timeout, qty: qty) } }
          end
        end
      end
    end

    def read_grouped_head(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        with_stale_connection_retry do
          synchronized { with_read_timeout { @pgmq.read_grouped_head(full_name, vt: vt || config.visibility_timeout, qty: qty) } }
        end
      end
    end

    # --- FIFO index management (PGMQ v1.11.0+) ---

    def create_fifo_index(queue_name)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        synchronized { @pgmq.create_fifo_index(full_name) }
      end
    end

    def create_fifo_indexes_all
      with_stale_connection_retry do
        synchronized { @pgmq.create_fifo_indexes_all }
      end
    end

    # --- LISTEN/NOTIFY management (PGMQ v1.11.0+) ---

    def wait_for_notify(queue_name, timeout: nil, &block)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        synchronized { @pgmq.wait_for_notify(full_name, timeout: timeout, &block) }
      end
    end

    def update_notify_insert(queue_name, throttle_interval_ms:)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        synchronized { @pgmq.update_notify_insert(full_name, throttle_interval_ms: throttle_interval_ms) }
      end
    end

    def list_notify_insert_throttles
      with_stale_connection_retry do
        synchronized { @pgmq.list_notify_insert_throttles }
      end
    end

    # --- Archive partitioning (requires pg_partman extension) ---

    def convert_archive_partitioned(queue_name, partition_interval: "10000", retention_interval: "100000",
                                    leading_partition: 10)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        synchronized do
          @pgmq.convert_archive_partitioned(
            full_name,
            partition_interval: partition_interval,
            retention_interval: retention_interval,
            leading_partition: leading_partition
          )
        end
      end
    end

    # Topic routing
    def bind_topic(pattern, queue_name)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        ensure_queue(queue_name)
        synchronized { @pgmq.bind_topic(pattern, full_name) }
      end
    end

    def publish_to_topic(routing_key, payload, headers: nil, delay: 0)
      with_stale_connection_retry do
        synchronized do
          @pgmq.produce_topic(
            routing_key,
            serialize(payload),
            headers: headers && serialize(headers),
            delay: delay
          )
        end
      end
    end

    def close
      synchronized { @pgmq.close }
    end

    private

    # Human-readable label for which config knob supplied the connection
    # options, mirroring Configuration#connection_options' precedence. Used in
    # verify_connection!'s error so the operator knows which setting to fix.
    def connection_source
      if config.database_url
        "database_url"
      elsif config.connection_params
        "connection_params"
      else
        "ActiveRecord-derived connection"
      end
    end

    # Accept either a logical name ("default") or an already-prefixed
    # physical name ("pgbus_default") and return the physical name.
    # Coerces symbols to strings so callers can pass either form.
    def resolve_full_queue_name(queue_name)
      name = queue_name.to_s
      prefix = "#{config.queue_prefix}_"
      name.start_with?(prefix) ? name : config.queue_name(name)
    end

    def msg_id_present?(conn, sanitized, msg_id)
      result = conn.exec_params(
        "SELECT 1 FROM pgmq.q_#{sanitized} WHERE msg_id = $1 LIMIT 1",
        [msg_id]
      )
      result.ntuples.positive?
    end

    def uniqueness_key_present?(conn, sanitized, uniqueness_key)
      result = conn.exec_params(
        "SELECT 1 FROM pgmq.q_#{sanitized} " \
        "WHERE message::jsonb ->> 'pgbus_uniqueness_key' = $1 LIMIT 1",
        [uniqueness_key]
      )
      result.ntuples.positive?
    end

    # Detect "relation does not exist" via the underlying PG error type.
    # Falls back to message matching only if PG::UndefinedTable is undefined
    # (very old pg gem) — never relies on locale-sensitive text.
    def undefined_table_error?(error)
      cause = error.respond_to?(:cause) ? error.cause : nil
      return true if defined?(PG::UndefinedTable) && cause.is_a?(PG::UndefinedTable)

      false
    end

    def collect_configured_queues
      queues = Set.new
      queues << config.default_queue

      # Queues from worker configs
      (config.workers || []).each do |w|
        worker_queues = w[:queues] || [config.default_queue]
        worker_queues.each { |q| queues << q unless q == "*" }
      end

      # Queues from recurring tasks
      (config.recurring_tasks || {}).each_value do |opts|
        opts = opts.transform_keys(&:to_s) if opts.is_a?(Hash)
        queue = opts["queue"] || opts[:queue]
        queues << queue if queue
      end

      queues.to_a
    end

    def ensure_pgmq_schema
      return if @schema_ensured

      synchronized do
        return if @schema_ensured

        with_raw_connection do |raw_conn|
          exists = raw_conn.exec("SELECT 1 FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'meta' LIMIT 1")
          install_pgmq_schema(raw_conn) if exists.ntuples.zero?
        end
        @schema_ensured = true
      end
    rescue StandardError => e
      raise Pgbus::SchemaNotReady,
            "PGMQ schema installation failed (#{e.class}: #{e.message}). " \
            "Ensure the pgbus database exists and migrations have been run."
    end

    def install_pgmq_schema(conn)
      mode = config.pgmq_schema_mode

      case mode
      when :extension
        Pgbus.logger.info { "[Pgbus] PGMQ schema not found — installing via extension" }
        conn.exec("CREATE EXTENSION IF NOT EXISTS pgmq")
      when :embedded
        Pgbus.logger.info { "[Pgbus] PGMQ schema not found — installing embedded SQL" }
        conn.exec(PgmqSchema.install_sql)
      else # :auto
        ext = conn.exec("SELECT 1 FROM pg_available_extensions WHERE name = 'pgmq' LIMIT 1")
        if ext.ntuples.positive?
          Pgbus.logger.info { "[Pgbus] PGMQ schema not found — installing via extension" }
          conn.exec("CREATE EXTENSION IF NOT EXISTS pgmq")
        else
          Pgbus.logger.info { "[Pgbus] PGMQ schema not found — installing embedded SQL" }
          conn.exec(PgmqSchema.install_sql)
        end
      end
    end

    def with_raw_connection
      opts = config.connection_options
      owned = false
      conn = case opts
             when Proc
               opts.call
             when String
               owned = true
               PG.connect(opts)
             when Hash
               owned = true
               PG.connect(**opts)
             else
               raise ConfigurationError, "Cannot resolve raw PG connection from #{opts.class}"
             end
      yield conn
    ensure
      conn&.close if owned
    end

    def ensure_single_queue(full_name)
      return if @queues_created[full_name]

      @queues_created.compute_if_absent(full_name) do
        synchronized do
          @pgmq.create(full_name)
          tune_autovacuum(full_name)
          enable_notify_if_needed(full_name, NOTIFY_THROTTLE_MS)
          create_fifo_index_if_needed(full_name)
        end
        true
      end
    end

    def enable_notify_if_needed(full_name, throttle_ms)
      return unless config.listen_notify
      return if notify_trigger_current?(full_name, throttle_ms)

      @pgmq.enable_notify_insert(full_name, throttle_interval_ms: throttle_ms)
    end

    def create_fifo_index_if_needed(full_name)
      return unless config.group_mode

      @pgmq.create_fifo_index(full_name)
    end

    # Check whether the NOTIFY trigger already exists on this queue with the
    # expected throttle interval. When it does, we can skip the destructive
    # DROP TRIGGER + CREATE TRIGGER cycle that causes deadlocks when multiple
    # forked processes race during bootstrap.
    #
    # Routes through the pooled @pgmq.with_connection (health-checked, reused)
    # rather than opening a fresh PG.connect per queue: on the String/Hash path
    # with_raw_connection did a full TCP/TLS/auth setup for every queue at every
    # supervisor boot — and again in each forked child — churning short-lived
    # connections through the pooler. The checkout here is a sequential sibling
    # of the @pgmq.create call above it (create's own checkout has already been
    # returned), so there is no nested checkout: safe even on the shared-Proc
    # pool_size=1 path.
    def notify_trigger_current?(full_name, throttle_ms)
      @pgmq.with_connection do |conn|
        result = conn.exec_params(<<~SQL, [full_name, throttle_ms])
          SELECT 1
          FROM pg_trigger t
          JOIN pg_class c ON t.tgrelid = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
          WHERE n.nspname = 'pgmq'
            AND c.relname = pgmq.format_table_name($1, 'q')
            AND t.tgname = 'trigger_notify_queue_insert_listeners'
            AND EXISTS (
              SELECT 1 FROM pgmq.notify_insert_throttle
              WHERE queue_name = $1
                AND throttle_interval_ms = $2
            )
          LIMIT 1
        SQL
        result.ntuples.positive?
      end
    rescue StandardError
      # If we can't check (e.g. pgmq schema not fully ready), fall back to
      # the unconditional path — same behavior as before this fix.
      false
    end

    # Apply PGMQ-tuned autovacuum + storage parameters to a queue's tables.
    #
    # Delegates to pgmq-ruby's tune_autovacuum (v0.7+), which sets the same
    # queue/archive parameters pgbus used to apply by hand — vacuum scale
    # factor 0.01/0.05, cost_delay 2/5, analyze scale factor 0.05, and
    # fillfactor 70 on the queue table — plus a vacuum_threshold floor of 50.
    # It quotes/lowercases the table name and runs both ALTER TABLEs in one
    # pooled checkout. Tuning is best-effort: a failure here never blocks a
    # queue from being usable, so we log and move on.
    #
    # Pgbus::AutovacuumTuning is still the source for the migration generators
    # (sql_for_all_queues, sql_for_high_churn_tables) which tune pgbus-owned
    # metadata tables the gem doesn't know about.
    def tune_autovacuum(queue_name)
      @pgmq.tune_autovacuum(queue_name)
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus::Client] Autovacuum tuning failed for #{queue_name}: #{e.message}" }
    end

    # Serialize PGMQ operations through a mutex when sharing a connection
    # with ActiveRecord (Proc path). When pgmq-ruby owns its own connections
    # (String/Hash path), the internal connection_pool handles concurrency.
    def synchronized(&)
      if @pgmq_mutex
        @pgmq_mutex.synchronize(&)
      else
        yield
      end
    end

    # Substrings that indicate the pooled PG::Connection was already dead
    # *before* pgmq-ruby tried to use it — typically killed by a connection
    # pooler (PgBouncer server_idle_timeout / client_idle_timeout), an admin
    # disconnect, or a TCP RST while the slot was idle.
    #
    # Only pre-checkout / pre-flight errors belong here. Mid-flight errors
    # like "server closed the connection" or "connection to server was lost"
    # are excluded because PG may have already committed the INSERT before
    # the socket died, and retrying would duplicate the message.
    #
    # See mensfeld/pgmq-ruby#94.
    STALE_CONNECTION_PATTERNS = [
      "pqsocket() can't get socket descriptor",
      "connection is closed",
      "connection has been closed",
      "connection not open",
      "no connection to the server",
      "ssl error: unexpected eof",
      "ssl syscall error"
    ].freeze
    private_constant :STALE_CONNECTION_PATTERNS

    # How many times a matched stale-connection error is retried before it
    # propagates. Two attempts (not one) so a transient window — a PgBouncer
    # restart or a brief failover — that outlasts the first immediate retry
    # still gets a second, backed-off chance rather than failing an enqueue
    # the caller may never retry.
    STALE_RETRY_ATTEMPTS = 2
    private_constant :STALE_RETRY_ATTEMPTS

    # Backoff before each retry, indexed by (attempt - 1): ~0.1s before the
    # first retry, ~0.5s before the second. Short enough to stay invisible on
    # a healthy path (error-path only — never slept on success) and to not
    # stall a worker loop, long enough to let a pooler/failover window clear.
    STALE_RETRY_DELAYS = [0.1, 0.5].freeze
    private_constant :STALE_RETRY_DELAYS

    # Rescue PGMQ::Errors::ConnectionError if its message matches a known
    # stale-socket pattern, retrying up to STALE_RETRY_ATTEMPTS times with a
    # short backoff (STALE_RETRY_DELAYS) between attempts. pgmq-ruby's
    # auto_reconnect + verify_connection! recovers a single dead pooled socket
    # on the *next* checkout, but a transient window — a PgBouncer restart or a
    # brief failover — can outlast an immediate retry; the backed-off second
    # attempt gives that window time to clear. Other connection errors (pool
    # timeout, misconfiguration, truly unreachable DB) propagate immediately.
    #
    # Wraps every @pgmq.* call site. Pattern matching is intentionally narrow
    # (pre-flight / idle-socket signals only), so retry is safe even for
    # non-idempotent ops like delete/archive — a matched error means the
    # connection was dead *before* pgmq-ruby tried to use it, so no SQL was
    # ever sent. Mid-flight errors like "server closed the connection" are
    # excluded from the pattern list for this reason.

    # Seconds by which the outer bounds (client-side tcp_user_timeout and the
    # Ruby Timeout last resort) exceed the server-side statement_timeout. Sizing
    # the outer bounds a little higher lets a live-but-slow server's clean
    # statement_timeout cancel win the race, so the outer bounds fire only when
    # the peer is genuinely gone. See apply_connection_bounds and with_read_timeout.
    READ_TIMEOUT_SLACK = 5
    private_constant :READ_TIMEOUT_SLACK

    # Bound a read and surface a timeout as Pgbus::ReadTimeoutError. Prefer
    # libpq-native bounds baked into the connection; the Ruby Timeout is a
    # narrow, last-resort fallback used only where libpq cannot bound a hung
    # socket. In order, cleanest to last-resort:
    #
    #   1. statement_timeout (server GUC, baked into the connection) — a slow
    #      query is cancelled by Postgres → PG::QueryCanceled, which pgmq-ruby
    #      wraps as PGMQ::Errors::ConnectionError ("canceling statement due to
    #      statement timeout"); mapping_statement_timeout re-raises it as
    #      Pgbus::ReadTimeoutError. The clean path for a live-but-slow server.
    #   2. tcp_user_timeout / keepalives (client-side libpq, baked into the
    #      connection) — a dead/hung socket makes libpq raise PG::ConnectionBad
    #      synchronously, which pgmq-ruby recognises and reconnects. NO
    #      Thread#raise, no buffer corruption. Linux + libpq >= 12 only.
    #
    # When @libpq_read_bounds_effective (the common production case: Linux,
    # dedicated connection, read_timeout set, libpq >= 12) BOTH bounds are in
    # force and Ruby Timeout is never wired in — pure libpq.
    #
    #   3. Ruby Timeout.timeout — the LAST resort, reached ONLY on a *dedicated*
    #      connection where libpq's socket bound is a no-op: non-Linux hosts
    #      (macOS/BSD/Windows) or a libpq < 12. It interrupts via Thread#raise —
    #      the mechanism issue #198 flags as unsafe — so it is slack-delayed and
    #      used only when there is no libpq alternative on that host.
    #
    #      The shared-AR Proc path deliberately gets NEITHER a baked-in bound NOR
    #      this Ruby Timeout: we don't own that socket, and Thread#raise on a
    #      connection ActiveRecord also queries is the most dangerous place to use
    #      it. Instead the operator configures libpq timeouts in database.yml
    #      (statement_timeout via `variables:`, plus tcp_user_timeout/keepalives),
    #      which AR passes straight through to the connection. #initialize logs a
    #      one-time hint when read_timeout is set on a Proc connection.
    #
    #      KNOWN LIMITATION: when (3) fires on a genuinely hung socket, libpq may
    #      leave the pooled PG::Connection reporting CONNECTION_OK while it will
    #      in fact re-hang on reuse, and pgmq-ruby's health check won't discard
    #      it (it isn't CONNECTION_BAD). The proper fix is a public pool-reload on
    #      pgmq-ruby (follow-up, cf. mensfeld/pgmq-ruby#94); until then it's
    #      documented and confined to the non-Linux dedicated path.
    #
    # MUST wrap only the bare `@pgmq.read*` call, inside both `synchronized` and
    # `with_stale_connection_retry`, so the Timeout clock starts only after the
    # mutex is acquired (a thread queued behind another read is not charged for
    # the wait) and each stale-retry attempt gets its own full budget:
    #
    #   with_stale_connection_retry { synchronized { with_read_timeout { @pgmq.read* } } }
    def with_read_timeout(&block)
      # libpq covers everything (Linux, dedicated conn, read_timeout, libpq>=12),
      # OR this is the Proc path where we defer to AR/database.yml — either way,
      # no Ruby Timeout. Only the dedicated-but-libpq-can't-bound-the-socket case
      # (non-Linux / libpq<12) falls through to the Timeout fallback below.
      return mapping_statement_timeout(&block) if @libpq_read_bounds_effective || @shared_connection

      timeout = config.read_timeout
      return mapping_statement_timeout(&block) unless timeout&.positive?

      # rubocop:disable Pgbus/NoRubyTimeout -- deliberate last-resort bound; see above
      Timeout.timeout(timeout + READ_TIMEOUT_SLACK, Pgbus::ReadTimeoutError) do
        mapping_statement_timeout(&block)
      end
      # rubocop:enable Pgbus/NoRubyTimeout
    end

    # True when libpq's connection-baked read bounds (statement_timeout +
    # tcp_user_timeout + keepalives) fully cover both a slow query AND a
    # dead/hung socket, so with_read_timeout can skip the Ruby Timeout entirely.
    # Requires ALL of:
    #   * a dedicated connection — the shared-AR Proc path has no baked-in bounds
    #     (we don't own that socket; statement_timeout would leak into app queries)
    #   * read_timeout set — apply_connection_bounds no-ops on nil, so skipping
    #     Timeout with no bound installed would leave a read unbounded forever
    #   * TCP_USER_TIMEOUT available — macOS/BSD/Windows no-op it, and keepalives
    #     alone can't bound a stall mid-reply (data sent, never ACKed)
    #   * libpq >= 12 — older libpq rejects the tcp_user_timeout conninfo keyword
    #     outright (it fails the whole connection), so we must not have baked it in
    def libpq_read_bounds_effective?
      return false if @shared_connection
      return false unless config.read_timeout&.positive?
      return false unless Socket.const_defined?(:TCP_USER_TIMEOUT)

      PG.library_version >= 120_000
    end

    # On the shared-AR (Proc) path pgbus doesn't own the connection, so it bakes
    # in no read bounds and deliberately does NOT wrap reads in Ruby Timeout
    # (Thread#raise on a socket ActiveRecord also uses is the most dangerous
    # place for it). When read_timeout is set the operator likely expects reads
    # to be bounded, so point them at the libpq timeouts AR passes through from
    # database.yml — the same bounds pgbus's dedicated path installs itself.
    def warn_shared_connection_read_bounds
      return unless @shared_connection && config.read_timeout&.positive?

      Pgbus.logger.warn do
        "[Pgbus::Client] read_timeout is set but pgbus is sharing ActiveRecord's " \
          "connection, so it can't bound reads itself. Configure libpq timeouts on " \
          "the pgbus connection in database.yml instead: `variables: { statement_timeout: <ms> }` " \
          "plus `tcp_user_timeout: <ms>` and `keepalives: 1` (Linux). Use a dedicated " \
          "database_url/connection_params for pgbus to have it apply these automatically."
      end
    end

    # Substring pgmq-ruby surfaces (wrapped as PGMQ::Errors::ConnectionError)
    # when Postgres cancels a query that overran statement_timeout. Detected in
    # the read paths and re-raised as Pgbus::ReadTimeoutError so the server-side
    # bound preserves the same public contract the Ruby Timeout gave callers.
    STATEMENT_TIMEOUT_PATTERN = "canceling statement due to statement timeout"
    private_constant :STATEMENT_TIMEOUT_PATTERN

    def statement_timeout_error?(error)
      error.message.to_s.downcase.include?(STATEMENT_TIMEOUT_PATTERN)
    end

    # Run a read block, re-raising a server-side statement_timeout cancellation
    # as Pgbus::ReadTimeoutError. Wraps the read call sites so the public
    # contract (ReadTimeoutError on a timed-out read) holds whether the bound
    # fired server-side (the normal case) or via the Ruby Timeout fallback.
    def mapping_statement_timeout
      yield
    rescue PGMQ::Errors::ConnectionError => e
      raise Pgbus::ReadTimeoutError, e.message if statement_timeout_error?(e)

      raise
    end

    # Idle seconds before libpq starts probing a quiet connection with TCP
    # keepalives, and the interval/count of those probes. Sized to detect a
    # dead peer (or a NAT/LB that silently dropped an idle flow) well inside a
    # typical cloud idle-drop window (~350–600s). Pool and LISTEN connections
    # sit idle between reads, so keepalives are what catch a peer that vanished
    # while nothing was in flight. Client-side libpq keywords — never sent in
    # the startup packet, so a pooler (PgBouncer) can't reject them.
    KEEPALIVE_IDLE_SECONDS = 30
    KEEPALIVE_INTERVAL_SECONDS = 10
    KEEPALIVE_COUNT = 3
    private_constant :KEEPALIVE_IDLE_SECONDS, :KEEPALIVE_INTERVAL_SECONDS, :KEEPALIVE_COUNT

    # Bake libpq-native read/connection bounds into the connection options of a
    # dedicated pgmq-ruby connection (issue #198). Two independent libpq
    # mechanisms, deliberately NOT Ruby's Timeout — Timeout interrupts via
    # Thread#raise, which can fire mid-libpq call and corrupt the pooled
    # PG::Connection's result buffer:
    #
    #   1. statement_timeout (server GUC, via `options=-c`) — bounds a query the
    #      server is actively running. Postgres cancels it and sends back
    #      PG::QueryCanceled, which the read paths map to Pgbus::ReadTimeoutError.
    #      This is the bound for a live-but-slow server.
    #   2. tcp_user_timeout + keepalives (client-side libpq conninfo keywords) —
    #      bound a dead/hung socket the server never answers on, where
    #      statement_timeout structurally cannot fire (no live server to cancel).
    #      libpq forces the socket closed and raises PG::ConnectionBad /
    #      PG::UnableToSend synchronously on the calling thread — a clean error
    #      through the normal pgmq path, no Thread#raise, no buffer corruption.
    #      tcp_user_timeout catches death mid-read (data sent, never ACKed);
    #      keepalives catch death on an idle connection.
    #
    # tcp_user_timeout is sized at read_timeout + a small slack so statement_timeout
    # (the clean server-side cancel) wins whenever the server is still answering;
    # the socket bound only fires when the peer is genuinely gone.
    #
    # Called only on the dedicated-connection branch (String URL / Hash params).
    # Never on the shared-AR Proc path — statement_timeout is connection-wide and
    # would leak into application queries, and the socket there is AR's to own.
    #
    # NOTE: statement_timeout is connection-wide, so writes on these connections
    # gain the same bound. Acceptable — an enqueue that can't complete within
    # read_timeout is already failing — and keeps a single server-side mechanism.
    #
    # Returns conn_opts unchanged when read_timeout is nil (bounding disabled).
    def apply_connection_bounds(conn_opts)
      timeout = config.read_timeout
      return conn_opts unless timeout&.positive?

      statement_ms = (timeout * 1000).to_i
      # Socket-death bound sits just above the server-side query bound so a live
      # server's clean statement_timeout cancel always wins the race.
      socket_ms = ((timeout + READ_TIMEOUT_SLACK) * 1000).to_i
      # tcp_user_timeout is a libpq 12+ conninfo keyword; libpq < 12 rejects it
      # outright and fails the whole connection. So only bake in the socket-level
      # keywords when the linked libpq understands them — statement_timeout (a
      # server GUC via `options`) is always safe. Older libpq keeps just the
      # query bound; the Ruby Timeout fallback covers the socket there.
      with_socket = libpq_supports_socket_bounds?

      case conn_opts
      when Hash
        merge_connection_bounds(conn_opts, statement_ms, socket_ms, with_socket: with_socket)
      when String
        append_connection_bounds(conn_opts, statement_ms, socket_ms, with_socket: with_socket)
      else
        conn_opts
      end
    end

    # Whether the linked libpq accepts the tcp_user_timeout / keepalives conninfo
    # keywords. Added in libpq 12; an older libpq raises "invalid connection
    # option" and fails the connection, so we must not emit them there.
    def libpq_supports_socket_bounds?
      defined?(PG) && PG.respond_to?(:library_version) && PG.library_version >= 120_000
    end

    # Hash form maps 1:1 to libpq keywords, so no escaping/encoding is needed.
    # The GUC stays nested in `options`; the socket keywords are top-level.
    # Preserve any caller-supplied `:options` (e.g. `-c search_path=…`) by
    # appending our `-c statement_timeout=…` rather than overwriting it.
    def merge_connection_bounds(conn_opts, statement_ms, socket_ms, with_socket:)
      options = [conn_opts[:options], "-c statement_timeout=#{statement_ms}"].compact.join(" ")
      merged = conn_opts.merge(options: options)
      return merged unless with_socket

      merged.merge(
        keepalives: 1,
        keepalives_idle: KEEPALIVE_IDLE_SECONDS,
        keepalives_interval: KEEPALIVE_INTERVAL_SECONDS,
        keepalives_count: KEEPALIVE_COUNT,
        tcp_user_timeout: socket_ms
      )
    end

    # libpq accepts two connection-string forms. URI form (postgres:// or
    # postgresql://) carries keywords as URL-encoded query params — the GUC in
    # `options` must percent-encode its space (%20) and `=` (%3D). key=value
    # conninfo form carries them space-separated, with the GUC single-quoted so
    # the outer parser keeps `-c statement_timeout=…` as one value.
    def append_connection_bounds(conn_opts, statement_ms, socket_ms, with_socket:)
      if conn_opts.start_with?("postgres://", "postgresql://")
        separator = conn_opts.include?("?") ? "&" : "?"
        socket = if with_socket
                   "keepalives=1&keepalives_idle=#{KEEPALIVE_IDLE_SECONDS}" \
                     "&keepalives_interval=#{KEEPALIVE_INTERVAL_SECONDS}" \
                     "&keepalives_count=#{KEEPALIVE_COUNT}&tcp_user_timeout=#{socket_ms}&"
                 else
                   ""
                 end
        "#{conn_opts}#{separator}#{socket}options=-c%20statement_timeout%3D#{statement_ms}"
      else
        socket = if with_socket
                   "keepalives=1 keepalives_idle=#{KEEPALIVE_IDLE_SECONDS} " \
                     "keepalives_interval=#{KEEPALIVE_INTERVAL_SECONDS} " \
                     "keepalives_count=#{KEEPALIVE_COUNT} tcp_user_timeout=#{socket_ms} "
                 else
                   ""
                 end
        "#{conn_opts} #{socket}options='-c statement_timeout=#{statement_ms}'"
      end
    end

    # Gate a read through the in-memory connection-health circuit breaker.
    # When the breaker is open the block never runs — Pgbus::ConnectionCircuitOpenError
    # is raised before any pool checkout, sparing a dead database from the whole
    # fleet re-polling and the error tracker from per-poll noise. A completed
    # read records success (closing/resetting the breaker); a
    # PGMQ::Errors::ConnectionError records a failure (and still propagates).
    # Writes are intentionally NOT gated — callers must see enqueue failures.
    def guarded_read(&)
      @connection_health.run_guarded(&)
    end

    def log_circuit_open(backoff)
      Pgbus.logger.warn do
        "[Pgbus::Client] Connection circuit opened after #{ConnectionHealth::OPEN_THRESHOLD}+ " \
          "consecutive connection failures — reads fail fast for ~#{backoff}s"
      end
    end

    def log_circuit_close
      Pgbus.logger.info { "[Pgbus::Client] Connection circuit closed — database reachable again" }
    end

    def with_stale_connection_retry
      attempts = 0
      begin
        yield
      rescue PGMQ::Errors::ConnectionError => e
        attempts += 1
        raise enrich_pool_timeout_error(e) unless attempts <= STALE_RETRY_ATTEMPTS && stale_connection_error?(e)

        # Sleep here — in the rescue, *outside* the yielded block — so the
        # backoff never runs while @pgmq_mutex is held: on the shared-connection
        # path the mutex lives inside `synchronized` within the yielded block,
        # and the raise unwinds out of it (releasing the mutex) before we get
        # here. See STALE_RETRY_DELAYS. Clamp the index to the last delay so a
        # future STALE_RETRY_ATTEMPTS > STALE_RETRY_DELAYS.size never sleeps nil.
        sleep STALE_RETRY_DELAYS[[attempts - 1, STALE_RETRY_DELAYS.size - 1].min]

        Pgbus.logger.warn do
          "[Pgbus::Client] Retrying after stale pgmq connection " \
            "(attempt #{attempts}/#{STALE_RETRY_ATTEMPTS}): #{e.message}"
        end
        retry
      end
    end

    def stale_connection_error?(error)
      message = error.message.to_s.downcase
      STALE_CONNECTION_PATTERNS.any? { |pattern| message.include?(pattern) }
    end

    # Substring pgmq-ruby uses when a pool checkout times out — a
    # ConnectionPool::TimeoutError re-raised as PGMQ::Errors::ConnectionError
    # "Connection pool timeout: ..." (see PGMQ::Connection#with_connection).
    # Deliberately NOT in STALE_CONNECTION_PATTERNS: a saturated pool must not
    # be retried (that just piles more waiters onto an already-exhausted pool).
    POOL_TIMEOUT_MARKER = "connection pool timeout"
    private_constant :POOL_TIMEOUT_MARKER

    def pool_timeout_error?(error)
      error.message.to_s.downcase.include?(POOL_TIMEOUT_MARKER)
    end

    # A bare "Connection pool timeout" tells an operator nothing actionable.
    # For that (and only that) error, return a same-class replacement whose
    # message carries the live pool state and a concrete next step, so the
    # first signal of saturation is diagnosable. The class is preserved so
    # callers rescuing PGMQ::Errors::ConnectionError behave identically. Any
    # other ConnectionError is returned untouched. Enrichment never raises:
    # pool_stats already rescues to {}, and a formatting failure falls back to
    # the original error.
    def enrich_pool_timeout_error(error)
      return error unless pool_timeout_error?(error)

      stats = pool_stats
      detail = stats.empty? ? "" : " (pool #{stats})"
      error.class.new(
        "#{error.message}#{detail} — " \
        "raise Pgbus.configuration.pool_size or reduce worker threads"
      )
    rescue StandardError
      error
    end

    def serialize(data)
      case data
      when String
        data
      else
        JSON.generate(data)
      end
    end

    # Single-pass serialization of payloads and optional headers.
    # Avoids two separate .map iterations over the same index range.
    def serialize_batch(payloads, headers)
      serialized = Array.new(payloads.size)
      serialized_headers = headers ? Array.new(headers.size) : nil

      payloads.each_with_index do |p, i|
        serialized[i] = serialize(p)
        if serialized_headers && i < headers.size
          h = headers[i]
          serialized_headers[i] = h.nil? ? nil : serialize(h)
        end
      end

      [serialized, serialized_headers]
    end
  end
end
