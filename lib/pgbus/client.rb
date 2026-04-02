# frozen_string_literal: true

require "json"

module Pgbus
  class Client
    attr_reader :pgmq, :config

    PGMQ_REQUIRE_MUTEX = Mutex.new
    private_constant :PGMQ_REQUIRE_MUTEX

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
        # Use the configured pool_size and let pgmq-ruby's connection_pool handle
        # concurrency internally (no mutex needed).
        @pgmq = PGMQ::Client.new(conn_opts, pool_size: config.pool_size, pool_timeout: config.pool_timeout)
        @pgmq_mutex = nil
      end

      @queues_created = Concurrent::Map.new
      @queue_strategy = QueueFactory.for(config)
      @schema_ensured = false
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
        synchronized { @pgmq.create(dlq_name) }
        true
      end
    end

    def send_message(queue_name, payload, headers: nil, delay: 0, priority: nil)
      target = @queue_strategy.target_queue(queue_name, priority)
      ensure_queue(queue_name)
      Instrumentation.instrument("pgbus.client.send_message", queue: target) do
        synchronized { @pgmq.produce(target, serialize(payload), headers: headers && serialize(headers), delay: delay) }
      end
    end

    def send_batch(queue_name, payloads, headers: nil, delay: 0)
      full_name = config.queue_name(queue_name)
      ensure_queue(queue_name)
      serialized = payloads.map { |p| serialize(p) }
      serialized_headers = headers&.map { |h| serialize(h) }
      Instrumentation.instrument("pgbus.client.send_batch", queue: full_name, size: payloads.size) do
        synchronized { @pgmq.produce_batch(full_name, serialized, headers: serialized_headers, delay: delay) }
      end
    end

    def read_message(queue_name, vt: nil)
      full_name = config.queue_name(queue_name)
      Instrumentation.instrument("pgbus.client.read_message", queue: full_name) do
        synchronized { @pgmq.read(full_name, vt: vt || config.visibility_timeout) }
      end
    end

    def read_batch(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      Instrumentation.instrument("pgbus.client.read_batch", queue: full_name, qty: qty) do
        synchronized { @pgmq.read_batch(full_name, vt: vt || config.visibility_timeout, qty: qty) }
      end
    end

    # Read from priority sub-queues, highest priority (p0) first.
    # Returns [priority_queue_name, messages] pairs.
    def read_batch_prioritized(queue_name, qty:, vt: nil)
      unless @queue_strategy.priority?
        return (read_batch(queue_name, qty: qty, vt: vt) || []).map do |m|
          [config.queue_name(queue_name), m]
        end
      end

      remaining = qty
      results = []

      config.priority_queue_names(queue_name).each do |pq_name|
        break if remaining <= 0

        msgs = Instrumentation.instrument("pgbus.client.read_batch", queue: pq_name, qty: remaining) do
          synchronized { @pgmq.read_batch(pq_name, vt: vt || config.visibility_timeout, qty: remaining) }
        end || []

        msgs.each { |m| results << [pq_name, m] }
        remaining -= msgs.size
      end

      results
    end

    def read_with_poll(queue_name, qty:, vt: nil, max_poll_seconds: 5, poll_interval_ms: 100)
      full_name = config.queue_name(queue_name)
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

    # Read from multiple queues in a single SQL query (UNION ALL).
    # Each returned message includes a queue_name field identifying its source.
    # queue_names should be logical names (prefix is added automatically).
    def read_multi(queue_names, qty:, vt: nil)
      full_names = queue_names.map { |q| config.queue_name(q) }
      Instrumentation.instrument("pgbus.client.read_multi", queues: full_names, qty: qty) do
        synchronized { @pgmq.read_multi(full_names, vt: vt || config.visibility_timeout, qty: qty) }
      end
    end

    # Delete a message. Pass prefixed: false when queue_name is already
    # the full PGMQ queue name (e.g. from priority sub-queues or dashboard).
    def delete_message(queue_name, msg_id, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      synchronized { @pgmq.delete(name, msg_id) }
    end

    # Archive a message. Pass prefixed: false when queue_name is already
    # the full PGMQ queue name.
    def archive_message(queue_name, msg_id, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      synchronized { @pgmq.archive(name, msg_id) }
    end

    # Batch archive — moves multiple messages to the archive table in one call.
    def archive_batch(queue_name, msg_ids, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      synchronized { @pgmq.archive_batch(name, msg_ids) }
    end

    # Batch delete — permanently removes multiple messages in one call.
    def delete_batch(queue_name, msg_ids, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      synchronized { @pgmq.delete_batch(name, msg_ids) }
    end

    # Set visibility timeout. Pass prefixed: false when queue_name is already
    # the full PGMQ queue name.
    def set_visibility_timeout(queue_name, msg_id, vt:, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      synchronized { @pgmq.set_vt(name, msg_id, vt: vt) }
    end

    def transaction(&block)
      synchronized { @pgmq.transaction(&block) }
    end

    def move_to_dead_letter(queue_name, message)
      ensure_dead_letter_queue(queue_name)
      dlq_name = config.dead_letter_queue_name(queue_name)
      full_queue = config.queue_name(queue_name)

      synchronized do
        @pgmq.transaction do |txn|
          txn.produce(dlq_name, message.message, headers: message.headers)
          txn.delete(full_queue, message.msg_id.to_i)
        end
      end
    end

    def metrics(queue_name = nil)
      synchronized do
        if queue_name
          @pgmq.metrics(config.queue_name(queue_name))
        else
          @pgmq.metrics_all
        end
      end
    end

    def list_queues
      synchronized { @pgmq.list_queues }
    end

    def purge_queue(queue_name)
      synchronized { @pgmq.purge_queue(config.queue_name(queue_name)) }
    end

    def purge_archive(queue_name, older_than:, batch_size: 1000)
      full_name = config.queue_name(queue_name)
      sanitized = full_name.gsub(/[^a-zA-Z0-9_]/, "")
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

    # Topic routing
    def bind_topic(pattern, queue_name)
      full_name = config.queue_name(queue_name)
      ensure_queue(queue_name)
      synchronized { @pgmq.bind_topic(pattern, full_name) }
    end

    def publish_to_topic(routing_key, payload, headers: nil, delay: 0)
      synchronized do
        @pgmq.produce_topic(
          routing_key,
          serialize(payload),
          headers: headers && serialize(headers),
          delay: delay
        )
      end
    end

    def close
      synchronized { @pgmq.close }
    end

    private

    def collect_configured_queues
      queues = Set.new
      queues << config.default_queue

      # Queues from worker configs
      (config.workers || []).each do |w|
        worker_queues = w[:queues] || w["queues"] || [config.default_queue]
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
          @pgmq.enable_notify_insert(full_name, throttle_interval_ms: config.notify_throttle_ms) if config.listen_notify
        end
        true
      end
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

    def serialize(data)
      case data
      when String
        data
      else
        JSON.generate(data)
      end
    end
  end
end
