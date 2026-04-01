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
      # Force pool_size=1. PG::Connection (libpq) is not thread-safe.
      # When using the Rails lambda path (-> { AR::Base.connection.raw_connection }),
      # the pool would return the same underlying PG::Connection that ActiveRecord
      # also uses, causing concurrent access corruption (segfaults, result.ntuples
      # NoMethodError). A single-connection pool combined with @pgmq_mutex ensures
      # all PGMQ operations are serialized.
      @pgmq = PGMQ::Client.new(
        config.connection_options,
        pool_size: 1,
        pool_timeout: config.pool_timeout
      )
      @pgmq_mutex = Mutex.new
      @queues_created = Concurrent::Map.new
      @queue_strategy = QueueFactory.for(config)
    end

    def ensure_queue(name)
      @queue_strategy.physical_queue_names(name).each { |pq| ensure_single_queue(pq) }
    rescue PGMQ::Errors::ConnectionError => e
      raise Pgbus::SchemaNotReady,
            "PGMQ schema is not available (#{e.message}). Run `rails db:migrate` for the pgbus database."
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

    def delete_message(queue_name, msg_id)
      full_name = config.queue_name(queue_name)
      synchronized { @pgmq.delete(full_name, msg_id) }
    end

    def archive_message(queue_name, msg_id)
      full_name = config.queue_name(queue_name)
      synchronized { @pgmq.archive(full_name, msg_id) }
    end

    def archive_from_queue(full_queue_name, msg_id)
      synchronized { @pgmq.archive(full_queue_name, msg_id) }
    end

    def extend_visibility(queue_name, msg_id, vt:)
      full_name = config.queue_name(queue_name)
      synchronized { @pgmq.set_vt(full_name, msg_id, vt: vt) }
    end

    def set_visibility_timeout(queue_name, msg_id, vt:)
      synchronized { @pgmq.set_vt(queue_name, msg_id, vt: vt) }
    end

    def delete_from_queue(queue_name, msg_id)
      synchronized { @pgmq.delete(queue_name, msg_id) }
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
          @pgmq.pool.with { |conn| conn.exec_params(sql, [older_than, batch_size]).cmd_tuples }
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

    # Serialize all PGMQ operations through a single mutex.
    # PG::Connection is not thread-safe — concurrent access from worker
    # threads causes segfaults and result corruption.
    def synchronized(&)
      @pgmq_mutex.synchronize(&)
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
