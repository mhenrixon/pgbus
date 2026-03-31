# frozen_string_literal: true

require "json"

module Pgbus
  class Client
    attr_reader :pgmq, :config

    def initialize(config = Pgbus.configuration)
      # Define the PGMQ module before requiring the gem so that Zeitwerk's
      # eager_load (called inside pgmq.rb) can resolve the constant.
      # Without this, Ruby 4.0 + Zeitwerk 2.7.5 raises NameError because
      # eager_load runs const_get(:Client) on PGMQ before the module is defined.
      Object.const_set(:PGMQ, Module.new) unless defined?(::PGMQ)
      require "pgmq"
      @config = config
      @pgmq = PGMQ::Client.new(
        config.connection_options,
        pool_size: config.pool_size,
        pool_timeout: config.pool_timeout
      )
      @queues_created = {}
      @mutex = Mutex.new
    end

    def ensure_queue(name)
      full_name = config.queue_name(name)
      @mutex.synchronize do
        return if @queues_created[full_name]

        @pgmq.create(full_name)
        @pgmq.enable_notify_insert(full_name, throttle_interval_ms: config.notify_throttle_ms) if config.listen_notify
        @queues_created[full_name] = true
      end
    end

    def ensure_dead_letter_queue(name)
      dlq_name = config.dead_letter_queue_name(name)
      @mutex.synchronize do
        return if @queues_created[dlq_name]

        @pgmq.create(dlq_name)
        @queues_created[dlq_name] = true
      end
    end

    def send_message(queue_name, payload, headers: nil, delay: 0)
      full_name = config.queue_name(queue_name)
      ensure_queue(queue_name)
      Instrumentation.instrument("pgbus.client.send_message", queue: full_name) do
        @pgmq.produce(full_name, serialize(payload), headers: headers && serialize(headers), delay: delay)
      end
    end

    def send_batch(queue_name, payloads, headers: nil, delay: 0)
      full_name = config.queue_name(queue_name)
      ensure_queue(queue_name)
      Instrumentation.instrument("pgbus.client.send_batch", queue: full_name, size: payloads.size) do
        serialized = payloads.map { |p| serialize(p) }
        serialized_headers = headers&.map { |h| serialize(h) }
        @pgmq.produce_batch(full_name, serialized, headers: serialized_headers, delay: delay)
      end
    end

    def read_message(queue_name, vt: nil)
      full_name = config.queue_name(queue_name)
      Instrumentation.instrument("pgbus.client.read_message", queue: full_name) do
        @pgmq.read(full_name, vt: vt || config.visibility_timeout)
      end
    end

    def read_batch(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      Instrumentation.instrument("pgbus.client.read_batch", queue: full_name, qty: qty) do
        @pgmq.read_batch(full_name, vt: vt || config.visibility_timeout, qty: qty)
      end
    end

    def read_with_poll(queue_name, qty:, vt: nil, max_poll_seconds: 5, poll_interval_ms: 100)
      full_name = config.queue_name(queue_name)
      @pgmq.read_with_poll(
        full_name,
        vt: vt || config.visibility_timeout,
        qty: qty,
        max_poll_seconds: max_poll_seconds,
        poll_interval_ms: poll_interval_ms
      )
    end

    def delete_message(queue_name, msg_id)
      full_name = config.queue_name(queue_name)
      @pgmq.delete(full_name, msg_id)
    end

    def archive_message(queue_name, msg_id)
      full_name = config.queue_name(queue_name)
      @pgmq.archive(full_name, msg_id)
    end

    def extend_visibility(queue_name, msg_id, vt:)
      full_name = config.queue_name(queue_name)
      @pgmq.set_vt(full_name, msg_id, vt: vt)
    end

    def set_visibility_timeout(queue_name, msg_id, vt:)
      @pgmq.set_vt(queue_name, msg_id, vt: vt)
    end

    def delete_from_queue(queue_name, msg_id)
      @pgmq.delete(queue_name, msg_id)
    end

    def transaction(&)
      @pgmq.transaction(&)
    end

    def move_to_dead_letter(queue_name, message)
      ensure_dead_letter_queue(queue_name)
      dlq_name = config.dead_letter_queue_name(queue_name)
      full_queue = config.queue_name(queue_name)

      @pgmq.transaction do |txn|
        txn.produce(dlq_name, message.message, headers: message.headers)
        txn.delete(full_queue, message.msg_id.to_i)
      end
    end

    def metrics(queue_name = nil)
      if queue_name
        @pgmq.metrics(config.queue_name(queue_name))
      else
        @pgmq.metrics_all
      end
    end

    def list_queues
      @pgmq.list_queues
    end

    def purge_queue(queue_name)
      @pgmq.purge_queue(config.queue_name(queue_name))
    end

    # Topic routing
    def bind_topic(pattern, queue_name)
      full_name = config.queue_name(queue_name)
      ensure_queue(queue_name)
      @pgmq.bind_topic(pattern, full_name)
    end

    def publish_to_topic(routing_key, payload, headers: nil, delay: 0)
      @pgmq.produce_topic(
        routing_key,
        serialize(payload),
        headers: headers && serialize(headers),
        delay: delay
      )
    end

    def close
      @pgmq.close
    end

    private

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
