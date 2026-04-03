# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Application boot (integration)", :integration do
  describe "engine initialization" do
    it "configures connects_to without error" do
      expect(Pgbus::BusRecord.connection).to be_active
    end

    it "BusRecord uses the configured database" do
      db_config = Pgbus::BusRecord.connection_db_config
      expect(db_config).to be_present
    end

    it "BusRecord is abstract" do
      expect(Pgbus::BusRecord.abstract_class).to be true
    end

    it "all pgbus models inherit from BusRecord" do
      models = [
        Pgbus::ProcessEntry,
        Pgbus::JobLock,
        Pgbus::JobStat,
        Pgbus::Semaphore,
        Pgbus::RecurringTask,
        Pgbus::RecurringExecution,
        Pgbus::ProcessedEvent,
        Pgbus::QueueState,
        Pgbus::BlockedExecution,
        Pgbus::Batch
      ]

      models.each do |model|
        expect(model.superclass).to eq(Pgbus::BusRecord),
                                    "#{model.name} should inherit from Pgbus::BusRecord, got #{model.superclass.name}"
      end
    end
  end

  describe "PGMQ schema" do
    it "pgmq schema exists" do
      conn = Pgbus::BusRecord.connection
      result = conn.select_value("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'")
      expect(result).to eq(1)
    end

    it "pgmq.meta table exists" do
      conn = Pgbus::BusRecord.connection
      result = conn.select_value(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgmq' AND table_name = 'meta'"
      )
      expect(result).to eq(1)
    end
  end

  describe "queue bootstrapping" do
    it "creates queues from configuration" do
      client = Pgbus.client
      client.ensure_queue("boot_test")

      conn = Pgbus::BusRecord.connection
      exists = conn.select_value("SELECT 1 FROM pgmq.meta WHERE queue_name = 'pgbus_int_boot_test'")
      expect(exists).to eq(1)
    end

    it "creates DLQ alongside the main queue" do
      client = Pgbus.client
      client.ensure_queue("dlq_test")

      conn = Pgbus::BusRecord.connection
      exists = conn.select_value("SELECT 1 FROM pgmq.meta WHERE queue_name = 'pgbus_int_dlq_test_dlq'")
      expect(exists).to eq(1)
    end

    it "is idempotent — calling ensure_queue twice does not error" do
      client = Pgbus.client
      expect { client.ensure_queue("idempotent_test") }.not_to raise_error
      expect { client.ensure_queue("idempotent_test") }.not_to raise_error
    end
  end

  describe "configuration" do
    it "loads pgbus.yml when present" do
      config = Pgbus.configuration
      expect(config.queue_prefix).to be_present
      expect(config.default_queue).to be_present
    end

    it "Pgbus.logger is available" do
      expect(Pgbus.logger).to be_present
    end

    it "Pgbus.logger= works" do
      original = Pgbus.logger
      custom = Logger.new(IO::NULL)
      Pgbus.logger = custom
      expect(Pgbus.logger).to eq(custom)
    ensure
      Pgbus.logger = original
    end
  end

  describe "ActiveJob adapter" do
    it "adapter is registered" do
      adapter = ActiveJob::QueueAdapters.lookup(:pgbus)
      expect(adapter).to be_present
    end

    it "adapter responds to enqueue" do
      adapter = ActiveJob::QueueAdapters::PgbusAdapter.new
      expect(adapter).to respond_to(:enqueue)
      expect(adapter).to respond_to(:enqueue_at)
    end

    it "adapter supports stopping?" do
      adapter = ActiveJob::QueueAdapters::PgbusAdapter.new
      expect(adapter).to respond_to(:stopping?)
    end
  end

  describe "worker wildcard resolution" do
    it "resolves '*' to actual queue names from pgmq.meta" do
      client = Pgbus.client
      client.ensure_queue("wildcard_a")
      client.ensure_queue("wildcard_b")

      worker = Pgbus::Process::Worker.new(
        queues: ["*"],
        threads: 1,
        config: Pgbus.configuration
      )

      worker.send(:resolve_wildcard_queues)
      resolved = worker.instance_variable_get(:@queues)

      expect(resolved).to include("wildcard_a")
      expect(resolved).to include("wildcard_b")
      expect(resolved).not_to include("*")
    end

    it "excludes DLQ queues from wildcard resolution" do
      client = Pgbus.client
      client.ensure_queue("dlq_wildcard")

      worker = Pgbus::Process::Worker.new(
        queues: ["*"],
        threads: 1,
        config: Pgbus.configuration
      )

      worker.send(:resolve_wildcard_queues)
      resolved = worker.instance_variable_get(:@queues)

      expect(resolved).to include("dlq_wildcard")
      expect(resolved).not_to include("dlq_wildcard_dlq")
    end
  end

  describe "selective railtie loading compatibility" do
    it "BusRecord is accessible without require 'rails/all'" do
      # BusRecord lives in lib/pgbus/ — loadable by Zeitwerk regardless
      # of whether Rails was loaded via require "rails/all" or individual railties
      expect(defined?(Pgbus::BusRecord)).to eq("constant")
      expect(Pgbus::BusRecord.superclass).to eq(ActiveRecord::Base)
    end
  end
end
