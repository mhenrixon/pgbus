# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  describe "defaults" do
    it "has default queue prefix" do
      expect(config.queue_prefix).to eq("pgbus")
    end

    it "has default queue name" do
      expect(config.default_queue).to eq("default")
    end

    it "has default pool size" do
      expect(config.pool_size).to eq(5)
    end

    it "has default visibility timeout" do
      expect(config.visibility_timeout).to eq(30)
    end

    it "has default max retries" do
      expect(config.max_retries).to eq(5)
    end

    it "has default polling interval" do
      expect(config.polling_interval).to eq(0.1)
    end

    it "enables listen/notify by default" do
      expect(config.listen_notify).to be true
    end

    it "has no worker recycling limits by default" do
      expect(config.max_jobs_per_worker).to be_nil
      expect(config.max_memory_mb).to be_nil
      expect(config.max_worker_lifetime).to be_nil
    end

    it "has no prefetch_limit by default" do
      expect(config.prefetch_limit).to be_nil
    end

    it "has circuit breaker enabled by default" do
      expect(config.circuit_breaker_enabled).to be true
      expect(config.circuit_breaker_threshold).to eq(5)
      expect(config.circuit_breaker_base_backoff).to eq(30)
      expect(config.circuit_breaker_max_backoff).to eq(600)
    end

    it "has no priority levels by default" do
      expect(config.priority_levels).to be_nil
      expect(config.default_priority).to eq(1)
    end

    it "has default archive retention of 7 days" do
      expect(config.archive_retention).to eq(7 * 24 * 3600)
      expect(config.archive_compaction_interval).to eq(3600)
      expect(config.archive_compaction_batch_size).to eq(1000)
    end

    it "has outbox disabled by default" do
      expect(config.outbox_enabled).to be false
      expect(config.outbox_poll_interval).to eq(1.0)
      expect(config.outbox_batch_size).to eq(100)
      expect(config.outbox_retention).to eq(24 * 3600)
    end

    it "has default recurring schedule interval" do
      expect(config.recurring_schedule_interval).to eq(1.0)
    end

    it "has no recurring tasks by default" do
      expect(config.recurring_tasks).to be_nil
    end

    it "does not skip recurring by default" do
      expect(config.skip_recurring).to be false
    end

    it "has default recurring execution retention of 7 days" do
      expect(config.recurring_execution_retention).to eq(7 * 24 * 3600)
    end
  end

  describe "#queue_name" do
    it "prefixes the queue name" do
      expect(config.queue_name("critical")).to eq("pgbus_critical")
    end
  end

  describe "#dead_letter_queue_name" do
    it "appends dlq suffix to prefixed name" do
      expect(config.dead_letter_queue_name("critical")).to eq("pgbus_critical_dlq")
    end
  end

  describe "#priority_queue_name" do
    it "returns the priority sub-queue name" do
      expect(config.priority_queue_name("critical", 0)).to eq("pgbus_critical_p0")
      expect(config.priority_queue_name("critical", 2)).to eq("pgbus_critical_p2")
    end
  end

  describe "#priority_queue_names" do
    it "returns single queue name when priority_levels is nil" do
      expect(config.priority_queue_names("default")).to eq(["pgbus_default"])
    end

    it "returns single queue name when priority_levels is 1" do
      config.priority_levels = 1
      expect(config.priority_queue_names("default")).to eq(["pgbus_default"])
    end

    it "returns sub-queue names when priority_levels > 1" do
      config.priority_levels = 3
      expect(config.priority_queue_names("default")).to eq(%w[pgbus_default_p0 pgbus_default_p1 pgbus_default_p2])
    end
  end

  describe "#validate!" do
    it "rejects invalid prefetch_limit" do
      config.prefetch_limit = 0
      expect { config.validate! }.to raise_error(ArgumentError, /prefetch_limit/)
    end

    it "accepts valid prefetch_limit" do
      config.prefetch_limit = 10
      expect { config.validate! }.not_to raise_error
    end

    it "rejects invalid priority_levels" do
      config.priority_levels = 0
      expect { config.validate! }.to raise_error(ArgumentError, /priority_levels/)
    end

    it "rejects priority_levels > 10" do
      config.priority_levels = 11
      expect { config.validate! }.to raise_error(ArgumentError, /priority_levels/)
    end

    it "accepts valid priority_levels" do
      config.priority_levels = 3
      expect { config.validate! }.not_to raise_error
    end
  end

  describe "#connection_options" do
    it "returns database_url when set" do
      config.database_url = "postgres://localhost/test"
      expect(config.connection_options).to eq("postgres://localhost/test")
    end

    it "returns connection_params when set" do
      params = { host: "localhost", dbname: "test" }
      config.connection_params = params
      expect(config.connection_options).to eq(params)
    end

    it "raises when no connection configured and no ActiveRecord" do
      hide_const("ActiveRecord::Base") if defined?(ActiveRecord::Base)
      expect { config.connection_options }.to raise_error(Pgbus::ConfigurationError)
    end

    context "with connects_to configured" do
      let(:raw_connection) { double("raw_connection") }
      let(:ar_connection) { double("ar_connection", raw_connection: raw_connection) }

      before do
        config.connects_to = { database: { writing: :pgbus } }
        stub_const("Pgbus::ApplicationRecord", Class.new)
        allow(Pgbus::ApplicationRecord).to receive(:connection).and_return(ar_connection)
      end

      it "returns a lambda that uses Pgbus::ApplicationRecord connection" do
        result = config.connection_options
        expect(result).to be_a(Proc)
        expect(result.call).to eq(raw_connection)
      end
    end

    context "without connects_to" do
      let(:raw_connection) { double("raw_connection") }
      let(:ar_connection) { double("ar_connection", raw_connection: raw_connection) }

      before do
        stub_const("ActiveRecord::Base", Class.new)
        allow(ActiveRecord::Base).to receive(:connection).and_return(ar_connection)
      end

      it "returns a lambda that uses ActiveRecord::Base connection" do
        result = config.connection_options
        expect(result).to be_a(Proc)
        expect(result.call).to eq(raw_connection)
      end
    end
  end
end
