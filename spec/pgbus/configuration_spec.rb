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

    it "leaves pool_size unset by default (auto-tuned at read time)" do
      expect(config.pool_size).to be_nil
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

    it "has stats enabled with 30 day retention by default" do
      expect(config.stats_enabled).to be true
      expect(config.stats_retention).to eq(30 * 24 * 3600)
    end

    it "has insights_default_minutes of 30 days" do
      expect(config.insights_default_minutes).to eq(30 * 24 * 60)
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

  describe "#resolved_pool_size" do
    context "when pool_size is explicitly set" do
      it "returns the explicit value (overrides auto-tune)" do
        config.pool_size = 17
        expect(config.resolved_pool_size).to eq(17)
      end

      it "returns the explicit value even when it's smaller than the auto-tuned value" do
        config.pool_size = 1
        config.workers = [{ queues: %w[default], threads: 50 }]
        expect(config.resolved_pool_size).to eq(1)
      end
    end

    context "when pool_size is nil (auto-tune)" do
      it "returns total worker threads + 2 (one for dispatcher, one for scheduler)" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 5 }]
        expect(config.resolved_pool_size).to eq(7)
      end

      it "sums threads across multiple worker entries (capsules)" do
        config.pool_size = nil
        config.workers = [
          { queues: %w[critical], threads: 5 },
          { queues: %w[default mailers], threads: 10 }
        ]
        expect(config.resolved_pool_size).to eq(17)
      end

      it "accepts string keys (YAML form)" do
        config.pool_size = nil
        config.workers = [{ "queues" => %w[default], "threads" => 8 }]
        expect(config.resolved_pool_size).to eq(10)
      end

      it "uses 5 as the default per-worker thread count when threads is missing" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default] }]
        expect(config.resolved_pool_size).to eq(7)
      end

      it "includes event_consumers thread counts" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 5 }]
        config.event_consumers = [{ topics: %w[orders.#], threads: 3 }]
        expect(config.resolved_pool_size).to eq(10)
      end

      it "uses 3 as the default per-consumer thread count when threads is missing" do
        config.pool_size = nil
        config.workers = nil
        config.event_consumers = [{ topics: %w[orders.#] }]
        expect(config.resolved_pool_size).to eq(5)
      end

      it "treats nil workers as zero" do
        config.pool_size = nil
        config.workers = nil
        config.event_consumers = nil
        expect(config.resolved_pool_size).to eq(2)
      end

      it "treats empty workers as zero" do
        config.pool_size = nil
        config.workers = []
        config.event_consumers = []
        expect(config.resolved_pool_size).to eq(2)
      end

      it "warns when the auto-tuned pool exceeds the sanity threshold" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 60 }]
        warned_message = nil
        allow(Pgbus.logger).to receive(:warn) { |&block| warned_message = block.call }
        config.resolved_pool_size
        expect(warned_message).to match(/pool_size .* 62/)
      end

      it "does not warn for normal sizes" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 5 }]
        allow(Pgbus.logger).to receive(:warn)
        config.resolved_pool_size
        expect(Pgbus.logger).not_to have_received(:warn)
      end

      it "rejects non-integer thread counts (e.g. string)" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: "5" }]
        expect { config.resolved_pool_size }.to raise_error(
          ArgumentError,
          /worker.*threads.*positive integer/
        )
      end

      it "rejects float thread counts" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 0.5 }]
        expect { config.resolved_pool_size }.to raise_error(
          ArgumentError,
          /worker.*threads.*positive integer/
        )
      end

      it "rejects zero thread counts" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: 0 }]
        expect { config.resolved_pool_size }.to raise_error(
          ArgumentError,
          /worker.*threads.*positive integer/
        )
      end

      it "rejects negative thread counts" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: -1 }]
        expect { config.resolved_pool_size }.to raise_error(
          ArgumentError,
          /worker.*threads.*positive integer/
        )
      end

      it "rejects bad event_consumer thread counts with the right group label" do
        config.pool_size = nil
        config.workers = nil
        config.event_consumers = [{ topics: %w[orders.#], threads: "abc" }]
        expect { config.resolved_pool_size }.to raise_error(
          ArgumentError,
          /event_consumer.*threads.*positive integer/
        )
      end

      it "includes the offending value in the error message" do
        config.pool_size = nil
        config.workers = [{ queues: %w[default], threads: "abc" }]
        expect { config.resolved_pool_size }.to raise_error(ArgumentError, /"abc"/)
      end
    end
  end

  describe "#workers=" do
    context "when given an Array (legacy form)" do
      it "stores the array unchanged" do
        config.workers = [{ queues: %w[default], threads: 5 }]
        expect(config.workers).to eq([{ queues: %w[default], threads: 5 }])
      end

      it "preserves additional keys like single_active_consumer" do
        config.workers = [{ queues: %w[critical], threads: 3, single_active_consumer: true }]
        expect(config.workers.first[:single_active_consumer]).to be(true)
      end
    end

    context "when given a String (new DSL form)" do
      it "parses the string into the legacy array shape" do
        config.workers = "*: 5"
        expect(config.workers).to eq([{ queues: ["*"], threads: 5, name: "*" }])
      end

      it "auto-names each capsule by its first queue" do
        config.workers = "critical, default: 5; mailers: 2"
        names = config.workers.map { |c| c[:name] }
        expect(names).to eq(%w[critical mailers])
      end

      it "raises CapsuleDSL::ParseError for invalid strings" do
        expect { config.workers = "default: 0" }.to raise_error(
          Pgbus::Configuration::CapsuleDSL::ParseError,
          /positive integer/
        )
      end
    end

    context "when given anything else" do
      it "raises ArgumentError for nil" do
        expect { config.workers = nil }.not_to raise_error # nil is allowed (no workers)
      end

      it "raises ArgumentError for an Integer" do
        expect { config.workers = 5 }.to raise_error(ArgumentError, /String.*Array|Array.*String/)
      end

      it "raises ArgumentError for a Hash" do
        expect { config.workers = { queues: %w[default] } }.to raise_error(ArgumentError, /String.*Array|Array.*String/)
      end
    end
  end

  describe "#capsule" do
    it "appends a named capsule to workers" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      expect(config.workers).to eq([
                                     { name: :critical, queues: %w[critical], threads: 5 }
                                   ])
    end

    it "preserves additional keys (single_active_consumer, consumer_priority)" do
      config.workers = nil
      config.capsule(
        :gated,
        queues: %w[gated],
        threads: 1,
        single_active_consumer: true,
        consumer_priority: 10
      )
      capsule = config.workers.first
      expect(capsule[:single_active_consumer]).to be(true)
      expect(capsule[:consumer_priority]).to eq(10)
    end

    it "appends to an existing workers list set via the string DSL" do
      config.workers = "*: 5"
      config.capsule(:critical, queues: %w[critical], threads: 3)
      names = config.workers.map { |c| c[:name] }
      expect(names).to eq(["*", :critical])
    end

    it "appends to an existing workers list set via the legacy array form" do
      config.workers = [{ queues: %w[default], threads: 5 }]
      config.capsule(:reports, queues: %w[reports], threads: 2)
      expect(config.workers.size).to eq(2)
      expect(config.workers.last[:name]).to eq(:reports)
    end

    it "raises if the same name is registered twice" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      expect do
        config.capsule(:critical, queues: %w[urgent], threads: 3)
      end.to raise_error(ArgumentError, /:critical.*already defined/)
    end

    it "rejects nil queues" do
      expect do
        config.capsule(:bad, queues: nil, threads: 5)
      end.to raise_error(ArgumentError, /queues/)
    end

    it "rejects empty queues" do
      expect do
        config.capsule(:bad, queues: [], threads: 5)
      end.to raise_error(ArgumentError, /queues/)
    end

    it "rejects non-positive threads" do
      expect do
        config.capsule(:bad, queues: %w[a], threads: 0)
      end.to raise_error(ArgumentError, /threads/)
    end

    it "raises if a queue overlaps with an already-defined capsule" do
      config.workers = nil
      config.capsule(:a, queues: %w[shared], threads: 5)
      expect do
        config.capsule(:b, queues: %w[shared], threads: 5)
      end.to raise_error(ArgumentError, /shared.*already.*capsule/i)
    end
  end

  describe "#capsule_named" do
    before do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      config.capsule(:default, queues: %w[default], threads: 10)
    end

    it "returns the matching capsule by symbol name" do
      expect(config.capsule_named(:critical)).to include(name: :critical, threads: 5)
    end

    it "returns the matching capsule by string name" do
      expect(config.capsule_named("critical")).to include(name: :critical, threads: 5)
    end

    it "returns nil when no capsule matches" do
      expect(config.capsule_named(:missing)).to be_nil
    end

    it "returns nil when workers is nil" do
      config.workers = nil
      expect(config.capsule_named(:any)).to be_nil
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

    it "rejects non-positive insights_default_minutes" do
      config.insights_default_minutes = 0
      expect { config.validate! }.to raise_error(ArgumentError, /insights_default_minutes/)
    end

    it "rejects negative insights_default_minutes" do
      config.insights_default_minutes = -1
      expect { config.validate! }.to raise_error(ArgumentError, /insights_default_minutes/)
    end

    it "rejects fractional insights_default_minutes" do
      config.insights_default_minutes = 90.5
      expect { config.validate! }.to raise_error(ArgumentError, /insights_default_minutes/)
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
      let(:db_config) do
        double("db_config", configuration_hash: {
                 host: "pgbus-host", port: 5433, database: "pgbus_db",
                 username: "pgbus_user", password: "secret"
               })
      end

      before do
        config.connects_to = { database: { writing: :pgbus } }
        stub_const("Pgbus::BusRecord", Class.new)
        allow(Pgbus::BusRecord).to receive(:connection_db_config).and_return(db_config)
      end

      it "returns a connection hash extracted from BusRecord config" do
        result = config.connection_options
        expect(result).to be_a(Hash)
        expect(result[:host]).to eq("pgbus-host")
        expect(result[:dbname]).to eq("pgbus_db")
        expect(result[:user]).to eq("pgbus_user")
      end
    end

    context "without connects_to" do
      let(:db_config) do
        double("db_config", configuration_hash: {
                 host: "localhost", port: 5432, database: "myapp_dev",
                 username: "dev_user", password: nil
               })
      end

      before do
        allow(ActiveRecord::Base).to receive(:connection_db_config).and_return(db_config)
      end

      it "returns a connection hash extracted from ActiveRecord config" do
        result = config.connection_options
        expect(result).to be_a(Hash)
        expect(result[:host]).to eq("localhost")
        expect(result[:dbname]).to eq("myapp_dev")
        expect(result[:user]).to eq("dev_user")
      end
    end

    context "when AR config extraction fails" do
      before do
        allow(ActiveRecord::Base).to receive(:connection_db_config)
          .and_raise(StandardError, "no connection established")
      end

      it "falls back to Proc with a warning" do
        allow(Pgbus.logger).to receive(:warn)
        result = config.connection_options
        expect(result).to be_a(Proc)
        expect(Pgbus.logger).to have_received(:warn)
      end
    end
  end
end
