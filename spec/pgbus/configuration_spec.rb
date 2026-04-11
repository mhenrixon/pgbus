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
    end

    it "exposes circuit breaker tuning as constants on Pgbus::CircuitBreaker" do
      # The threshold/backoff values were silent settings nobody tuned and
      # were culled from configuration. They live on the CircuitBreaker
      # class as documented constants.
      expect(Pgbus::CircuitBreaker::THRESHOLD).to eq(5)
      expect(Pgbus::CircuitBreaker::BASE_BACKOFF).to eq(30)
      expect(Pgbus::CircuitBreaker::MAX_BACKOFF).to eq(600)
    end

    it "has no priority levels by default" do
      expect(config.priority_levels).to be_nil
      expect(config.default_priority).to eq(1)
    end

    it "has default archive retention of 7 days" do
      expect(config.archive_retention).to eq(7 * 24 * 3600)
    end

    it "exposes archive compaction tuning as constants on Pgbus::Process::Dispatcher" do
      # The compaction interval and batch size were silent settings; they
      # live on the dispatcher class as constants now.
      expect(Pgbus::Process::Dispatcher::ARCHIVE_COMPACTION_INTERVAL).to eq(3600)
      expect(Pgbus::Process::Dispatcher::ARCHIVE_COMPACTION_BATCH_SIZE).to eq(1000)
    end

    it "has stats enabled with 30 day retention by default" do
      expect(config.stats_enabled).to be true
      expect(config.stats_retention).to eq(30 * 24 * 3600)
    end

    it "has streams_stats_enabled disabled by default (opt-in)" do
      expect(config.streams_stats_enabled).to be false
    end

    it "has zombie_detection enabled by default" do
      expect(config.zombie_detection).to be true
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

    it "defaults execution_mode to :threads" do
      expect(config.execution_mode).to eq(:threads)
    end
  end

  describe "#execution_mode_for" do
    it "returns the global default when worker has no override" do
      expect(config.execution_mode_for({})).to eq(:threads)
    end

    it "returns the worker-level override when present" do
      expect(config.execution_mode_for(execution_mode: :async)).to eq(:async)
    end

    it "normalizes :fiber to :async" do
      expect(config.execution_mode_for(execution_mode: :fiber)).to eq(:async)
    end

    it "falls back to global execution_mode" do
      config.execution_mode = :async
      expect(config.execution_mode_for({})).to eq(:async)
    end

    it "accepts string keys" do
      expect(config.execution_mode_for("execution_mode" => "async")).to eq(:async)
    end
  end

  describe "#queue_name" do
    it "prefixes the queue name" do
      expect(config.queue_name("critical")).to eq("pgbus_critical")
    end

    it "normalizes hyphens to underscores" do
      expect(config.queue_name("hotwire-livereload")).to eq("pgbus_hotwire_livereload")
    end

    it "normalizes dots to underscores" do
      expect(config.queue_name("my.queue")).to eq("pgbus_my_queue")
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

      it "uses fewer connections for async workers (fibers share connections)" do
        config.pool_size = nil
        config.workers = [{ queues: %w[webhooks], threads: 100, execution_mode: :async }]
        # Async workers need ~3 connections (reactor + polling + headroom),
        # not 100 (one per fiber). Total: 3 + 1 dispatcher + 1 scheduler = 5
        expect(config.resolved_pool_size).to eq(5)
      end

      it "mixes async and thread workers correctly" do
        config.pool_size = nil
        config.workers = [
          { queues: %w[webhooks], threads: 50, execution_mode: :async },
          { queues: %w[default], threads: 5 }
        ]
        # 3 (async) + 5 (threads) + 1 dispatcher + 1 scheduler = 10
        expect(config.resolved_pool_size).to eq(10)
      end

      it "uses fewer connections for fiber mode (alias for async)" do
        config.pool_size = nil
        config.workers = [{ queues: %w[llm], threads: 200, execution_mode: :fiber }]
        # 3 + 1 + 1 = 5
        expect(config.resolved_pool_size).to eq(5)
      end

      it "honors global execution_mode when workers have no per-entry override" do
        config.pool_size = nil
        config.execution_mode = :async
        config.workers = [{ queues: %w[default], threads: 50 }]
        # Global async: 3 + 1 dispatcher + 1 scheduler = 5
        expect(config.resolved_pool_size).to eq(5)
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
      it "parses the wildcard form to an anonymous capsule (no :name)" do
        # Wildcards never get auto-named — see the long comment on
        # Configuration#workers= for the rationale. Anonymous capsules
        # let the user run multiple identical forks without colliding
        # with the named-capsule overlap check.
        config.workers = "*: 5"
        expect(config.workers).to eq([{ queues: ["*"], threads: 5 }])
        expect(config.workers.first).not_to have_key(:name)
      end

      it "auto-names each capsule whose first-queue is unique and not the wildcard" do
        config.workers = "critical, default: 5; mailers: 2"
        names = config.workers.map { |c| c[:name] }
        expect(names).to eq(%w[critical mailers])
      end

      it "leaves duplicate-first-queue capsules anonymous (the 'N forks' pattern)" do
        # Restores the legacy YAML pattern: 5 × {queues: ["*"], threads: 3}.
        # Each capsule becomes its own forked process at boot time.
        config.workers = "*: 3; *: 3; *: 3"
        expect(config.workers.size).to eq(3)
        expect(config.workers).to all(eq(queues: ["*"], threads: 3))
        config.workers.each { |c| expect(c).not_to have_key(:name) }
      end

      it "leaves duplicate non-wildcard first-queues anonymous too" do
        # Same logic — if naming would collide, neither side gets a name.
        config.workers = "default: 5; default: 3"
        expect(config.workers).to eq([
                                       { queues: ["default"], threads: 5 },
                                       { queues: ["default"], threads: 3 }
                                     ])
        config.workers.each { |c| expect(c).not_to have_key(:name) }
      end

      it "raises CapsuleDSL::ParseError for invalid strings" do
        expect { config.workers = "default: 0" }.to raise_error(
          Pgbus::Configuration::CapsuleDSL::ParseError,
          /positive integer/
        )
      end
    end

    context "when given nil" do
      it "allows nil (no workers configured — used by scheduler-only / dispatcher-only deployments)" do
        expect { config.workers = nil }.not_to raise_error
        expect(config.workers).to be_nil
      end
    end

    context "when given anything else" do
      it "raises ArgumentError for an Integer" do
        expect { config.workers = 5 }.to raise_error(ArgumentError, /String.*Array|Array.*String/)
      end

      it "raises ArgumentError for a Hash" do
        expect { config.workers = { queues: %w[default] } }.to raise_error(ArgumentError, /String.*Array|Array.*String/)
      end
    end
  end

  describe "#capsule" do
    it "appends a named capsule to workers (with name normalized to string)" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      expect(config.workers).to eq([
                                     { name: "critical", queues: %w[critical], threads: 5 }
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
      config.workers = "default: 5"
      config.capsule(:critical, queues: %w[critical], threads: 3)
      names = config.workers.map { |c| c[:name] }
      expect(names).to eq(%w[default critical])
    end

    it "appends to an existing workers list set via the legacy array form" do
      config.workers = [{ queues: %w[default], threads: 5 }]
      config.capsule(:reports, queues: %w[reports], threads: 2)
      expect(config.workers.size).to eq(2)
      expect(config.workers.last[:name]).to eq("reports")
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
      expect(config.capsule_named(:critical)).to include(name: "critical", threads: 5)
    end

    it "returns the matching capsule by string name" do
      expect(config.capsule_named("critical")).to include(name: "critical", threads: 5)
    end

    it "returns nil when no capsule matches" do
      expect(config.capsule_named(:missing)).to be_nil
    end

    it "returns nil when workers is nil" do
      config.workers = nil
      expect(config.capsule_named(:any)).to be_nil
    end
  end

  describe "capsule name normalization" do
    it "stores symbol-named capsules as strings internally" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      expect(config.workers.first[:name]).to eq("critical")
    end

    it "stores string-named capsules as strings internally" do
      config.workers = nil
      config.capsule("critical", queues: %w[critical], threads: 5)
      expect(config.workers.first[:name]).to eq("critical")
    end

    it "rejects symbol/string name collision (treats them as the same name)" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 5)
      expect do
        config.capsule("critical", queues: %w[urgent], threads: 3)
      end.to raise_error(ArgumentError, /already defined/)
    end

    it "string DSL stores auto-generated names as strings" do
      config.workers = "critical: 5"
      expect(config.workers.first[:name]).to eq("critical")
    end
  end

  describe "wildcard overlap detection" do
    # Anonymous capsules (parsed from "*: N" or duplicate-first-queue
    # strings) are intentionally invisible to overlap detection — they
    # represent "N forks of the same pool" rather than addressable units.
    # Only named capsules trigger the overlap rule.
    it "allows adding a named capsule when an existing anonymous '*' capsule exists" do
      config.workers = "*: 5" # anonymous (wildcard never gets auto-named)
      expect do
        config.capsule(:critical, queues: %w[critical], threads: 3)
      end.not_to raise_error
    end

    it "allows adding a wildcard capsule on top of anonymous wildcards" do
      config.workers = "*: 3; *: 3"
      expect do
        config.capsule(:catch_all, queues: ["*"], threads: 5)
      end.not_to raise_error
    end

    it "rejects adding a wildcard capsule when an existing NAMED capsule exists" do
      config.workers = nil
      config.capsule(:critical, queues: %w[critical], threads: 3)
      expect do
        config.capsule(:catch_all, queues: ["*"], threads: 5)
      end.to raise_error(ArgumentError, /already.*capsule|wildcard/i)
    end

    it "rejects adding a named capsule when an existing NAMED wildcard capsule exists" do
      config.workers = nil
      config.capsule(:catch_all, queues: ["*"], threads: 5)
      expect do
        config.capsule(:critical, queues: %w[critical], threads: 3)
      end.to raise_error(ArgumentError, /already.*capsule|wildcard/i)
    end
  end

  describe "anonymous N-forks pattern (legacy YAML compatibility)" do
    # The pattern that triggered the v0.5.1 regression fix: legacy YAML
    # could declare 5 × {queues: ["*"], threads: 3} to mean "5 forked
    # processes each running 3 threads, all reading every queue". The
    # PR-3 capsule DSL initially rejected this as a queue overlap. The
    # fix: anonymous capsules can overlap freely, named capsules cannot.
    it "produces N capsules from N-fork wildcard syntax" do
      config.workers = "*: 3; *: 3; *: 3; *: 3; *: 3"
      expect(config.workers.size).to eq(5)
      expect(config.workers.map { |c| c[:threads] }.sum).to eq(15)
    end

    it "still rejects two named capsules with the same explicit queue" do
      config.workers = nil
      config.capsule(:foo, queues: %w[shared], threads: 5)
      expect do
        config.capsule(:bar, queues: %w[shared], threads: 5)
      end.to raise_error(ArgumentError, /shared.*already.*capsule/i)
    end
  end

  describe "#role_enabled?" do
    context "when roles is nil (the default — boot everything)" do
      it "returns true for every role" do
        config.roles = nil
        %i[workers dispatcher scheduler consumers outbox].each do |role|
          expect(config.role_enabled?(role)).to be(true)
        end
      end
    end

    context "when roles is set to a subset" do
      it "returns true only for roles in the subset" do
        config.roles = %i[workers dispatcher]
        expect(config.role_enabled?(:workers)).to be(true)
        expect(config.role_enabled?(:dispatcher)).to be(true)
        expect(config.role_enabled?(:scheduler)).to be(false)
        expect(config.role_enabled?(:consumers)).to be(false)
      end

      it "accepts string role names" do
        config.roles = %i[workers]
        expect(config.role_enabled?("workers")).to be(true)
        expect(config.role_enabled?("scheduler")).to be(false)
      end
    end

    context "when roles is an empty array" do
      it "returns false for every role (effectively disables the supervisor)" do
        config.roles = []
        %i[workers dispatcher scheduler consumers outbox].each do |role|
          expect(config.role_enabled?(role)).to be(false)
        end
      end
    end
  end

  describe "#roles=" do
    it "stores nil unchanged" do
      config.roles = nil
      expect(config.roles).to be_nil
    end

    it "normalizes string roles to symbols" do
      config.roles = %w[workers dispatcher]
      expect(config.roles).to eq(%i[workers dispatcher])
    end

    it "lowercases mixed-case role names" do
      config.roles = %w[WORKERS Dispatcher]
      expect(config.roles).to eq(%i[workers dispatcher])
    end

    it "wraps a single non-array value into an array" do
      config.roles = :workers
      expect(config.roles).to eq([:workers])
    end

    it "deduplicates" do
      config.roles = %i[workers workers dispatcher]
      expect(config.roles).to eq(%i[workers dispatcher])
    end

    it "raises ArgumentError for an unknown role (typo protection)" do
      expect { config.roles = [:workres] }.to raise_error(ArgumentError, /invalid role.*workres/i)
    end

    it "lists valid roles in the error message" do
      expect { config.roles = [:bogus] }.to raise_error(ArgumentError, /workers.*dispatcher.*scheduler/i)
    end

    it "does not raise for any of the supported roles" do
      %i[workers dispatcher scheduler consumers outbox].each do |role|
        expect { config.roles = [role] }.not_to raise_error
      end
    end
  end

  describe "#resolved_pool_size with role filtering" do
    context "when roles is nil (default — boot everything)" do
      it "includes worker + event_consumer + dispatcher + scheduler thread counts" do
        config.pool_size = nil
        config.roles = nil
        config.workers = [{ queues: %w[default], threads: 5 }]
        config.event_consumers = [{ topics: %w[orders.#], threads: 3 }]
        # 5 workers + 3 consumers + 1 dispatcher + 1 scheduler = 10
        expect(config.resolved_pool_size).to eq(10)
      end
    end

    context "when running --workers-only" do
      it "excludes dispatcher and scheduler overhead from the pool size" do
        config.pool_size = nil
        config.roles = [:workers]
        config.workers = [{ queues: %w[default], threads: 5 }]
        config.event_consumers = [{ topics: %w[orders.#], threads: 3 }]
        # only workers: 5 threads, no overhead, no consumers
        expect(config.resolved_pool_size).to eq(5)
      end
    end

    context "when running --scheduler-only" do
      it "needs only the scheduler's connection slot" do
        config.pool_size = nil
        config.roles = [:scheduler]
        config.workers = [{ queues: %w[default], threads: 50 }]
        # workers are configured but not booted by this process
        expect(config.resolved_pool_size).to eq(1)
      end
    end

    context "when running --dispatcher-only" do
      it "needs only the dispatcher's connection slot" do
        config.pool_size = nil
        config.roles = [:dispatcher]
        config.workers = [{ queues: %w[default], threads: 50 }]
        expect(config.resolved_pool_size).to eq(1)
      end
    end

    context "when explicitly set" do
      it "ignores roles and returns the explicit value" do
        config.pool_size = 3
        config.roles = [:scheduler]
        config.workers = [{ queues: %w[default], threads: 5 }]
        expect(config.resolved_pool_size).to eq(3)
      end
    end
  end

  describe "duration coercion on assignment" do
    let(:duration_settings) do
      %i[
        visibility_timeout
        archive_retention
        idempotency_ttl
        outbox_retention
        stats_retention
        recurring_execution_retention
      ]
    end

    it "accepts a Numeric (interpreted as seconds, existing behavior)" do
      duration_settings.each do |setting|
        config.public_send("#{setting}=", 60)
        expect(config.public_send(setting)).to eq(60)
      end
    end

    it "accepts an ActiveSupport::Duration and stores as integer seconds" do
      config.visibility_timeout = 10.minutes
      expect(config.visibility_timeout).to eq(600)

      config.archive_retention = 7.days
      expect(config.archive_retention).to eq(7 * 24 * 3600)

      config.idempotency_ttl = 7.days
      expect(config.idempotency_ttl).to eq(7 * 24 * 3600)

      config.outbox_retention = 1.day
      expect(config.outbox_retention).to eq(24 * 3600)

      config.stats_retention = 30.days
      expect(config.stats_retention).to eq(30 * 24 * 3600)

      config.recurring_execution_retention = 7.days
      expect(config.recurring_execution_retention).to eq(7 * 24 * 3600)
    end

    it "raises ArgumentError immediately when assigned a negative number" do
      duration_settings.each do |setting|
        expect { config.public_send("#{setting}=", -1) }.to raise_error(
          ArgumentError, /#{setting}.*positive/
        )
      end
    end

    it "raises ArgumentError immediately when assigned zero" do
      duration_settings.each do |setting|
        expect { config.public_send("#{setting}=", 0) }.to raise_error(
          ArgumentError, /#{setting}.*positive/
        )
      end
    end

    it "raises ArgumentError immediately when assigned a non-numeric value" do
      duration_settings.each do |setting|
        expect { config.public_send("#{setting}=", "five seconds") }.to raise_error(
          ArgumentError, /#{setting}.*Numeric.*Duration/
        )
      end
    end

    it "accepts nil as a valid sentinel for 'feature disabled'" do
      # archive_retention, idempotency_ttl, recurring_execution_retention all
      # use nil to skip the corresponding maintenance task in the dispatcher.
      duration_settings.each do |setting|
        expect { config.public_send("#{setting}=", nil) }.not_to raise_error
        expect(config.public_send(setting)).to be_nil
      end
    end

    it "stores Duration values as a plain Integer (downstream code reads seconds)" do
      config.visibility_timeout = 30.seconds
      # ActiveSupport::Duration overrides BOTH is_a? AND instance_of? to return
      # true for Integer, so we have to check the actual class identity to
      # confirm the Duration was coerced rather than stored as-is.
      expect(config.visibility_timeout.class).to eq(Integer)
      expect(config.visibility_timeout).to eq(30)
    end

    it "preserves Float values for sub-second settings" do
      # Numerics that happen to be float should pass through unchanged
      config.visibility_timeout = 0.5
      expect(config.visibility_timeout).to eq(0.5)
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

    it "accepts nil workers (workerless modes like scheduler-only or dispatcher-only)" do
      config.workers = nil
      expect { config.validate! }.not_to raise_error
    end

    it "rejects fractional insights_default_minutes" do
      config.insights_default_minutes = 90.5
      expect { config.validate! }.to raise_error(ArgumentError, /insights_default_minutes/)
    end

    it "rejects negative retry_backoff" do
      config.retry_backoff = -1
      expect { config.validate! }.to raise_error(ArgumentError, /retry_backoff must be > 0/)
    end

    it "rejects nil retry_backoff_max" do
      config.retry_backoff_max = nil
      expect { config.validate! }.to raise_error(ArgumentError, /retry_backoff_max must be > 0/)
    end

    it "rejects jitter > 1" do
      config.retry_backoff_jitter = 1.5
      expect { config.validate! }.to raise_error(ArgumentError, /retry_backoff_jitter must be between 0 and 1/)
    end

    it "accepts valid backoff settings" do
      config.retry_backoff = 10
      config.retry_backoff_max = 600
      config.retry_backoff_jitter = 0.2
      expect { config.validate! }.not_to raise_error
    end

    it "rejects invalid global execution_mode" do
      config.execution_mode = :bogus
      expect { config.validate! }.to raise_error(ArgumentError, /execution_mode/i)
    end

    it "accepts valid execution_mode values" do
      %i[threads async fiber].each do |mode|
        config.execution_mode = mode
        expect { config.validate! }.not_to raise_error
      end
    end

    it "rejects invalid per-worker execution_mode" do
      config.workers = [{ queues: %w[default], threads: 5, execution_mode: :bogus }]
      expect { config.validate! }.to raise_error(ArgumentError, /execution_mode/i)
    end

    it "accepts per-worker execution_mode override" do
      config.workers = [{ queues: %w[default], threads: 50, execution_mode: :async }]
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

  describe "streams settings" do
    it "is enabled by default" do
      expect(config.streams_enabled).to be true
    end

    it "has a default queue prefix for streams" do
      expect(config.streams_queue_prefix).to eq("pgbus_stream")
    end

    it "has no signed name secret by default (falls back to Turbo's key)" do
      expect(config.streams_signed_name_secret).to be_nil
    end

    it "has a 5 minute default retention" do
      expect(config.streams_default_retention).to eq(5 * 60)
    end

    it "has an empty per-stream retention map by default" do
      expect(config.streams_retention).to eq({})
    end

    it "has a 15 second heartbeat interval" do
      expect(config.streams_heartbeat_interval).to eq(15)
    end

    it "caps connections per worker at 2000" do
      expect(config.streams_max_connections).to eq(2_000)
    end

    it "has a 1 hour idle timeout" do
      expect(config.streams_idle_timeout).to eq(3_600)
    end

    it "has a 250ms LISTEN health check interval" do
      expect(config.streams_listen_health_check_ms).to eq(250)
    end

    it "has a 5 second write deadline" do
      expect(config.streams_write_deadline_ms).to eq(5_000)
    end

    it "does not opt into the Falcon streaming body code path by default" do
      expect(config.streams_falcon_streaming_body).to be false
    end
  end

  describe "#validate! with streams settings" do
    it "rejects negative streams_default_retention" do
      config.streams_default_retention = -1
      expect { config.validate! }.to raise_error(ArgumentError, /streams_default_retention/)
    end

    it "rejects non-positive streams_max_connections" do
      config.streams_max_connections = 0
      expect { config.validate! }.to raise_error(ArgumentError, /streams_max_connections/)
    end

    it "rejects non-positive streams_heartbeat_interval" do
      config.streams_heartbeat_interval = 0
      expect { config.validate! }.to raise_error(ArgumentError, /streams_heartbeat_interval/)
    end

    it "rejects non-Hash streams_retention" do
      config.streams_retention = "nope"
      expect { config.validate! }.to raise_error(ArgumentError, /streams_retention/)
    end

    it "rejects non-positive streams_idle_timeout" do
      config.streams_idle_timeout = 0
      expect { config.validate! }.to raise_error(ArgumentError, /streams_idle_timeout/)
    end

    it "rejects non-positive streams_listen_health_check_ms" do
      config.streams_listen_health_check_ms = 0
      expect { config.validate! }.to raise_error(ArgumentError, /streams_listen_health_check_ms/)
    end

    it "rejects non-positive streams_write_deadline_ms" do
      config.streams_write_deadline_ms = 0
      expect { config.validate! }.to raise_error(ArgumentError, /streams_write_deadline_ms/)
    end

    it "accepts a valid streams config" do
      expect { config.validate! }.not_to raise_error
    end
  end
end
