# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"
require "pgbus/generators/config_converter"

RSpec.describe Pgbus::Generators::ConfigConverter do
  describe ".from_hash" do
    subject(:output) { described_class.from_hash(input) }

    context "with a single-environment minimal config" do
      let(:input) do
        {
          "production" => {
            "queue_prefix" => "pgbus",
            "default_queue" => "default",
            "workers" => [{ "queues" => ["*"], "threads" => 5 }]
          }
        }
      end

      it "emits a Pgbus.configure block" do
        expect(output).to include("Pgbus.configure do |c|")
        expect(output).to include("end")
      end

      it "drops settings that match the gem default" do
        expect(output).not_to include("queue_prefix")
        expect(output).not_to include("default_queue")
      end

      it "emits the workers using the string DSL" do
        expect(output).to include('c.workers = "*: 5"')
      end
    end

    context "with multi-queue capsules in array form" do
      let(:input) do
        {
          "production" => {
            "workers" => [
              { "queues" => ["critical"], "threads" => 5 },
              { "queues" => %w[default mailers], "threads" => 10 }
            ]
          }
        }
      end

      it "emits the workers using the string DSL with capsule separators" do
        expect(output).to include('c.workers = "critical: 5; default, mailers: 10"')
      end
    end

    context "with capsules that have advanced options" do
      let(:input) do
        {
          "production" => {
            "workers" => [
              { "queues" => ["critical"], "threads" => 5, "single_active_consumer" => true }
            ]
          }
        }
      end

      it "falls back to c.capsule for capsules with extra options" do
        expect(output).to include("c.capsule")
        expect(output).to include("single_active_consumer: true")
      end

      it "does NOT emit the string DSL form when extras exist" do
        expect(output).not_to include("c.workers =")
      end
    end

    context "with duration-shaped values that differ from defaults" do
      let(:input) do
        {
          "production" => {
            # All non-default so they actually get emitted (default-matching
            # values are dropped to keep the initializer minimal — see
            # `drop?` for the logic).
            "visibility_timeout" => 600, # default 30
            "archive_retention" => 14 * 24 * 3600, # default 7 days
            "idempotency_ttl" => 30 * 24 * 3600,   # default 7 days
            "stats_retention" => 90 * 24 * 3600    # default 30 days
          }
        }
      end

      it "converts seconds to the largest evenly-dividing duration unit" do
        expect(output).to include("c.visibility_timeout = 10.minutes")
        expect(output).to include("c.archive_retention = 14.days")
        expect(output).to include("c.idempotency_ttl = 30.days")
        expect(output).to include("c.stats_retention = 90.days")
      end
    end

    context "with values that don't divide evenly into duration units" do
      let(:input) do
        {
          "production" => {
            "visibility_timeout" => 90 # 90 seconds — not 1.5 minutes (Float)
          }
        }
      end

      it "keeps the raw integer when no clean duration unit fits" do
        expect(output).to include("c.visibility_timeout = 90")
      end
    end

    context "with multi-environment overrides" do
      let(:input) do
        {
          "development" => {
            "workers" => [{ "queues" => ["*"], "threads" => 5 }],
            "max_jobs_per_worker" => nil,
            "max_memory_mb" => nil
          },
          "production" => {
            "workers" => [{ "queues" => ["*"], "threads" => 5 }],
            "max_jobs_per_worker" => 10_000,
            "max_memory_mb" => 512
          }
        }
      end

      it "emits constant settings unconditionally" do
        expect(output).to include('c.workers = "*: 5"')
      end

      it "emits env-specific settings with Rails.env guards" do
        # The 'unless development' shape is preferred when there are 2 envs
        # and one is dev — keeps the production line clean.
        expect(output).to match(/c\.max_jobs_per_worker = 10_000 unless Rails\.env\.development\?/)
        expect(output).to match(/c\.max_memory_mb = 512 unless Rails\.env\.development\?/)
      end
    end

    context "with a setting present in only one environment" do
      # Regression for issue #93. When a YAML uses `<<: *default`
      # anchors and a setting like `polling_interval: 0.01` is added
      # ONLY under `test:`, the other envs have no value for it at
      # all. The generator must not emit the setting as an
      # unconditional line — doing so silently applies a dev/test
      # tuning knob to production. (10ms polling = 10x the default
      # DB load under a 100ms default.)
      let(:input) do
        {
          "development" => {
            "workers" => [{ "queues" => ["*"], "threads" => 5 }]
          },
          "test" => {
            "workers" => [{ "queues" => ["*"], "threads" => 5 }],
            "polling_interval" => 0.01
          },
          "production" => {
            "workers" => [{ "queues" => ["*"], "threads" => 5 }]
          }
        }
      end

      it "does not emit the setting as an unconditional line" do
        # This is the exact regression from #93 — without the fix,
        # the generator emitted `c.polling_interval = 0.01` flat,
        # applying the 10ms test tuning to dev + test + prod.
        expect(output).not_to match(/^\s*c\.polling_interval = 0\.01\s*$/)
      end

      it "guards the setting with `if Rails.env.test?`" do
        expect(output).to include("c.polling_interval = 0.01 if Rails.env.test?")
      end

      it "still emits the constant workers line unconditionally" do
        expect(output).to include('c.workers = "*: 5"')
      end
    end

    context "with a setting present in a subset of environments" do
      # Related to #93: two of three envs set the value, one doesn't.
      # Whatever the formatter shape is (case block vs guarded line),
      # the value MUST NOT leak to the environment that didn't set it.
      let(:input) do
        {
          "development" => {
            "workers" => [{ "queues" => ["*"], "threads" => 5 }],
            "polling_interval" => 0.05
          },
          "test" => {
            "workers" => [{ "queues" => ["*"], "threads" => 5 }],
            "polling_interval" => 0.01
          },
          "production" => {
            "workers" => [{ "queues" => ["*"], "threads" => 5 }]
          }
        }
      end

      it "does not emit polling_interval as an unconditional line" do
        expect(output).not_to match(/^\s*c\.polling_interval = 0\.\d+\s*$/)
      end

      it "scopes the setting to only the environments that declared it" do
        # Must mention development and test (the envs that set it)
        # but NOT production. A case block with only dev+test clauses
        # is an acceptable shape — what we must prevent is the
        # production env silently inheriting the value.
        expect(output).to match(/polling_interval/)
        expect(output).to match(/development/)
        expect(output).to match(/\btest\b/)
        # Only `workers` should mention production (from a production:
        # capsule/worker config); polling_interval must not.
        polling_block = output[/c\.polling_interval[\s\S]*?(?:\n  end|$)/]
        expect(polling_block).not_to include("production")
      end
    end

    context "with three or more environments having different values" do
      let(:input) do
        {
          "development" => { "workers" => [{ "queues" => ["*"], "threads" => 2 }] },
          "staging" => { "workers" => [{ "queues" => ["*"], "threads" => 5 }] },
          "production" => { "workers" => [{ "queues" => ["*"], "threads" => 15 }] }
        }
      end

      it "emits a case Rails.env block" do
        expect(output).to include("case Rails.env")
        expect(output).to include('when "development"')
        expect(output).to include('when "staging"')
        expect(output).to include('when "production"')
      end
    end

    context "with deprecated settings (no longer supported)" do
      let(:input) do
        {
          "production" => {
            "pool_size" => 17,
            "workers" => [{ "queues" => ["*"], "threads" => 15 }]
          }
        }
      end

      it "drops pool_size (auto-tuned now)" do
        expect(output).not_to include("pool_size")
      end

      it "still emits the workers (pool_size is what was dropped)" do
        expect(output).to include('c.workers = "*: 15"')
      end
    end

    context "with culled settings that moved to constants" do
      # These settings used to be config knobs but were silent (nobody
      # tuned them) so they were moved to constants on their owning
      # classes. The converter drops them silently — users on legacy
      # YAML get a clean migration without manual cleanup.
      let(:input) do
        {
          "production" => {
            "notify_throttle_ms" => 500,
            "circuit_breaker_threshold" => 10,
            "circuit_breaker_base_backoff" => 60,
            "circuit_breaker_max_backoff" => 1200,
            "archive_compaction_interval" => 7200,
            "archive_compaction_batch_size" => 5000,
            "dead_letter_queue_suffix" => "_dead",
            "workers" => [{ "queues" => ["*"], "threads" => 5 }]
          }
        }
      end

      it "drops every culled setting from the generated initializer" do
        %w[notify_throttle_ms
           circuit_breaker_threshold circuit_breaker_base_backoff circuit_breaker_max_backoff
           archive_compaction_interval archive_compaction_batch_size
           dead_letter_queue_suffix].each do |key|
          expect(output).not_to include(key)
        end
      end
    end

    context "with a non-default pool_timeout" do
      let(:input) do
        {
          "production" => {
            "pool_timeout" => 10 # default is 5; emit the override
          }
        }
      end

      it "emits pool_timeout when it differs from the gem default" do
        # pool_timeout is a KNOWN_SETTING (still supported), not in
        # DEPRECATED_SETTINGS — only the value-matches-default rule
        # would drop it. With a non-default value it must round-trip.
        expect(output).to include("c.pool_timeout = 10")
      end
    end

    context "with pool_timeout matching the default" do
      let(:input) do
        {
          "production" => {
            "pool_timeout" => 5 # default
          }
        }
      end

      it "drops pool_timeout to keep the initializer minimal" do
        expect(output).not_to include("pool_timeout")
      end
    end

    context "with the install template's default config" do
      let(:input) do
        {
          "production" => {
            "queue_prefix" => "pgbus",
            "default_queue" => "default",
            "pool_timeout" => 5,
            "listen_notify" => true,
            "visibility_timeout" => 30,
            "max_retries" => 5,
            "workers" => [{ "queues" => ["default"], "threads" => 5 }]
          }
        }
      end

      it "emits an empty configure block (everything is default)" do
        # Only the boilerplate Pgbus.configure do |c| ... end remains.
        # The workers default is [{queues: ["default"], threads: 5}], which
        # the converter recognizes as the gem default.
        expect(output).to match(/Pgbus\.configure do \|c\|\s*end/)
      end
    end

    context "with frozen string literal magic comment" do
      let(:input) { { "production" => {} } }

      it "starts the file with frozen_string_literal" do
        expect(output).to start_with("# frozen_string_literal: true")
      end
    end

    context "with header comment" do
      let(:input) { { "production" => {} } }

      it "includes a generated-from comment" do
        expect(output).to include("Generated by `rails generate pgbus:update`")
      end

      it "tells the user to delete the YAML when ready" do
        expect(output).to include("delete config/pgbus.yml")
      end
    end
  end

  describe ".from_yaml" do
    let(:tmpdir) { Dir.mktmpdir }
    let(:yaml_path) { File.join(tmpdir, "pgbus.yml") }

    after { FileUtils.rm_rf(tmpdir) }

    it "reads a YAML file and returns the converted Ruby source" do
      File.write(yaml_path, <<~YAML)
        default: &default
          visibility_timeout: 600
          workers:
            - queues: ["*"]
              threads: 5

        development:
          <<: *default

        production:
          <<: *default
      YAML

      result = described_class.from_yaml(yaml_path)

      expect(result).to include("Pgbus.configure do |c|")
      expect(result).to include("c.visibility_timeout = 10.minutes")
      expect(result).to include('c.workers = "*: 5"')
    end

    it "raises a clear error when the file does not exist" do
      expect { described_class.from_yaml("/nonexistent.yml") }.to raise_error(
        Pgbus::Generators::ConfigConverter::Error, /not found/i
      )
    end
  end
end
