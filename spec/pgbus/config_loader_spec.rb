# frozen_string_literal: true

require "spec_helper"

require "tempfile"

RSpec.describe Pgbus::ConfigLoader do
  # Reset before AND after each example: the `after` alone leaves the first
  # example run under a random seed exposed to config state another spec file
  # left dirty (e.g. queue_prefix = "pgbus_test"). Examples here that assert the
  # pristine default (a sectioned file with no matching env applies nothing) then
  # fail on the leaked value. The `before` guarantees a clean default regardless
  # of run order.
  before { Pgbus.reset! }
  after { Pgbus.reset! }

  def with_yaml(content)
    file = Tempfile.new(["pgbus", ".yml"])
    file.write(content)
    file.rewind
    Pgbus.reset!
    yield file.path
  ensure
    file.close
    file.unlink
  end

  describe ".load" do
    let(:config_content) do
      <<~YAML
        test:
          queue_prefix: pgbus_test
          default_queue: test_default
          pool_size: 3
          max_retries: 10
          workers:
            - queues:
                - default
              threads: 2
      YAML
    end

    it "loads configuration from YAML file" do
      Tmpfile = Tempfile.new(["pgbus", ".yml"])
      Tmpfile.write(config_content)
      Tmpfile.rewind

      Pgbus.reset!
      described_class.load(Tmpfile.path, env: "test")

      expect(Pgbus.configuration.queue_prefix).to eq("pgbus_test")
      expect(Pgbus.configuration.default_queue).to eq("test_default")
      expect(Pgbus.configuration.pool_size).to eq(3)
      expect(Pgbus.configuration.max_retries).to eq(10)
    ensure
      Tmpfile.close
      Tmpfile.unlink
    end

    it "normalizes YAML workers to symbol keys" do
      yaml = <<~YAML
        test:
          workers:
            - queues:
                - critical
              threads: 2
              single_active_consumer: true
              group_mode: fifo
      YAML
      with_yaml(yaml) { |path| described_class.load(path, env: "test") }

      entry = Pgbus.configuration.workers.first
      expect(entry).to eq(
        queues: %w[critical],
        threads: 2,
        single_active_consumer: true,
        group_mode: "fifo"
      )
    end

    it "boots the loaded workers with correct queues, threads, and modes" do
      yaml = <<~YAML
        test:
          pool_size: null
          workers:
            - queues:
                - critical
              threads: 4
      YAML
      with_yaml(yaml) { |path| described_class.load(path, env: "test") }

      entry = Pgbus.configuration.workers.first
      expect(entry[:queues]).to eq(%w[critical])
      expect(entry[:threads]).to eq(4)
      expect(Pgbus.configuration.execution_mode_for(entry)).to eq(:threads)
      expect { Pgbus.configuration.validate! }.not_to raise_error
      expect(Pgbus.configuration.resolved_pool_size).to eq(6)
    end

    it "normalizes YAML event_consumers to symbol keys" do
      yaml = <<~YAML
        test:
          event_consumers:
            - topics:
                - orders.*
              threads: 4
      YAML
      with_yaml(yaml) { |path| described_class.load(path, env: "test") }

      entry = Pgbus.configuration.event_consumers.first
      expect(entry).to eq(topics: %w[orders.*], threads: 4)
    end

    it "raises ArgumentError naming the offending key for an invalid value" do
      yaml = <<~YAML
        test:
          visibility_timeout: 0
      YAML
      expect do
        with_yaml(yaml) { |path| described_class.load(path, env: "test") }
      end.to raise_error(ArgumentError, /visibility_timeout/)
    end

    it "raises for an invalid value in a flat (un-sectioned) file" do
      yaml = <<~YAML
        polling_interval: 0
      YAML
      expect do
        with_yaml(yaml) { |path| described_class.load(path, env: "development") }
      end.to raise_error(ArgumentError, /polling_interval/)
    end

    it "loads a valid file without raising" do
      yaml = <<~YAML
        test:
          visibility_timeout: 45
          polling_interval: 0.2
      YAML
      expect do
        with_yaml(yaml) { |path| described_class.load(path, env: "test") }
      end.not_to raise_error
      expect(Pgbus.configuration.visibility_timeout).to eq(45)
    end

    it "does not validate when eager_validation: false is set in the YAML" do
      yaml = <<~YAML
        test:
          eager_validation: false
          polling_interval: 0
      YAML
      expect do
        with_yaml(yaml) { |path| described_class.load(path, env: "test") }
      end.not_to raise_error
    end
  end

  describe ".load with env sections" do
    let(:sectioned_content) do
      <<~YAML
        development:
          queue_prefix: pgbus_dev
        production:
          queue_prefix: pgbus_prod
      YAML
    end

    # When the running env has no matching section, load must NOT silently
    # no-op: it warns naming the env and the available sections, and applies
    # nothing (the file genuinely has no settings for this env). This
    # replaced a prior "silently fall back to the whole file" behavior which
    # was itself a silent-failure surface — see issue #276.
    it "warns naming the env and available sections when no section matches" do
      io = StringIO.new
      Pgbus.configuration.logger = Logger.new(io)

      with_temp_config(sectioned_content) do |path|
        described_class.load(path, env: "staging")
      end

      expect(io.string).to include("staging")
      expect(io.string).to include("development")
      expect(io.string).to include("production")
    end

    it "applies nothing when no section matches the current env" do
      with_temp_config(sectioned_content) do |path|
        described_class.load(path, env: "staging")
      end

      expect(Pgbus.configuration.queue_prefix).to eq("pgbus")
    end

    it "does not warn about 'Unknown configuration key' for env section names" do
      io = StringIO.new
      Pgbus.configuration.logger = Logger.new(io)

      with_temp_config(sectioned_content) do |path|
        described_class.load(path, env: "staging")
      end

      expect(io.string).not_to include("Unknown configuration key")
    end

    it "applies the matched section normally when present" do
      with_temp_config(sectioned_content) do |path|
        described_class.load(path, env: "development")
      end

      expect(Pgbus.configuration.queue_prefix).to eq("pgbus_dev")
    end

    it "still warns about typos inside a matched env section" do
      io = StringIO.new
      Pgbus.configuration.logger = Logger.new(io)

      content = <<~YAML
        test:
          pooling_interval: 0.5
      YAML
      with_temp_config(content) do |path|
        described_class.load(path, env: "test")
      end

      expect(io.string).to include("Unknown configuration key")
      expect(io.string).to include("pooling_interval")
    end

    # A flat (un-sectioned) config file has top-level setter keys, not env
    # names, so parsed.key?(env) is false — but that is not the same as
    # "sectioned config missing this env." Detecting on Hash-valued top
    # levels distinguishes the two, so typos in flat configs still warn.
    it "warns about typos in a flat (un-sectioned) config file" do
      io = StringIO.new
      Pgbus.configuration.logger = Logger.new(io)

      content = <<~YAML
        queue_prefix: my_app
        pooling_interval: 0.5
      YAML
      with_temp_config(content) do |path|
        described_class.load(path, env: "development")
      end

      expect(io.string).to include("Unknown configuration key")
      expect(io.string).to include("pooling_interval")
    end

    # Regression for the misclassification bug: a legitimate flat pgbus.yml
    # whose every top-level setting happens to be Hash-valued (connection_params,
    # streams_retention, recurring_tasks, etc.) must still be detected as flat
    # and applied — not misclassified as "sectioned" with no matching env,
    # which previously silently applied nothing.
    it "treats an all-Hash-valued flat file as flat and applies its keys" do
      content = <<~YAML
        connection_params:
          host: db.internal
          dbname: myapp
        streams_retention:
          "orders.*": 2592000
        recurring_tasks:
          nightly_cleanup:
            queue: default
      YAML
      with_temp_config(content) do |path|
        described_class.load(path, env: "production")
      end

      expect(Pgbus.configuration.connection_params).to eq("host" => "db.internal", "dbname" => "myapp")
      expect(Pgbus.configuration.streams_retention).to eq("orders.*" => 2_592_000)
      expect(Pgbus.configuration.recurring_tasks).to eq(
        "nightly_cleanup" => { "queue" => "default" }
      )
    end

    it "does not warn for the all-Hash-valued flat file (correctly classified as flat)" do
      io = StringIO.new
      Pgbus.configuration.logger = Logger.new(io)

      content = <<~YAML
        connection_params:
          host: db.internal
          dbname: myapp
      YAML
      with_temp_config(content) do |path|
        described_class.load(path, env: "production")
      end

      expect(io.string).not_to include("Unknown configuration key")
    end

    def with_temp_config(content)
      file = Tempfile.new(["pgbus", ".yml"])
      file.write(content)
      file.rewind
      yield file.path
    ensure
      file.close
      file.unlink
    end
  end

  describe ".sectioned?" do
    it "returns false for a flat file with a real setter key even if all values are Hashes" do
      parsed = {
        "connection_params" => { "host" => "db.internal" },
        "streams_retention" => { "orders.*" => 100 }
      }
      expect(described_class.sectioned?(parsed)).to be(false)
    end

    it "returns true for a sectioned file whose top-level keys are env names, not setters" do
      parsed = {
        "development" => { "queue_prefix" => "dev" },
        "production" => { "queue_prefix" => "prod" }
      }
      expect(described_class.sectioned?(parsed)).to be(true)
    end

    it "returns false for an empty hash" do
      expect(described_class.sectioned?({})).to be(false)
    end

    it "returns false for a non-Hash" do
      expect(described_class.sectioned?(nil)).to be(false)
      expect(described_class.sectioned?([1, 2])).to be(false)
    end

    it "returns false when a mix of setter and non-setter Hash-valued keys are present" do
      # As soon as ONE top-level key is a real setter, the file is flat --
      # env section names are never real Configuration setters.
      parsed = {
        "connection_params" => { "host" => "db.internal" },
        "production" => { "queue_prefix" => "prod" }
      }
      expect(described_class.sectioned?(parsed)).to be(false)
    end
  end

  describe ".apply" do
    it "sets configuration values from a hash" do
      Pgbus.reset!
      described_class.apply({
                              "queue_prefix" => "custom",
                              "max_retries" => 7,
                              "polling_interval" => 0.5
                            })

      expect(Pgbus.configuration.queue_prefix).to eq("custom")
      expect(Pgbus.configuration.max_retries).to eq(7)
      expect(Pgbus.configuration.polling_interval).to eq(0.5)
    end

    it "ignores unknown keys" do
      expect do
        described_class.apply({ "nonexistent_setting" => "value" })
      end.not_to raise_error
    end

    it "logs a warning naming each unknown key" do
      io = StringIO.new
      Pgbus.configuration.logger = Logger.new(io)

      described_class.apply({ "pooling_interval" => 0.5, "queue_prefix" => "ok" })

      expect(io.string).to include("Unknown configuration key")
      expect(io.string).to include("pooling_interval")
    end

    it "does not warn for valid keys" do
      io = StringIO.new
      Pgbus.configuration.logger = Logger.new(io)

      described_class.apply({ "queue_prefix" => "custom", "max_retries" => 3 })

      expect(io.string).not_to include("Unknown configuration key")
    end

    it "validates after applying keys and raises on an invalid value" do
      Pgbus.reset!
      expect do
        described_class.apply({ "polling_interval" => 0 })
      end.to raise_error(ArgumentError, /polling_interval/)
    end

    it "does not validate when eager_validation is disabled" do
      Pgbus.reset!
      expect do
        described_class.apply({ "eager_validation" => false, "polling_interval" => 0 })
      end.not_to raise_error
    end

    it "still warns about unknown keys before validating" do
      io = StringIO.new
      Pgbus.configuration.logger = Logger.new(io)

      described_class.apply({ "nonexistent_setting" => "value", "queue_prefix" => "ok" })

      expect(io.string).to include("Unknown configuration key")
      expect(io.string).to include("nonexistent_setting")
    end
  end
end
