# frozen_string_literal: true

require "tempfile"

RSpec.describe Pgbus::ConfigLoader do
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
      Tmpfile&.close
      Tmpfile&.unlink
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
      expect {
        described_class.apply({ "nonexistent_setting" => "value" })
      }.not_to raise_error
    end
  end
end
