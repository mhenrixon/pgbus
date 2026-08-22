# frozen_string_literal: true

require "spec_helper"

RSpec.describe PgmqDoubles do
  describe "#build_mock_client target_queue" do
    subject(:client) { build_mock_client }

    around do |example|
      levels = Pgbus.configuration.priority_levels
      default = Pgbus.configuration.default_priority
      example.run
    ensure
      Pgbus.configuration.priority_levels = levels
      Pgbus.configuration.default_priority = default
    end

    it "prefixes a logical name and leaves an already-physical name unchanged" do
      expect(client.target_queue("jobs")).to eq(Pgbus.configuration.queue_name("jobs"))
      prefixed = Pgbus.configuration.queue_name("jobs")
      expect(client.target_queue(prefixed)).to eq(prefixed)
    end

    it "appends _pN from the configured priority when levels > 1" do
      Pgbus.configuration.priority_levels = 3
      Pgbus.configuration.default_priority = 1

      expect(client.target_queue("jobs", 0)).to eq(Pgbus.configuration.priority_queue_name("jobs", 0))
      expect(client.target_queue("jobs")).to eq(Pgbus.configuration.priority_queue_name("jobs", 1))
      already = Pgbus.configuration.priority_queue_name("jobs", 2)
      expect(client.target_queue(already)).to eq(already)
    end

    it "still applies the priority suffix when the logical name ends in _pN" do
      Pgbus.configuration.priority_levels = 3
      Pgbus.configuration.default_priority = 1

      expect(client.target_queue("orders_p0", 0))
        .to eq(Pgbus.configuration.priority_queue_name("orders_p0", 0))
    end
  end
end
