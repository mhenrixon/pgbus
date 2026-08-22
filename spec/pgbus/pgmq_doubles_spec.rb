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
      expect(client.target_queue("jobs")).to eq("pgbus_test_jobs")
      expect(client.target_queue("pgbus_test_jobs")).to eq("pgbus_test_jobs")
    end

    it "appends _pN from the configured priority when levels > 1" do
      Pgbus.configuration.priority_levels = 3
      Pgbus.configuration.default_priority = 1

      expect(client.target_queue("jobs", 0)).to eq("pgbus_test_jobs_p0")
      expect(client.target_queue("jobs")).to eq("pgbus_test_jobs_p1")
      expect(client.target_queue("pgbus_test_jobs_p2")).to eq("pgbus_test_jobs_p2")
    end
  end
end
