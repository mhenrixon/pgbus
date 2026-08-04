# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Claimable age metrics (integration)", :integration do
  let(:client) { Pgbus.client }

  before do
    client.ensure_queue("claimable_test")
    client.purge_queue("claimable_test")
  end

  describe "#oldest_claimable_ages" do
    it "reports nil for a queue holding only a delayed message, while the raw age counts it" do
      client.send_message("claimable_test", { "delayed" => true }, delay: 3600)

      expect(client.oldest_claimable_ages("claimable_test")).to be_nil

      metrics = client.metrics("claimable_test")
      expect(metrics.queue_length.to_i).to eq(1)
      expect(metrics.oldest_msg_age_sec.to_i).to be >= 0
    end

    it "reports the age once a message is eligible for pickup" do
      client.send_message("claimable_test", { "now" => true })

      age = client.oldest_claimable_ages("claimable_test")
      expect(age).to be_an(Integer)
      expect(age).to be >= 0

      # The age is wall-clock-relative and only grows between the two calls;
      # an exact match would flake when connecting to a contended server.
      all = client.oldest_claimable_ages
      expect(all.fetch(client.config.queue_name("claimable_test"))).to be >= age
    end

    it "excludes an in-flight message whose visibility timeout was pushed forward" do
      client.send_message("claimable_test", { "in_flight" => true })
      client.read_batch("claimable_test", qty: 1, vt: 60)

      expect(client.oldest_claimable_ages("claimable_test")).to be_nil
    end
  end
end
