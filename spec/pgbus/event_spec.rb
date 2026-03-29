# frozen_string_literal: true

RSpec.describe Pgbus::Event do
  subject(:event) do
    described_class.new(
      event_id: "abc-123",
      payload: { "order_id" => 42, "status" => "completed" },
      routing_key: "orders.completed"
    )
  end

  describe "#[]" do
    it "accesses payload keys" do
      expect(event["order_id"]).to eq(42)
    end

    it "returns nil for missing keys" do
      expect(event["nonexistent"]).to be_nil
    end
  end

  describe "#to_h" do
    it "returns a hash representation" do
      hash = event.to_h
      expect(hash["event_id"]).to eq("abc-123")
      expect(hash["payload"]["order_id"]).to eq(42)
      expect(hash["routing_key"]).to eq("orders.completed")
    end
  end

  describe "#published_at" do
    it "defaults to current time" do
      event = described_class.new(event_id: "x", payload: {})
      expect(event.published_at).to be_within(1).of(Time.now.utc)
    end
  end
end
