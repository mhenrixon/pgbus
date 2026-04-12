# frozen_string_literal: true

require "spec_helper"
require "pgbus/testing/rspec"

RSpec.describe "RSpec matchers" do
  before do
    Pgbus::Testing.fake!
    Pgbus::Testing.store.clear!
    allow(Pgbus).to receive(:client).and_return(double("Pgbus::Client", publish_to_topic: nil))
  end

  after do
    Pgbus::Testing.disabled!
    Pgbus::Testing.store.clear!
  end

  describe "have_published_event" do
    it "matches when an event with the routing key is published" do
      expect {
        Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
      }.to have_published_event("orders.created")
    end

    it "fails when no event with the routing key is published" do
      expect {
        expect {
          Pgbus::EventBus::Publisher.publish("orders.shipped", { "id" => 1 })
        }.to have_published_event("orders.created")
      }.to raise_error(RSpec::Expectations::ExpectationNotMetError)
    end

    it "supports negation" do
      expect {
        Pgbus::EventBus::Publisher.publish("orders.shipped", { "id" => 1 })
      }.not_to have_published_event("orders.created")
    end

    describe ".with_payload" do
      it "matches payload with hash_including" do
        expect {
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1, "total" => 99 })
        }.to have_published_event("orders.created").with_payload(hash_including("id" => 1))
      end

      it "matches exact payload" do
        expect {
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
        }.to have_published_event("orders.created").with_payload("id" => 1)
      end

      it "fails when payload doesn't match" do
        expect {
          expect {
            Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
          }.to have_published_event("orders.created").with_payload("id" => 2)
        }.to raise_error(RSpec::Expectations::ExpectationNotMetError)
      end
    end

    describe ".with_headers" do
      it "matches headers" do
        expect {
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 }, headers: { "x-trace" => "abc" })
        }.to have_published_event("orders.created").with_headers(hash_including("x-trace" => "abc"))
      end
    end

    it "only counts events published within the block" do
      Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 0 })

      expect {
        Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
      }.to have_published_event("orders.created").with_payload("id" => 1)
    end

    describe ".exactly" do
      it "matches exact count" do
        expect {
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 2 })
        }.to have_published_event("orders.created").exactly(2)
      end

      it "fails when count doesn't match" do
        expect {
          expect {
            Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
          }.to have_published_event("orders.created").exactly(2)
        }.to raise_error(RSpec::Expectations::ExpectationNotMetError)
      end
    end
  end
end
