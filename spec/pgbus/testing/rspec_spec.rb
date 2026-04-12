# frozen_string_literal: true

require "spec_helper"
require "pgbus/testing/rspec"

RSpec.describe Pgbus::Testing do
  before do
    described_class.fake!
    described_class.store.clear!
    allow(Pgbus).to receive(:client).and_return(double("Pgbus::Client", publish_to_topic: nil))
  end

  after do
    described_class.disabled!
    described_class.store.clear!
  end

  describe "have_published_event" do
    it "matches when an event with the routing key is published" do
      expect do
        Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
      end.to have_published_event("orders.created")
    end

    it "fails when no event with the routing key is published" do
      expect do
        expect do
          Pgbus::EventBus::Publisher.publish("orders.shipped", { "id" => 1 })
        end.to have_published_event("orders.created")
      end.to raise_error(RSpec::Expectations::ExpectationNotMetError)
    end

    it "supports negation" do
      expect do
        Pgbus::EventBus::Publisher.publish("orders.shipped", { "id" => 1 })
      end.not_to have_published_event("orders.created")
    end

    describe ".with_payload" do
      it "matches payload with hash_including" do
        expect do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1, "total" => 99 })
        end.to have_published_event("orders.created").with_payload(hash_including("id" => 1))
      end

      it "matches exact payload" do
        expect do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
        end.to have_published_event("orders.created").with_payload("id" => 1)
      end

      it "fails when payload doesn't match" do
        expect do
          expect do
            Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
          end.to have_published_event("orders.created").with_payload("id" => 2)
        end.to raise_error(RSpec::Expectations::ExpectationNotMetError)
      end
    end

    describe ".with_headers" do
      it "matches headers" do
        expect do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 }, headers: { "x-trace" => "abc" })
        end.to have_published_event("orders.created").with_headers(hash_including("x-trace" => "abc"))
      end
    end

    it "only counts events published within the block" do
      Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 0 })

      expect do
        Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
      end.to have_published_event("orders.created").with_payload("id" => 1)
    end

    describe ".exactly" do
      it "matches exact count" do
        expect do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 2 })
        end.to have_published_event("orders.created").exactly(2)
      end

      it "fails when count doesn't match" do
        expect do
          expect do
            Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
          end.to have_published_event("orders.created").exactly(2)
        end.to raise_error(RSpec::Expectations::ExpectationNotMetError)
      end

      it "counts only events matching payload when chained with .with_payload" do
        expect do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 2 })
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
        end.to have_published_event("orders.created").with_payload("id" => 1).exactly(2)
      end
    end

    describe "negation with chained constraints" do
      it "passes when events exist but payload doesn't match" do
        expect do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
        end.not_to have_published_event("orders.created").with_payload("id" => 99)
      end

      it "fails when events match payload" do
        expect do
          expect do
            Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
          end.not_to have_published_event("orders.created").with_payload("id" => 1)
        end.to raise_error(RSpec::Expectations::ExpectationNotMetError)
      end

      it "passes when events exist but headers don't match" do
        expect do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 }, headers: { "x" => "a" })
        end.not_to have_published_event("orders.created").with_headers("x" => "z")
      end
    end
  end
end
