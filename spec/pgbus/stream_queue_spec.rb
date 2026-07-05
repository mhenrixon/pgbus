# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::StreamQueue do
  before { described_class.reset_cache! }

  describe ".record!" do
    before do
      allow(described_class).to receive_messages(upsert: nil, table_exists?: true)
    end

    it "upserts the physical queue name (idempotent)" do
      described_class.record!("pgbus_chat_42")

      expect(described_class).to have_received(:upsert).with(
        { queue_name: "pgbus_chat_42" },
        unique_by: :queue_name
      )
    end

    it "skips when the table does not exist (unmigrated install)" do
      allow(described_class).to receive(:table_exists?).and_return(false)

      described_class.record!("pgbus_chat_42")

      expect(described_class).not_to have_received(:upsert)
    end

    it "swallows errors so a registry blip cannot kill a broadcast" do
      allow(described_class).to receive(:upsert).and_raise(StandardError, "db down")

      expect { described_class.record!("pgbus_chat_42") }.not_to raise_error
    end

    it "adds the name to the in-process cache without a re-query" do
      described_class.record!("pgbus_chat_42")

      expect(described_class.stream?("pgbus_chat_42")).to be(true)
    end
  end

  describe ".all_names" do
    it "returns the set of registered physical queue names" do
      allow(described_class).to receive_messages(table_exists?: true)
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_chat_42 pgbus_room_1])
      allow(described_class).to receive(:all).and_return(relation)

      expect(described_class.all_names).to eq(Set.new(%w[pgbus_chat_42 pgbus_room_1]))
    end

    it "returns an empty set when the table does not exist" do
      allow(described_class).to receive(:table_exists?).and_return(false)

      expect(described_class.all_names).to eq(Set.new)
    end

    it "memoizes so repeated dispatcher loops do not re-query" do
      allow(described_class).to receive_messages(table_exists?: true)
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_chat_42])
      allow(described_class).to receive(:all).and_return(relation)

      described_class.all_names
      described_class.all_names

      expect(described_class).to have_received(:all).once
    end

    it "re-queries after reset_cache!" do
      allow(described_class).to receive_messages(table_exists?: true)
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_chat_42])
      allow(described_class).to receive(:all).and_return(relation)

      described_class.all_names
      described_class.reset_cache!
      described_class.all_names

      expect(described_class).to have_received(:all).twice
    end
  end

  describe ".stream?" do
    before do
      allow(described_class).to receive_messages(table_exists?: true)
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_chat_42])
      allow(described_class).to receive(:all).and_return(relation)
    end

    it "is true for a registered stream queue" do
      expect(described_class.stream?("pgbus_chat_42")).to be(true)
    end

    it "is false for a job queue" do
      expect(described_class.stream?("pgbus_default")).to be(false)
    end
  end

  describe ".table_exists?" do
    it "returns false (and does not raise) when the connection is unavailable" do
      allow(described_class).to receive(:connection).and_raise(StandardError, "no db")

      expect(described_class.table_exists?).to be(false)
    end
  end
end
