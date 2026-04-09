# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::StreamStat do
  describe ".record!" do
    before do
      allow(described_class).to receive_messages(create!: nil, table_exists?: true)
    end

    it "creates a broadcast stat with fanout" do
      described_class.record!(
        stream_name: "chat_42",
        event_type: "broadcast",
        duration_ms: 17,
        fanout: 12
      )

      expect(described_class).to have_received(:create!).with(
        stream_name: "chat_42",
        event_type: "broadcast",
        duration_ms: 17,
        fanout: 12
      )
    end

    it "creates a connect stat without fanout" do
      described_class.record!(
        stream_name: "chat_42",
        event_type: "connect",
        duration_ms: 3
      )

      expect(described_class).to have_received(:create!).with(
        stream_name: "chat_42",
        event_type: "connect",
        duration_ms: 3,
        fanout: nil
      )
    end

    it "skips when table does not exist" do
      allow(described_class).to receive(:table_exists?).and_return(false)

      described_class.record!(stream_name: "chat", event_type: "broadcast", duration_ms: 0)

      expect(described_class).not_to have_received(:create!)
    end

    it "swallows errors" do
      allow(described_class).to receive(:create!).and_raise(StandardError, "db down")

      expect do
        described_class.record!(stream_name: "chat", event_type: "broadcast", duration_ms: 0)
      end.not_to raise_error
    end

    it "coerces duration_ms to integer" do
      described_class.record!(
        stream_name: "chat",
        event_type: "broadcast",
        duration_ms: 4.7
      )

      expect(described_class).to have_received(:create!).with(
        hash_including(duration_ms: 4)
      )
    end
  end

  describe ".table_exists?" do
    before do
      described_class.remove_instance_variable(:@table_exists) if described_class.instance_variable_defined?(:@table_exists)
    end

    after do
      described_class.remove_instance_variable(:@table_exists) if described_class.instance_variable_defined?(:@table_exists)
    end

    it "returns false on connection error without poisoning the memo" do
      allow(described_class).to receive(:connection).and_raise(StandardError, "no db")

      expect(described_class.table_exists?).to be false
      # The failed probe must NOT set @table_exists — otherwise a
      # transient PG blip during boot permanently disables stream
      # stats for the process lifetime.
      expect(described_class.instance_variable_defined?(:@table_exists)).to be false
    end

    it "memoizes a successful probe" do
      fake_connection = instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter)
      allow(fake_connection).to receive(:table_exists?).with(described_class.table_name).and_return(true)
      allow(described_class).to receive(:connection).and_return(fake_connection)

      expect(described_class.table_exists?).to be true
      # Second call hits the memo, not the connection.
      expect(described_class.table_exists?).to be true
      expect(fake_connection).to have_received(:table_exists?).once
    end
  end
end
