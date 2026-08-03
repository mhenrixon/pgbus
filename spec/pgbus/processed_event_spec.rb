# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ProcessedEvent do
  describe ".completion_column?" do
    after { described_class.reset_completion_column_check! }

    it "is true when the completed_at column exists" do
      allow(described_class).to receive(:column_names)
        .and_return(%w[id event_id handler_class processed_at completed_at])

      expect(described_class.completion_column?).to be true
    end

    it "is false when the column is missing and warns pointing at the upgrade generator" do
      allow(described_class).to receive(:column_names)
        .and_return(%w[id event_id handler_class processed_at])
      allow(Pgbus.logger).to receive(:warn)

      expect(described_class.completion_column?).to be false
      expect(Pgbus.logger).to have_received(:warn).once
    end

    it "memoizes the detection so the schema is only inspected once" do
      allow(described_class).to receive(:column_names)
        .and_return(%w[id event_id handler_class processed_at completed_at])

      3.times { described_class.completion_column? }

      expect(described_class).to have_received(:column_names).once
    end

    it "warns only once even when the fallback answer is served repeatedly" do
      allow(described_class).to receive(:column_names)
        .and_return(%w[id event_id handler_class processed_at])
      allow(Pgbus.logger).to receive(:warn)

      3.times { described_class.completion_column? }

      expect(Pgbus.logger).to have_received(:warn).once
    end

    it "does not memoize a failed detection (retries after a transient error)" do
      calls = 0
      allow(described_class).to receive(:column_names) do
        calls += 1
        raise ActiveRecord::ConnectionNotEstablished if calls == 1

        %w[id event_id handler_class processed_at completed_at]
      end

      expect { described_class.completion_column? }.to raise_error(ActiveRecord::ConnectionNotEstablished)
      expect(described_class.completion_column?).to be true
    end
  end
end
