# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::BatchEntry do
  describe ".increment_total_jobs!" do
    it "raises AlreadyFinished when no unfinished row is updated" do
      relation = double("relation", update_all: 0)
      allow(described_class).to receive(:where)
        .with(batch_id: "b1", status: %w[pending processing])
        .and_return(relation)

      expect do
        described_class.increment_total_jobs!("b1", 2)
      end.to raise_error(Pgbus::Batch::AlreadyFinished, /already finished/)
    end

    it "returns true when the unfinished row is updated" do
      relation = double("relation", update_all: 1)
      allow(described_class).to receive(:where)
        .with(batch_id: "b1", status: %w[pending processing])
        .and_return(relation)

      expect(described_class.increment_total_jobs!("b1", 2)).to be true
    end
  end
end
