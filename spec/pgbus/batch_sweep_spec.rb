# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Batch::Sweep do
  after { Pgbus::Batch.reset_executions_migrated_cache! }

  it "is a no-op when the executions table is missing" do
    allow(Pgbus::BatchExecution).to receive(:table_exists?).and_return(false)
    Pgbus::Batch.reset_executions_migrated_cache!
    allow(Pgbus::BatchExecution).to receive(:where)

    described_class.run(client: double("client"))

    expect(Pgbus::BatchExecution).not_to have_received(:where)
  end

  it "defines a 5-minute stall threshold matching solid_queue" do
    expect(described_class::STALL_THRESHOLD).to eq(300)
  end
end
