# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Recurring::CommandJob do
  it "inherits from ActiveJob::Base" do
    expect(described_class.ancestors).to include(ActiveJob::Base)
  end

  it "uses the default ActiveJob queue (no hardcoded queue)" do
    job = described_class.new("1 + 1")
    expect(job.queue_name).to eq("default")
  end
end
