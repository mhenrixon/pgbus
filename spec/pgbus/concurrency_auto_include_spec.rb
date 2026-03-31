# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

RSpec.describe "Pgbus::Concurrency auto-include" do # rubocop:disable RSpec/DescribeClass
  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    # Simulate what the Engine does in on_load(:active_job)
    ActiveJob::Base.include(Pgbus::Concurrency) unless ActiveJob::Base < Pgbus::Concurrency
  end

  it "makes limits_concurrency available on ActiveJob::Base subclasses" do
    stub_const("AutoIncludeTestJob", Class.new(ActiveJob::Base))
    expect(AutoIncludeTestJob).to respond_to(:limits_concurrency)
  end

  it "does not break jobs that explicitly include Pgbus::Concurrency" do
    klass = Class.new(ActiveJob::Base) do
      include Pgbus::Concurrency

      limits_concurrency to: 1, key: ->(*) { "test" }
    end
    stub_const("ExplicitIncludeTestJob", klass)

    expect(ExplicitIncludeTestJob.pgbus_concurrency).to include(limit: 1)
  end

  it "allows limits_concurrency with all options" do
    klass = Class.new(ActiveJob::Base) do
      limits_concurrency to: 3, key: ->(id) { "job-#{id}" }, duration: 300, on_conflict: :discard
    end
    stub_const("FullOptionsTestJob", klass)

    config = FullOptionsTestJob.pgbus_concurrency
    expect(config[:limit]).to eq(3)
    expect(config[:duration]).to eq(300)
    expect(config[:on_conflict]).to eq(:discard)
  end
end
