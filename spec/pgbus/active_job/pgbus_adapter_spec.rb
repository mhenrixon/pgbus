# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

RSpec.describe "ActiveJob::QueueAdapters::PgbusAdapter" do
  it "is defined in the ActiveJob::QueueAdapters namespace" do
    expect(defined?(ActiveJob::QueueAdapters::PgbusAdapter)).to be_truthy
  end

  it "can be found via const_get (Rails 8.1 adapter lookup)" do
    adapter_class = ActiveJob::QueueAdapters.const_get("PgbusAdapter")
    expect(adapter_class).to eq(ActiveJob::QueueAdapters::PgbusAdapter)
  end

  it "responds to enqueue" do
    adapter = ActiveJob::QueueAdapters::PgbusAdapter.new
    expect(adapter).to respond_to(:enqueue)
  end

  it "responds to enqueue_at" do
    adapter = ActiveJob::QueueAdapters::PgbusAdapter.new
    expect(adapter).to respond_to(:enqueue_at)
  end

  it "responds to enqueue_all" do
    adapter = ActiveJob::QueueAdapters::PgbusAdapter.new
    expect(adapter).to respond_to(:enqueue_all)
  end
end
