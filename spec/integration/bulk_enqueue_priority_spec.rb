# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# ActiveJob.perform_all_later used to target the bare `pgbus_<queue>` table,
# which the priority strategy never creates — bulk enqueue raised
# PG::UndefinedTable and no worker would have read those messages anyway.
RSpec.describe "Bulk enqueue under priority routing", :integration do
  let(:queue) { "prio_bulk" }
  let(:job_class) do
    name = queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as name
      def self.name = "BulkPrioritySpec::Job"
      def perform(*); end
    end
  end

  around do |example|
    original = Pgbus.configuration.priority_levels
    Pgbus.configuration.priority_levels = 3
    Pgbus.reset_client!
    example.run
    Pgbus.configuration.priority_levels = original
    Pgbus.reset_client!
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("BulkPrioritySpec", Module.new)
    stub_const("BulkPrioritySpec::Job", job_class)
    Pgbus.client.ensure_queue(queue)
  end

  it "lands bulk payloads on the priority sub-queues workers actually read" do
    high = job_class.new
    high.priority = 0
    low = job_class.new
    low.priority = 2

    ActiveJob.perform_all_later([high, low])

    conn = ActiveRecord::Base.connection
    depths = Pgbus.client.physical_queue_names(queue).to_h do |physical|
      [physical, conn.select_value("SELECT count(*) FROM pgmq.q_#{physical}").to_i]
    end

    expect(depths).to include(
      "pgbus_int_#{queue}_p0" => 1,
      "pgbus_int_#{queue}_p2" => 1
    )
  end
end
