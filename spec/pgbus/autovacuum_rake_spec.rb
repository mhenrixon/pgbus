# frozen_string_literal: true

require "spec_helper"
require "rake"

# Connection discipline for the pgbus:tune_autovacuum task
# (lib/tasks/pgbus_autovacuum.rake). The task is enhanced onto
# db:schema:load, so on a dedicated pgbus database it must not leave an
# idle session behind — one would block a later DROP DATABASE in the same
# rake process (issue #409).
RSpec.describe "pgbus:tune_autovacuum rake task" do # rubocop:disable RSpec/DescribeClass
  let(:rake) { Rake::Application.new }
  let(:conn) { double("connection") }
  let(:pool) { double("connection_pool") }

  around do |example|
    original = Rake.application
    Rake.application = rake
    example.run
  ensure
    Rake.application = original
  end

  before do
    Rake::Task.define_task(:environment)
    load File.expand_path("../../lib/tasks/pgbus_autovacuum.rake", __dir__)

    allow(conn).to receive(:select_value).and_return(1)
    allow(conn).to receive(:execute)
  end

  def invoke
    expect { Rake::Task["pgbus:tune_autovacuum"].invoke }.to output(/Autovacuum tuning|skipping/).to_stdout
  end

  context "with a dedicated pgbus database (connects_to set)" do
    before do
      allow(Pgbus.configuration).to receive(:connects_to).and_return({ database: { writing: :pgbus } })
      allow(Pgbus::BusRecord).to receive(:connection_pool).and_return(pool)
      allow(pool).to receive(:with_connection).and_yield(conn)
      allow(pool).to receive(:disconnect!)
    end

    it "checks out the connection via the pool (no permanent lease)" do
      invoke
      expect(pool).to have_received(:with_connection)
    end

    it "disconnects the pool after tuning so no idle session survives the task" do
      invoke
      expect(pool).to have_received(:disconnect!)
    end

    it "applies tuning SQL for queues and high-churn tables" do
      invoke
      expect(conn).to have_received(:execute).twice
    end

    it "still disconnects when the pgmq schema is absent" do
      allow(conn).to receive(:select_value).and_return(nil)

      expect { Rake::Task["pgbus:tune_autovacuum"].invoke }.to output(/skipping/).to_stdout
      expect(pool).to have_received(:disconnect!)
      expect(conn).not_to have_received(:execute)
    end
  end

  context "with pgbus in the primary database (connects_to nil)" do
    before do
      allow(Pgbus.configuration).to receive(:connects_to).and_return(nil)
      allow(ActiveRecord::Base).to receive(:connection).and_return(conn)
    end

    it "uses the primary connection and never touches BusRecord" do
      allow(Pgbus::BusRecord).to receive(:connection_pool)

      invoke

      expect(conn).to have_received(:execute).twice
      expect(Pgbus::BusRecord).not_to have_received(:connection_pool)
    end
  end
end
