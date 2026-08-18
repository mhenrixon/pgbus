# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::DatabaseTasksGuard do
  # A stand-in for ActiveRecord::Tasks::DatabaseTasks (`extend self` module):
  # prepending onto the singleton class must intercept module-level calls.
  let(:tasks) do
    guard = described_class
    Module.new do
      extend self

      singleton_class.prepend(guard)

      def purge(configuration)
        [:purged, configuration]
      end

      def drop(configuration, *arguments)
        [:dropped, configuration, arguments]
      end
    end
  end

  before do
    allow(Pgbus::BusRecord).to receive(:disconnect_all_pools!)
  end

  it "disconnects BusRecord pools before purge runs, then delegates" do
    expect(tasks.purge(:cfg)).to eq(%i[purged cfg])
    expect(Pgbus::BusRecord).to have_received(:disconnect_all_pools!)
  end

  it "disconnects BusRecord pools before drop runs, then delegates" do
    expect(tasks.drop(:cfg, :extra)).to eq([:dropped, :cfg, [:extra]])
    expect(Pgbus::BusRecord).to have_received(:disconnect_all_pools!)
  end
end
