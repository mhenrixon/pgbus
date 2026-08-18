# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::DatabaseTasksGuard do
  # Records the interleaving of guard and base-method calls: the whole point
  # of the guard is that the disconnect happens BEFORE purge/drop runs —
  # a super-first refactor would reopen the idle session ahead of
  # DROP DATABASE and reintroduce the #409 wedge.
  let(:order) { [] }

  # A stand-in for ActiveRecord::Tasks::DatabaseTasks (`extend self` module):
  # prepending onto the singleton class must intercept module-level calls.
  let(:tasks) do
    guard = described_class
    calls = order
    Module.new do
      extend self

      singleton_class.prepend(guard)

      define_method(:purge) do |configuration|
        calls << :purge
        [:purged, configuration]
      end

      define_method(:drop) do |configuration, *arguments|
        calls << :drop
        [:dropped, configuration, arguments]
      end
    end
  end

  before do
    allow(Pgbus::BusRecord).to receive(:disconnect_all_pools!) { order << :disconnect }
  end

  it "disconnects BusRecord pools before purge runs, then delegates" do
    expect(tasks.purge(:cfg)).to eq(%i[purged cfg])
    expect(order).to eq(%i[disconnect purge])
  end

  it "disconnects BusRecord pools before drop runs, then delegates" do
    expect(tasks.drop(:cfg, :extra)).to eq([:dropped, :cfg, [:extra]])
    expect(order).to eq(%i[disconnect drop])
  end
end
