# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Filters do
  subject(:filters) { described_class.new }

  describe "#register and #lookup" do
    it "stores a predicate under a symbol label" do
      filters.register(:admin_only) { |ctx| ctx[:role] == "admin" }
      expect(filters.lookup(:admin_only)).to be_a(Proc)
    end

    it "allows registering a filter as a lambda argument (not just a block)" do
      filters.register(:odd, lambda(&:odd?))
      expect(filters.lookup(:odd).call(3)).to be true
    end

    it "returns nil for an unknown label" do
      expect(filters.lookup(:nope)).to be_nil
    end

    it "is thread-safe for concurrent registration" do
      # 100 threads each registering a unique filter. All should be
      # present in the lookup table at the end.
      threads = 100.times.map do |i|
        Thread.new { filters.register(:"filter_#{i}") { i } }
      end
      threads.each(&:join)

      100.times do |i|
        expect(filters.lookup(:"filter_#{i}")).not_to be_nil
      end
    end

    it "rejects non-symbol labels" do
      expect { filters.register("admin") { true } }
        .to raise_error(ArgumentError, /Symbol/)
    end

    it "rejects registration without either a block or a callable" do
      expect { filters.register(:empty) }
        .to raise_error(ArgumentError, /block or callable/)
    end
  end

  describe "#visible?" do
    it "returns true when the filter matches" do
      filters.register(:admin_only) { |ctx| ctx[:role] == "admin" }
      expect(filters.visible?(:admin_only, { role: "admin" })).to be true
    end

    it "returns false when the filter rejects" do
      filters.register(:admin_only) { |ctx| ctx[:role] == "admin" }
      expect(filters.visible?(:admin_only, { role: "viewer" })).to be false
    end

    it "returns true for an unknown label (fail-open for backwards compat)" do
      # An unknown filter label is almost always a typo or a boot-order
      # bug. Fail-open means clients receive the broadcast when the
      # developer intended to restrict it — the opposite of what you
      # want. But fail-CLOSED means a typo silently drops every
      # broadcast to zero subscribers, which is worse for debugging.
      # We choose fail-open AND log a warning at lookup time so the
      # developer sees the error in the log.
      expect(filters.visible?(:nope, { any: :context })).to be true
    end

    it "returns true when the label is nil (no filter applied)" do
      expect(filters.visible?(nil, { any: :context })).to be true
    end

    it "gracefully handles a raising filter by logging and returning false (fail-closed on exception)" do
      # If the predicate itself raises, we fail CLOSED — better to drop
      # one broadcast than to show private data to the wrong user.
      filters.register(:broken) { |_ctx| raise "boom" }
      expect(filters.visible?(:broken, {})).to be false
    end
  end
end
