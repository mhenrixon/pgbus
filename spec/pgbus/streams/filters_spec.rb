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

    it "returns false for an unknown label (fail-closed) and logs a warning" do
      # Audience filtering is a data-isolation feature. Failing open
      # on a typo'd or renamed label would turn a restricted broadcast
      # into a public one — the opposite of what the developer wanted.
      # The warning log is the developer-facing signal; assert it fires
      # so future changes can't silently drop the log and regress the
      # "typos are loud" guarantee.
      warnings = []
      logger = instance_double(Logger)
      allow(logger).to receive(:warn) { |&block| warnings << block.call }
      filters_with_logger = described_class.new(logger: logger)

      expect(filters_with_logger.visible?(:nope, { any: :context })).to be false
      expect(warnings).to include(a_string_matching(/unknown filter label.*:nope/))
    end

    it "returns true when the label is nil (no filter applied)" do
      expect(filters.visible?(nil, { any: :context })).to be true
    end

    it "gracefully handles a raising filter by logging and returning false (fail-closed on exception)" do
      # If the predicate itself raises, we fail CLOSED — better to drop
      # one broadcast than to show private data to the wrong user.
      # Verify the error log fires as well, for the same "don't let
      # this behavior silently regress" reason as the unknown-label test.
      errors = []
      logger = instance_double(Logger)
      allow(logger).to receive(:error) { |&block| errors << block.call }
      filters_with_logger = described_class.new(logger: logger)
      filters_with_logger.register(:broken) { |_ctx| raise "boom" }

      expect(filters_with_logger.visible?(:broken, {})).to be false
      expect(errors).to include(a_string_matching(/filter.*:broken.*raised.*boom/))
    end
  end
end
