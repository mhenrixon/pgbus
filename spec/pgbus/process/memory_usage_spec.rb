# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::MemoryUsage do
  before { described_class.reset! }

  after { described_class.reset! }

  describe ".current_mb" do
    context "when on darwin" do
      before do
        stub_const("RUBY_PLATFORM", "x86_64-darwin23")
        allow(described_class).to receive(:`).with(/ps -o rss=/).and_return("131072\n")
      end

      it "returns RSS in megabytes" do
        expect(described_class.current_mb).to eq(128)
      end

      it "caches the value within the TTL window" do
        described_class.current_mb
        described_class.current_mb

        expect(described_class).to have_received(:`).once
      end

      it "re-reads after TTL expires" do
        described_class.current_mb

        allow(Process).to receive(:clock_gettime)
          .with(Process::CLOCK_MONOTONIC)
          .and_return(Process.clock_gettime(Process::CLOCK_MONOTONIC) + described_class::MEMORY_CHECK_TTL + 1)

        described_class.current_mb

        expect(described_class).to have_received(:`).twice
      end
    end

    context "when on linux" do
      before do
        stub_const("RUBY_PLATFORM", "x86_64-linux")
      end

      it "reads /proc/PID/statm and converts pages to MB" do
        # statm second field = pages; 4096 bytes/page
        pages = 65_536
        allow(File).to receive(:read)
          .with("/proc/#{Process.pid}/statm")
          .and_return("100000 #{pages} 50000 1000 0 40000 0")

        expect(described_class.current_mb).to eq(256)
      end

      it "returns 0 when /proc file is missing" do
        allow(File).to receive(:read)
          .with("/proc/#{Process.pid}/statm")
          .and_raise(Errno::ENOENT)

        expect(described_class.current_mb).to eq(0)
      end
    end

    describe "thread safety" do
      before do
        stub_const("RUBY_PLATFORM", "x86_64-darwin23")
        allow(described_class).to receive(:`).with(/ps -o rss=/).and_return("131072\n")
      end

      it "handles concurrent access without errors" do
        threads = Array.new(10) do
          Thread.new { described_class.current_mb }
        end

        results = threads.map(&:value)
        expect(results).to all(eq(128))
      end
    end
  end

  describe ".reset!" do
    before do
      stub_const("RUBY_PLATFORM", "x86_64-darwin23")
      allow(described_class).to receive(:`).with(/ps -o rss=/).and_return("131072\n")
    end

    it "clears the cached value so the next call re-reads" do
      described_class.current_mb
      described_class.reset!
      described_class.current_mb

      expect(described_class).to have_received(:`).twice
    end
  end
end
