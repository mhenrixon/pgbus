# frozen_string_literal: true

require "spec_helper"
require_relative "../../benchmarks/bench_support"

RSpec.describe "BenchSupport" do
  describe ".header" do
    it "prints a colored title with underline" do
      expect { BenchSupport.header("Test Title") }
        .to output(/Test Title.*#{"─" * "Test Title".length}/m).to_stdout
    end
  end

  describe ".allocations" do
    it "reports allocated and retained objects" do
      report = nil
      expect { report = BenchSupport.allocations("test alloc") { "hello" * 10 } }
        .to output(/test alloc\s+\d+ objects\s+\d+ bytes \(retained: \d+ objects\)/).to_stdout
      expect(report).to be_a(MemoryProfiler::Results)
    end
  end

  describe ".ips" do
    it "runs benchmark-ips with comparison" do
      expect do
        BenchSupport.ips(time: 0.1, warmup: 0.05) do |x|
          x.report("fast") { 1 + 1 }
          x.report("slow") { (1..100).to_a }
        end
      end.to output(/fast.*slow/m).to_stdout
    end
  end
end
