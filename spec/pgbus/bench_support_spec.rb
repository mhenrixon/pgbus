# frozen_string_literal: true

require "spec_helper"

RSpec.describe "BenchSupport" do
  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    require_relative "../../benchmarks/bench_helper"
  end

  describe ".header" do
    it "prints a colored title with underline" do
      output = capture_stdout { BenchSupport.header("Test Title") }
      expect(output).to include("Test Title")
      expect(output).to include("─" * "Test Title".length)
    end
  end

  describe ".allocations" do
    it "reports allocated and retained objects" do
      output = capture_stdout do
        report = BenchSupport.allocations("test alloc") { "hello" * 10 }
        expect(report).to be_a(MemoryProfiler::Results)
      end
      expect(output).to include("test alloc")
      expect(output).to match(/\d+ objects/)
      expect(output).to match(/\d+ bytes/)
      expect(output).to include("retained:")
    end
  end

  describe ".ips" do
    it "runs benchmark-ips with comparison" do
      output = capture_stdout do
        BenchSupport.ips(time: 0.1, warmup: 0.05) do |x|
          x.report("fast") { 1 + 1 }
          x.report("slow") { (1..100).to_a }
        end
      end
      expect(output).to include("fast")
      expect(output).to include("slow")
    end
  end

  private

  def capture_stdout
    original = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = original
  end
end
