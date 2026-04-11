# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Pgbus::LogFormatter do
  describe Pgbus::LogFormatter::Text do
    subject(:formatter) { described_class.new }

    it "formats a basic message" do
      output = formatter.call("INFO", Time.now, "Pgbus", "test message")

      expect(output).to include("INFO")
      expect(output).to include("test message")
      expect(output).to end_with("\n")
    end

    it "includes timestamp, pid, and tid" do
      output = formatter.call("WARN", Time.now, "Pgbus", "hello")

      expect(output).to include("pid=#{Process.pid}")
      expect(output).to include("tid=")
    end
  end

  describe Pgbus::LogFormatter::JSON do
    subject(:formatter) { described_class.new }

    it "outputs valid JSON with a trailing newline" do
      output = formatter.call("INFO", Time.now.utc, "Pgbus", "test message")

      expect(output).to end_with("\n")
      parsed = JSON.parse(output)
      expect(parsed).to be_a(Hash)
    end

    it "includes standard fields" do
      time = Time.now.utc
      output = formatter.call("ERROR", time, "Pgbus", "boom")
      parsed = JSON.parse(output)

      expect(parsed["ts"]).to eq(time.iso8601(3))
      expect(parsed["pid"]).to eq(Process.pid)
      expect(parsed["tid"]).to be_a(String)
      expect(parsed["lvl"]).to eq("ERROR")
      expect(parsed["msg"]).to eq("boom")
    end

    it "extracts the [Pgbus] component prefix into a separate field" do
      output = formatter.call("INFO", Time.now.utc, "Pgbus", "[Pgbus] Starting server")
      parsed = JSON.parse(output)

      expect(parsed["component"]).to eq("Pgbus")
      expect(parsed["msg"]).to eq("Starting server")
    end

    it "extracts nested component prefixes like [Pgbus::Web]" do
      output = formatter.call("ERROR", Time.now.utc, "Pgbus", "[Pgbus::Web] Error fetching data")
      parsed = JSON.parse(output)

      expect(parsed["component"]).to eq("Pgbus::Web")
      expect(parsed["msg"]).to eq("Error fetching data")
    end

    it "includes ctx when provided via thread-local context" do
      Pgbus::LogFormatter.with_context(queue: "default", job_class: "MyJob") do
        output = formatter.call("INFO", Time.now.utc, "Pgbus", "processing")
        parsed = JSON.parse(output)

        expect(parsed["ctx"]).to eq("queue" => "default", "job_class" => "MyJob")
      end
    end

    it "omits ctx when context is empty" do
      output = formatter.call("INFO", Time.now.utc, "Pgbus", "hello")
      parsed = JSON.parse(output)

      expect(parsed).not_to have_key("ctx")
    end
  end

  describe ".with_context" do
    it "merges context for the duration of the block" do
      inner_ctx = nil

      described_class.with_context(queue: "default") do
        described_class.with_context(job_class: "MyJob") do
          inner_ctx = described_class.current_context.dup
        end
      end

      expect(inner_ctx).to eq(queue: "default", job_class: "MyJob")
      expect(described_class.current_context).to be_empty
    end
  end
end
