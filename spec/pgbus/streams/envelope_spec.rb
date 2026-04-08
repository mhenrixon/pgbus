# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Envelope do
  describe ".message" do
    it "encodes a basic id + event + data triple" do
      data = '<turbo-stream action="replace"></turbo-stream>'
      result = described_class.message(id: 1248, event: "turbo-stream", data: data)
      expect(result).to eq(
        "id: 1248\n" \
        "event: turbo-stream\n" \
        "data: #{data}\n" \
        "\n"
      )
    end

    it "strips newlines from data so the SSE parser sees one logical event" do
      data = "<turbo-stream>\n  <template>x</template>\n</turbo-stream>"
      result = described_class.message(id: 1, event: "turbo-stream", data: data)
      # Newlines collapsed; surrounding spaces preserved
      expect(result).not_to include("\n  <template>")
      expect(result).to include("data: <turbo-stream>  <template>x</template></turbo-stream>")
    end

    it "strips carriage returns from data" do
      data = "a\r\nb\rc"
      result = described_class.message(id: 1, event: "msg", data: data)
      expect(result).to include("data: abc\n")
    end

    it "preserves UTF-8 characters in data" do
      data = "héllo 世界"
      result = described_class.message(id: 1, event: "msg", data: data)
      expect(result).to include("data: héllo 世界\n")
      expect(result.encoding).to eq(Encoding::UTF_8)
    end

    it "writes id even when zero" do
      result = described_class.message(id: 0, event: "msg", data: "x")
      expect(result).to start_with("id: 0\n")
    end

    it "raises when id is nil" do
      expect { described_class.message(id: nil, event: "msg", data: "x") }
        .to raise_error(ArgumentError, /id/)
    end

    it "raises when event is nil or empty" do
      expect { described_class.message(id: 1, event: nil, data: "x") }
        .to raise_error(ArgumentError, /event/)
      expect { described_class.message(id: 1, event: "", data: "x") }
        .to raise_error(ArgumentError, /event/)
    end

    it "ends with a blank line (the SSE event terminator)" do
      result = described_class.message(id: 1, event: "msg", data: "x")
      expect(result).to end_with("\n\n")
    end
  end

  describe ".comment" do
    it "encodes an SSE comment line" do
      expect(described_class.comment("hello")).to eq(": hello\n\n")
    end

    it "strips newlines from comments" do
      expect(described_class.comment("a\nb")).to eq(": ab\n\n")
    end

    it "handles empty comments (still ends with the terminator)" do
      expect(described_class.comment("")).to eq(": \n\n")
    end
  end

  describe ".retry_directive" do
    it "encodes a retry: directive in milliseconds" do
      expect(described_class.retry_directive(2000)).to eq("retry: 2000\n\n")
    end

    it "rejects negative values" do
      expect { described_class.retry_directive(-1) }
        .to raise_error(ArgumentError, /retry/)
    end

    it "rejects non-integer values" do
      expect { described_class.retry_directive(1.5) }
        .to raise_error(ArgumentError, /retry/)
    end
  end

  describe ".http_response_headers" do
    it "returns the SSE response status line and headers as a single string" do
      result = described_class.http_response_headers
      expect(result).to start_with("HTTP/1.1 200 OK\r\n")
      expect(result).to include("content-type: text/event-stream\r\n")
      expect(result).to include("cache-control: no-cache, no-transform\r\n")
      expect(result).to include("x-accel-buffering: no\r\n")
      expect(result).to include("connection: keep-alive\r\n")
      expect(result).to end_with("\r\n\r\n")
    end
  end
end
