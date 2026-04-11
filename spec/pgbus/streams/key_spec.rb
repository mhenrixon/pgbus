# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Key do
  # Minimal stand-in that quacks like an ActiveRecord model without
  # needing a live DB connection. Overrides `is_a?` so the code path
  # that checks `part.is_a?(::ActiveRecord::Base)` fires.
  let(:ar_like_class) do
    param_key = "chat"
    Class.new do
      attr_reader :id

      def initialize(id)
        @id = id
      end

      define_singleton_method(:model_name) do
        Struct.new(:param_key).new(param_key)
      end

      def is_a?(klass)
        return true if klass == ActiveRecord::Base

        super
      end
    end
  end

  describe ".short_id" do
    let(:record) { double("record", id: "9c14e8b2-94c3-4c6f-8ca1-f50d2f5e22ca") }

    it "returns a 16-char (64-bit) hex prefix of SHA-256(id) by default" do
      expect(described_class.short_id(record)).to match(/\A[0-9a-f]{16}\z/)
    end

    it "is deterministic for the same id" do
      expect(described_class.short_id(record)).to eq(described_class.short_id(record))
    end

    it "differs for different ids" do
      other = double("record", id: "11111111-2222-3333-4444-555555555555")
      expect(described_class.short_id(record)).not_to eq(described_class.short_id(other))
    end

    it "respects digest_bits override (128 bits -> 32 hex chars)" do
      expect(described_class.short_id(record, digest_bits: 128)).to match(/\A[0-9a-f]{32}\z/)
    end

    it "rejects non-multiple-of-4 digest_bits" do
      expect { described_class.short_id(record, digest_bits: 63) }
        .to raise_error(ArgumentError, /multiple of 4/)
    end

    it "rejects non-positive digest_bits" do
      expect { described_class.short_id(record, digest_bits: 0) }
        .to raise_error(ArgumentError, /positive/)
    end

    it "handles numeric ids by coercing to string" do
      numeric = double("record", id: 42)
      expect(described_class.short_id(numeric)).to match(/\A[0-9a-f]{16}\z/)
    end
  end

  describe ".normalize" do
    it "passes strings through verbatim" do
      expect(described_class.normalize("messages")).to eq("messages")
    end

    it "coerces symbols to strings" do
      expect(described_class.normalize(:messages)).to eq("messages")
    end

    it "hashes ActiveRecord models to <param_key>_<short_id>" do
      record = ar_like_class.new("abc-123")
      result = described_class.normalize(record)
      expect(result).to match(/\Achat_[0-9a-f]{16}\z/)
    end

    it "uses to_stream_key when the object responds to it (non-AR)" do
      obj = double("stream_keyable", to_stream_key: "custom_abc")
      expect(described_class.normalize(obj)).to eq("custom_abc")
    end

    it "falls back to to_gid_param when present" do
      obj = double("gid", to_gid_param: "gid_param_value")
      expect(described_class.normalize(obj)).to eq("gid_param_value")
    end

    it "falls back to to_param when neither to_stream_key nor to_gid_param is present" do
      klass = Class.new do
        def to_param = "param_value"
      end
      expect(described_class.normalize(klass.new)).to eq("param_value")
    end

    it "falls back to to_s for anything else" do
      expect(described_class.normalize(42)).to eq("42")
    end
  end

  describe ".stream_key" do
    it "joins normalized parts with ':'" do
      expect(described_class.stream_key("room", :messages)).to eq("room:messages")
    end

    it "flattens nested arrays (turbo-rails-compatible)" do
      expect(described_class.stream_key(%w[a b], :c)).to eq("a:b:c")
    end

    it "hashes an ActiveRecord model part to a short id" do
      record = ar_like_class.new("550e8400-e29b-41d4-a716-446655440000")
      result = described_class.stream_key(record, :msgs)
      expect(result).to match(/\Achat_[0-9a-f]{16}:msgs\z/)
    end

    it "returns a key that fits within the queue-name budget for normal inputs" do
      key = described_class.stream_key("ai_chat_3a4f9c21b7d20e18", :messages)
      budget = described_class.queue_name_budget
      expect(key.length).to be <= budget
    end

    it "raises ArgumentError when the composed key exceeds the pgbus budget" do
      oversized = "x" * (described_class.queue_name_budget + 1)
      expect { described_class.stream_key(oversized) }
        .to raise_error(ArgumentError, /exceeds pgbus budget/)
    end

    it "mentions the current queue_prefix in the error message" do
      original_prefix = Pgbus.configuration.queue_prefix
      Pgbus.configuration.queue_prefix = "custom_prefix"
      oversized = "x" * 100
      expect { described_class.stream_key(oversized) }
        .to raise_error(ArgumentError, /custom_prefix/)
    ensure
      Pgbus.configuration.queue_prefix = original_prefix
    end
  end

  describe ".queue_name_budget" do
    it "is derived from the current queue_prefix so prefix overrides propagate" do
      original_prefix = Pgbus.configuration.queue_prefix
      Pgbus.configuration.queue_prefix = "abc"    # 3 chars + underscore = 4
      short_budget = described_class.queue_name_budget
      Pgbus.configuration.queue_prefix = "abcdef" # 6 chars + underscore = 7
      long_prefix_budget = described_class.queue_name_budget
      expect(short_budget - long_prefix_budget).to eq(3)
    ensure
      Pgbus.configuration.queue_prefix = original_prefix
    end

    it "reserves NAMEDATALEN minus queue_prefix minus separator" do
      max = Pgbus::QueueNameValidator::MAX_QUEUE_NAME_LENGTH
      expected = max - Pgbus.configuration.queue_prefix.length - 1
      expect(described_class.queue_name_budget).to eq(expected)
    end
  end
end
