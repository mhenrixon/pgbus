# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Streamable do
  # Minimal stand-in for an ActiveRecord model. Streamable only relies on
  # `id` and `class.model_name.param_key`, so we avoid pulling in a real
  # ActiveRecord setup for a unit spec.
  let(:model_class) do
    Class.new do
      include Pgbus::Streams::Streamable

      attr_reader :id

      def initialize(id)
        @id = id
      end

      def self.model_name
        Struct.new(:param_key).new("ai_chat")
      end
    end
  end

  let(:record) { model_class.new("9c14e8b2-94c3-4c6f-8ca1-f50d2f5e22ca") }

  describe "#short_id" do
    it "delegates to Pgbus::Streams::Key.short_id (16 hex chars by default)" do
      expect(record.short_id).to match(/\A[0-9a-f]{16}\z/)
    end

    it "is deterministic across calls" do
      expect(record.short_id).to eq(record.short_id)
    end

    it "accepts digest_bits override" do
      expect(record.short_id(digest_bits: 128)).to match(/\A[0-9a-f]{32}\z/)
    end
  end

  describe "#to_stream_key" do
    it "returns '<param_key>_<short_id>'" do
      expect(record.to_stream_key).to match(/\Aai_chat_[0-9a-f]{16}\z/)
    end

    it "is stable for the same id" do
      expect(record.to_stream_key).to eq(record.to_stream_key)
    end

    it "differs for different ids on the same class" do
      other = model_class.new("ffffffff-ffff-ffff-ffff-ffffffffffff")
      expect(record.to_stream_key).not_to eq(other.to_stream_key)
    end

    it "bypasses a host-class #short_id override (method dispatch would silently win)" do
      # Ruby does NOT warn when a class defines an instance method and
      # then includes a module with the same name — the class method
      # wins method lookup. If `to_stream_key` went through the
      # unqualified `short_id` call, a host class defining its own
      # `#short_id` (e.g. a display helper) would silently hijack the
      # wire format. Streamable calls Key.short_id(self) explicitly to
      # defeat this, and this spec pins the behavior.
      hostile_class = Class.new do
        include Pgbus::Streams::Streamable

        attr_reader :id

        def initialize(id)
          @id = id
        end

        def self.model_name
          Struct.new(:param_key).new("ai_chat")
        end

        def short_id(*)
          "HOSTILE" # would break the wire format if dispatched to
        end
      end

      record = hostile_class.new("9c14e8b2-94c3-4c6f-8ca1-f50d2f5e22ca")
      expect(record.to_stream_key).to match(/\Aai_chat_[0-9a-f]{16}\z/)
      expect(record.to_stream_key).not_to include("HOSTILE")
    end
  end

  describe "integration with Pgbus.stream_key" do
    it "produces a composable key when mixed with other streamables" do
      key = Pgbus.stream_key(record.to_stream_key, :messages)
      expect(key).to match(/\Aai_chat_[0-9a-f]{16}:messages\z/)
    end

    it "fits within the pgbus queue-name budget for UUID primary keys" do
      key = Pgbus.stream_key(record.to_stream_key, :messages)
      expect(key.length).to be <= Pgbus::Streams::Key.queue_name_budget
    end
  end
end
