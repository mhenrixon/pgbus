# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::CurrentAttributes do
  let(:config) { Pgbus::Configuration.new }
  let(:current_class) { Class.new(ActiveSupport::CurrentAttributes) { attribute :tenant, :request_id, :request } }
  let(:admin_class) { Class.new(ActiveSupport::CurrentAttributes) { attribute :admin_id } }
  # A GlobalID-identifiable value, like an Active Record model would be.
  let(:tenant_class) do
    Class.new do
      include GlobalID::Identification

      attr_reader :id

      def initialize(id) = @id = id
      def self.name = "SpecTenant"
      def ==(other) = other.is_a?(self.class) && other.id == id
    end
  end

  before do
    stub_const("SpecCurrent", current_class)
    stub_const("SpecAdminCurrent", admin_class)
    stub_const("SpecTenant", tenant_class)
    allow(GlobalID::Locator).to receive(:locate) { |gid| SpecTenant.new(GlobalID.parse(gid).model_id.to_i) }
  end

  after { ActiveSupport::CurrentAttributes.clear_all }

  describe ".enabled?" do
    it "is false when config.current_attributes is nil" do
      expect(described_class.enabled?(config)).to be(false)
    end

    it "is true for :auto or an explicit list" do
      config.current_attributes = :auto
      expect(described_class.enabled?(config)).to be(true)
      config.current_attributes = ["SpecCurrent"]
      expect(described_class.enabled?(config)).to be(true)
    end
  end

  describe ".capture" do
    it "returns nil when disabled" do
      SpecCurrent.tenant = "t"
      expect(described_class.capture(config)).to be_nil
    end

    it "returns nil when no persisted class has assigned attributes" do
      config.current_attributes = ["SpecCurrent"]
      expect(described_class.capture(config)).to be_nil
    end

    it "serializes assigned attributes of an explicitly listed class via ActiveJob::Arguments" do
      config.current_attributes = ["SpecCurrent"]
      SpecCurrent.tenant = SpecTenant.new(42)
      SpecCurrent.request_id = "req-1"

      captured = described_class.capture(config)

      expect(captured.keys).to eq(["SpecCurrent"])
      # GlobalID.app differs when the dummy Rails app is loaded in the same process — match the path only.
      expect(captured["SpecCurrent"]["tenant"]["_aj_globalid"]).to match(%r{\Agid://[^/]+/SpecTenant/42\z})
      expect(captured["SpecCurrent"]["request_id"]).to eq("req-1")
    end

    it "omits nil-valued attributes" do
      config.current_attributes = ["SpecCurrent"]
      SpecCurrent.tenant = "t"
      SpecCurrent.request_id = nil

      expect(described_class.capture(config)["SpecCurrent"]).not_to have_key("request_id")
    end

    it "with :auto captures every CurrentAttributes subclass that has assigned attributes" do
      config.current_attributes = :auto
      SpecCurrent.request_id = "r"
      SpecAdminCurrent.admin_id = 7

      captured = described_class.capture(config)

      expect(captured).to include("SpecCurrent" => hash_including("request_id" => "r"),
                                  "SpecAdminCurrent" => hash_including("admin_id" => 7))
    end

    it "honours except: filters" do
      config.current_attributes = { "SpecCurrent" => { except: [:request] } }
      SpecCurrent.request_id = "r"
      SpecCurrent.request = Object.new # unserializable, but excluded

      expect(described_class.capture(config)["SpecCurrent"].except("_aj_symbol_keys").keys).to eq(["request_id"])
    end

    it "honours only: filters" do
      config.current_attributes = { "SpecCurrent" => { only: [:tenant] } }
      SpecCurrent.tenant = "t"
      SpecCurrent.request_id = "r"

      expect(described_class.capture(config)["SpecCurrent"].except("_aj_symbol_keys").keys).to eq(["tenant"])
    end

    it "raises CurrentAttributesError naming the class and attribute for an unserializable value" do
      config.current_attributes = ["SpecCurrent"]
      SpecCurrent.request = Object.new

      expect { described_class.capture(config) }.to raise_error(Pgbus::CurrentAttributesError) { |e|
        expect(e.message).to include("SpecCurrent#request")
        expect(e.message).to include("Object")
        expect(e.message).to include("except: [:request]")
      }
    end

    it "warns once and skips an explicitly listed class that does not exist" do
      config.current_attributes = %w[Nope::Current SpecCurrent]
      SpecCurrent.request_id = "r"
      warnings = []
      allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }

      2.times { described_class.capture(config) }

      expect(warnings.grep(/Nope::Current/).size).to eq(1)
      expect(described_class.capture(config).keys).to eq(["SpecCurrent"])
    end

    it "accepts a per-job override spec in place of the config" do
      config.current_attributes = :auto
      SpecCurrent.request_id = "r"
      SpecAdminCurrent.admin_id = 7

      captured = described_class.capture(config, override: ["SpecAdminCurrent"])

      expect(captured.keys).to eq(["SpecAdminCurrent"])
    end

    it "returns nil when the per-job override is false" do
      config.current_attributes = :auto
      SpecCurrent.request_id = "r"

      expect(described_class.capture(config, override: false)).to be_nil
    end
  end

  describe ".restore" do
    let(:stored) do
      {
        "SpecCurrent" => { "tenant" => { "_aj_globalid" => "gid://pgbus-test/SpecTenant/42" }, "request_id" => "r" },
        "SpecAdminCurrent" => { "admin_id" => 7 }
      }
    end

    it "sets every class's attributes for the block and restores the previous values after" do
      SpecCurrent.request_id = "before"
      seen = nil

      result = described_class.restore(stored) do
        seen = [SpecCurrent.tenant, SpecCurrent.request_id, SpecAdminCurrent.admin_id]
        :done
      end

      expect(result).to eq(:done)
      expect(seen).to eq([SpecTenant.new(42), "r", 7])
      expect(SpecCurrent.request_id).to eq("before")
      expect(SpecCurrent.tenant).to be_nil
      expect(SpecAdminCurrent.admin_id).to be_nil
    end

    it "restores previous values even when the block raises" do
      SpecCurrent.request_id = "before"

      expect { described_class.restore(stored) { raise "boom" } }.to raise_error("boom")
      expect(SpecCurrent.request_id).to eq("before")
    end

    it "just yields when nothing is stored" do
      expect(described_class.restore(nil) { :x }).to eq(:x)
      expect(described_class.restore({}) { :y }).to eq(:y)
    end

    it "warns and skips a class that no longer exists, restoring the rest" do
      warnings = []
      allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }
      seen = nil

      described_class.restore(stored.merge("Gone::Current" => { "x" => 1 })) { seen = SpecAdminCurrent.admin_id }

      expect(seen).to eq(7)
      expect(warnings.grep(/Gone::Current/)).not_to be_empty
    end

    it "drops an attribute the class no longer defines and restores the rest" do
      seen = nil

      described_class.restore("SpecCurrent" => { "request_id" => "r", "removed" => 1 }) { seen = SpecCurrent.request_id }

      expect(seen).to eq("r")
    end

    it "raises DeserializationError when a GlobalID cannot be located (like a job argument)" do
      allow(GlobalID::Locator).to receive(:locate).and_raise(ActiveRecord::RecordNotFound) if defined?(ActiveRecord)
      allow(GlobalID::Locator).to receive(:locate).and_raise(StandardError, "missing") unless defined?(ActiveRecord)

      expect { described_class.restore(stored) { :never } }.to raise_error(ActiveJob::DeserializationError)
    end
  end
end
