# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Security" do
  describe "GlobalID type validation" do
    let(:gid_uri) { "gid://pgbus-test/Order/42" }
    let(:resolved_object) { double("Order", id: 42) }

    before do
      allow(GlobalID::Locator).to receive(:locate).and_return(resolved_object)
    end

    context "when allowed_global_id_models is nil (default)" do
      before { Pgbus.configuration.allowed_global_id_models = nil }

      it "allows any GlobalID model" do
        result = Pgbus::Serializer.locate_global_id(gid_uri)
        expect(result).to eq(resolved_object)
      end
    end

    context "when allowed_global_id_models is configured" do
      let(:order_class) { Class.new }

      before do
        stub_const("Order", order_class)
        Pgbus.configuration.allowed_global_id_models = [order_class]
      end

      after { Pgbus.configuration.allowed_global_id_models = nil }

      it "allows models in the allowlist" do
        result = Pgbus::Serializer.locate_global_id(gid_uri)
        expect(result).to eq(resolved_object)
      end

      it "rejects models not in the allowlist" do
        secret_class = Class.new
        stub_const("Secret", secret_class)

        expect do
          Pgbus::Serializer.locate_global_id("gid://pgbus-test/Secret/1")
        end.to raise_error(ArgumentError, /not in allowed_global_id_models/)
      end
    end

    it "raises on invalid GlobalID strings" do
      expect do
        Pgbus::Serializer.locate_global_id("not-a-gid")
      end.to raise_error(ArgumentError, /Invalid GlobalID/)
    end
  end

  describe "Configuration queue name validation" do
    it "validates queue names at configuration time" do
      config = Pgbus::Configuration.new
      config.queue_prefix = "pgbus"

      expect(config.queue_name("default")).to eq("pgbus_default")
    end

    it "rejects queue names with special characters" do
      config = Pgbus::Configuration.new
      config.queue_prefix = "pgbus"

      expect { config.queue_name("my;queue") }.to raise_error(ArgumentError)
    end
  end

  describe "Dashboard authentication" do
    it "defaults web_auth to nil" do
      config = Pgbus::Configuration.new
      expect(config.web_auth).to be_nil
    end
  end

  describe "Serialization safety" do
    it "never uses Marshal.load or YAML.load in library code" do
      lib_dir = File.expand_path("../../lib", __dir__)
      ruby_files = Dir.glob(File.join(lib_dir, "**/*.rb"))

      dangerous_patterns = ruby_files.flat_map do |file|
        content = File.read(file)
        lines = content.lines
        lines.each_with_index.filter_map do |line, idx|
          "#{file}:#{idx + 1}: #{line.strip}" if line.match?(/Marshal\.load|YAML\.load[^_]/) && !line.match?(/^\s*#/)
        end
      end

      expect(dangerous_patterns).to be_empty,
                                    "Found unsafe deserialization:\n#{dangerous_patterns.join("\n")}"
    end
  end
end
