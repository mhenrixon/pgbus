# frozen_string_literal: true

require "spec_helper"
require "active_job"

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
        end.to raise_error(Pgbus::SerializationError, /not in allowed_global_id_models/)
      end
    end

    context "when allowed_global_id_models is an empty array" do
      before { Pgbus.configuration.allowed_global_id_models = [] }
      after { Pgbus.configuration.allowed_global_id_models = nil }

      it "rejects all GlobalID models with a clear message" do
        expect do
          Pgbus::Serializer.locate_global_id(gid_uri)
        end.to raise_error(Pgbus::SerializationError, /deserialization is disabled/)
      end
    end

    context "when allowed_global_id_models contains non-Class values" do
      before { Pgbus.configuration.allowed_global_id_models = ["Order"] }
      after { Pgbus.configuration.allowed_global_id_models = nil }

      it "raises with a clear type error" do
        expect do
          Pgbus::Serializer.locate_global_id(gid_uri)
        end.to raise_error(Pgbus::SerializationError, %r{must contain Class/Module objects})
      end
    end

    it "raises on invalid GlobalID strings" do
      expect do
        Pgbus::Serializer.locate_global_id("not-a-gid")
      end.to raise_error(Pgbus::SerializationError, /Invalid GlobalID/)
    end
  end

  # Issue #368: the allowlist only applied to EventBus `_global_id` payloads.
  # Job arguments use ActiveJob's `_aj_globalid` encoding and went through
  # Rails' unrestricted GlobalID::Locator. Both paths must share the gate.
  describe "GlobalID type validation on ActiveJob arguments (issue #368)" do
    let(:order_class) { Class.new }
    let(:job_data) do
      {
        "job_class" => "TestJob",
        "job_id" => "j-1",
        "queue_name" => "default",
        "arguments" => [{ "_aj_globalid" => "gid://pgbus-test/Order/42" }]
      }
    end

    before do
      stub_const("Order", order_class)
      allow(GlobalID::Locator).to receive(:locate).and_return(double("Order"))
      fake_job = double("job", perform_now: true)
      allow(ActiveJob::Base).to receive(:deserialize).and_return(fake_job)
    end

    after { Pgbus.configuration.allowed_global_id_models = nil }

    context "when allowed_global_id_models is nil (allow-all)" do
      before { Pgbus.configuration.allowed_global_id_models = nil }

      it "deserializes job arguments without an allowlist walk" do
        expect { Pgbus::Serializer.deserialize_job_data(job_data) }.not_to raise_error
        expect(ActiveJob::Base).to have_received(:deserialize).with(job_data)
      end
    end

    context "when allowed_global_id_models is configured" do
      before { Pgbus.configuration.allowed_global_id_models = [order_class] }

      it "allows job arguments whose GlobalID model is on the allowlist" do
        expect { Pgbus::Serializer.deserialize_job_data(job_data) }.not_to raise_error
      end

      it "rejects job arguments whose GlobalID model is not on the allowlist" do
        secret_class = Class.new
        stub_const("Secret", secret_class)
        job_data["arguments"] = [{ "_aj_globalid" => "gid://pgbus-test/Secret/1" }]

        expect do
          Pgbus::Serializer.deserialize_job_data(job_data)
        end.to raise_error(Pgbus::SerializationError, /not in allowed_global_id_models/)
        expect(ActiveJob::Base).not_to have_received(:deserialize)
      end

      it "rejects nested GlobalID arguments in arrays and hashes" do
        secret_class = Class.new
        stub_const("Secret", secret_class)
        job_data["arguments"] = [
          {
            "records" => [
              { "_aj_globalid" => "gid://pgbus-test/Secret/9" }
            ]
          }
        ]

        expect do
          Pgbus::Serializer.deserialize_job_data(job_data)
        end.to raise_error(Pgbus::SerializationError, /not in allowed_global_id_models/)
      end
    end

    context "when allowed_global_id_models is an empty array (deny-all)" do
      before { Pgbus.configuration.allowed_global_id_models = [] }

      it "rejects every job GlobalID argument" do
        expect do
          Pgbus::Serializer.deserialize_job_data(job_data)
        end.to raise_error(Pgbus::SerializationError, /deserialization is disabled/)
      end
    end

    context "when an injected configuration differs from Pgbus.configuration" do
      it "enforces the injected allowlist, not the global one" do
        # Global is allow-all; injected config denies everything.
        Pgbus.configuration.allowed_global_id_models = nil
        injected = Pgbus::Configuration.new
        injected.allowed_global_id_models = []

        expect do
          Pgbus::Serializer.deserialize_job_data(job_data, configuration: injected)
        end.to raise_error(Pgbus::SerializationError, /deserialization is disabled/)
        expect(ActiveJob::Base).not_to have_received(:deserialize)
      end
    end
  end

  describe "Configuration queue name validation" do
    it "validates queue names at configuration time" do
      config = Pgbus::Configuration.new
      config.queue_prefix = "pgbus"

      expect(config.queue_name("default")).to eq("pgbus_default")
    end

    it "normalizes queue names with special characters instead of rejecting" do
      config = Pgbus::Configuration.new
      config.queue_prefix = "pgbus"

      # Semicolons are stripped, hyphens/dots become underscores — safe for SQL
      expect(config.queue_name("my;queue")).to eq("pgbus_myqueue")
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
