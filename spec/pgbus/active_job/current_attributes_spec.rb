# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::ActiveJob::CurrentAttributes do
  let(:current_class) { Class.new(ActiveSupport::CurrentAttributes) { attribute :tenant, :request_id } }
  let(:seen) { [] }
  let(:job_class) do
    sink = seen
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::CurrentAttributes

      self.queue_adapter = :test

      def self.name = "CurrentSpec::Job"

      before_perform { |_job| CurrentSpec.seen << [:before_perform, CurrentSpec::Current.tenant] }

      define_method(:perform) do |*|
        sink << [:perform, CurrentSpec::Current.tenant, CurrentSpec::Current.request_id]
      end
    end
  end

  before do
    stub_const("CurrentSpec", Module.new)
    stub_const("CurrentSpec::Current", current_class)
    stub_const("CurrentSpec::Job", job_class)
    sink = seen
    CurrentSpec.define_singleton_method(:seen) { sink }
    Pgbus.configuration.current_attributes = :auto
  end

  after do
    Pgbus.configuration.current_attributes = nil
    ActiveSupport::CurrentAttributes.clear_all
  end

  describe "#serialize" do
    it "adds pgbus_current when a persisted class has assigned attributes" do
      CurrentSpec::Current.tenant = "acme"

      data = job_class.new.serialize

      expect(data["pgbus_current"]).to eq("CurrentSpec::Current" => { "tenant" => "acme", "_aj_symbol_keys" => ["tenant"] })
    end

    it "omits the key when nothing is assigned" do
      expect(job_class.new.serialize).not_to have_key("pgbus_current")
    end

    it "omits the key when the feature is off" do
      Pgbus.configuration.current_attributes = nil
      CurrentSpec::Current.tenant = "acme"

      expect(job_class.new.serialize).not_to have_key("pgbus_current")
    end

    it "omits the key when the job class opts out" do
      job_class.pgbus_persist_current_attributes = false
      CurrentSpec::Current.tenant = "acme"

      expect(job_class.new.serialize).not_to have_key("pgbus_current")
    end

    it "uses a class-level spec override in place of the config" do
      other = Class.new(ActiveSupport::CurrentAttributes) { attribute :locale }
      stub_const("CurrentSpec::Other", other)
      job_class.pgbus_persist_current_attributes = ["CurrentSpec::Other"]
      CurrentSpec::Current.tenant = "acme"
      CurrentSpec::Other.locale = "sv"

      expect(job_class.new.serialize["pgbus_current"].keys).to eq(["CurrentSpec::Other"])
    end

    it "reuses the context captured at first enqueue when re-serializing a deserialized job (retry)" do
      CurrentSpec::Current.tenant = "acme"
      data = job_class.new.serialize
      job = ActiveJob::Base.deserialize(data)
      CurrentSpec::Current.tenant = "changed"

      expect(job.serialize["pgbus_current"]).to eq(data["pgbus_current"])
    end
  end

  describe "#perform_now" do
    let(:data) do
      CurrentSpec::Current.tenant = "acme"
      CurrentSpec::Current.request_id = "r-1"
      job_class.new.serialize.tap { ActiveSupport::CurrentAttributes.clear_all }
    end

    let(:capturing_adapter) do
      sink = captured
      Class.new do
        def self.name = "CurrentSpec::CapturingAdapter"
        define_method(:enqueue) { |job| sink << job.serialize }
        define_method(:enqueue_at) { |job, _ts| sink << job.serialize }
        def enqueue_after_transaction_commit? = false
      end.new
    end
    let(:captured) { [] }

    it "restores the persisted attributes for perform and before_perform, then puts the previous values back" do
      payload = data
      CurrentSpec::Current.tenant = "outer"

      ActiveJob::Base.deserialize(payload).perform_now

      expect(seen).to include([:before_perform, "acme"], [:perform, "acme", "r-1"])
      expect(CurrentSpec::Current.tenant).to eq("outer")
      expect(CurrentSpec::Current.request_id).to be_nil
    end

    it "restores under ActiveJob::Base.execute (what the :test and :inline adapters use)" do
      ActiveJob::Base.execute(data)

      expect(seen).to include([:perform, "acme", "r-1"])
    end

    it "makes the context visible to retry_on and discard_on blocks" do
      retry_seen = []
      discard_seen = []
      klass = Class.new(job_class) do
        def self.name = "CurrentSpec::FailingJob"
        retry_on(ArgumentError, attempts: 1) { |_job, _e| CurrentSpec.retry_seen << CurrentSpec::Current.tenant }
        discard_on(KeyError) { |_job, _e| CurrentSpec.discard_seen << CurrentSpec::Current.tenant }
      end
      stub_const("CurrentSpec::FailingJob", klass)
      CurrentSpec.define_singleton_method(:retry_seen) { retry_seen }
      CurrentSpec.define_singleton_method(:discard_seen) { discard_seen }

      CurrentSpec::Current.tenant = "acme"
      retry_data = klass.new.serialize
      discard_data = klass.new.serialize
      ActiveSupport::CurrentAttributes.clear_all

      klass.define_method(:perform) { |*| raise ArgumentError }
      ActiveJob::Base.deserialize(retry_data).perform_now
      klass.define_method(:perform) { |*| raise KeyError }
      ActiveJob::Base.deserialize(discard_data).perform_now

      expect(retry_seen).to eq(["acme"])
      expect(discard_seen).to eq(["acme"])
    end

    it "lets a job enqueued from inside perform capture the restored context" do
      child = Class.new(job_class) { def self.name = "CurrentSpec::Child" }
      child.queue_adapter = capturing_adapter
      stub_const("CurrentSpec::Child", child)
      klass = Class.new(job_class) do
        def self.name = "CurrentSpec::Parent"
        define_method(:perform) { |*| CurrentSpec::Child.perform_later }
      end
      stub_const("CurrentSpec::Parent", klass)
      CurrentSpec::Current.tenant = "acme"
      parent_data = klass.new.serialize
      ActiveSupport::CurrentAttributes.clear_all

      ActiveJob::Base.deserialize(parent_data).perform_now

      expect(captured.size).to eq(1)
      expect(captured.first.dig("pgbus_current", "CurrentSpec::Current", "tenant")).to eq("acme")
    end

    it "performs normally when no context was persisted" do
      job_class.new.perform_now

      expect(seen).to eq([[:before_perform, nil], [:perform, nil, nil]])
    end
  end
end
