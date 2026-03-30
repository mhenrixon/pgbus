# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Concurrency do
  let(:test_job_class) do
    Class.new(ActiveJob::Base) do
      include Pgbus::Concurrency

      self.queue_adapter = :test

      limits_concurrency to: 2,
                         key: ->(user_id) { "TestJob-#{user_id}" },
                         duration: 900,
                         on_conflict: :discard

      def perform(user_id); end
    end
  end

  let(:no_concurrency_job_class) do
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :test
      def perform; end
    end
  end

  describe ".limits_concurrency" do
    it "stores concurrency config on the class" do
      config = test_job_class.pgbus_concurrency
      expect(config[:limit]).to eq(2)
      expect(config[:duration]).to eq(900)
      expect(config[:on_conflict]).to eq(:discard)
      expect(config[:key]).to be_a(Proc)
    end

    it "validates to: is a positive integer" do
      expect do
        Class.new(ActiveJob::Base) do
          include Pgbus::Concurrency
          limits_concurrency to: 0, key: -> { "x" }
        end
      end.to raise_error(ArgumentError, /positive integer/)
    end

    it "validates on_conflict is a known strategy" do
      expect do
        Class.new(ActiveJob::Base) do
          include Pgbus::Concurrency
          limits_concurrency to: 1, on_conflict: :unknown
        end
      end.to raise_error(ArgumentError, /on_conflict/)
    end

    it "defaults key to class name" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Concurrency
        limits_concurrency to: 1

        def perform; end
      end

      config = job_class.pgbus_concurrency
      expect(config[:key].call).to eq(job_class.name)
    end
  end

  describe ".resolve_key" do
    it "resolves the concurrency key from job arguments" do
      job = test_job_class.new(42)
      expect(described_class.resolve_key(job)).to eq("TestJob-42")
    end

    it "returns nil for jobs without concurrency" do
      job = no_concurrency_job_class.new
      expect(described_class.resolve_key(job)).to be_nil
    end
  end

  describe ".inject_metadata" do
    it "adds concurrency key to payload hash" do
      job = test_job_class.new(42)
      payload = { "job_class" => "TestJob", "arguments" => [42] }
      result = described_class.inject_metadata(job, payload)
      expect(result["pgbus_concurrency_key"]).to eq("TestJob-42")
    end

    it "returns original payload for jobs without concurrency" do
      job = no_concurrency_job_class.new
      payload = { "job_class" => "NoConcurrency" }
      result = described_class.inject_metadata(job, payload)
      expect(result).not_to have_key("pgbus_concurrency_key")
    end
  end

  describe ".extract_key" do
    it "extracts the concurrency key from a payload" do
      payload = { "pgbus_concurrency_key" => "TestJob-42" }
      expect(described_class.extract_key(payload)).to eq("TestJob-42")
    end

    it "returns nil when no concurrency key present" do
      expect(described_class.extract_key({})).to be_nil
    end
  end
end
