# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Uniqueness do
  let(:job_lock_class) { stub_const("Pgbus::JobLock", Class.new) }

  before do
    job_lock_class
    allow(Pgbus::JobLock).to receive_messages(acquire!: true, release!: 1, locked?: false)
  end

  describe ".ensures_uniqueness" do
    it "stores uniqueness config on the class" do
      job_class = Class.new do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, key: ->(id) { "test-#{id}" }, lock_ttl: 600
      end

      config = job_class.pgbus_uniqueness
      expect(config[:strategy]).to eq(:until_executed)
      expect(config[:lock_ttl]).to eq(600)
    end

    it "defaults to :until_executed strategy with 24h TTL" do
      job_class = Class.new do
        include Pgbus::Uniqueness

        ensures_uniqueness
      end

      expect(job_class.pgbus_uniqueness[:strategy]).to eq(:until_executed)
      expect(job_class.pgbus_uniqueness[:lock_ttl]).to eq(24 * 60 * 60)
    end

    it "rejects invalid strategies" do
      expect do
        Class.new do
          include Pgbus::Uniqueness

          ensures_uniqueness strategy: :bogus
        end
      end.to raise_error(ArgumentError, /strategy/)
    end

    it "rejects invalid on_conflict" do
      expect do
        Class.new do
          include Pgbus::Uniqueness

          ensures_uniqueness on_conflict: :bogus
        end
      end.to raise_error(ArgumentError, /on_conflict/)
    end
  end

  describe ".resolve_key" do
    it "resolves key from job arguments" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness key: ->(order_id) { "import-#{order_id}" }
      end
      job = job_class.new(42)

      expect(described_class.resolve_key(job)).to eq("import-42")
    end

    it "returns nil for jobs without uniqueness" do
      job_class = Class.new(ActiveJob::Base)
      job = job_class.new
      expect(described_class.resolve_key(job)).to be_nil
    end

    it "uses class name as default key" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness
      end
      stub_const("MyUniqueJob", job_class)
      job = MyUniqueJob.new

      expect(described_class.resolve_key(job)).to eq("MyUniqueJob")
    end
  end

  describe ".inject_metadata" do
    it "adds uniqueness key and strategy to payload" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :while_executing, key: ->(*) { "test-key" }
      end
      job = job_class.new

      result = described_class.inject_metadata(job, { "job_class" => "Test" })
      expect(result[described_class::METADATA_KEY]).to eq("test-key")
      expect(result[described_class::STRATEGY_KEY]).to eq("while_executing")
    end

    it "returns payload unchanged for jobs without uniqueness" do
      job_class = Class.new(ActiveJob::Base)
      job = job_class.new
      payload = { "job_class" => "Test" }

      result = described_class.inject_metadata(job, payload)
      expect(result).to eq(payload)
    end
  end

  describe ".acquire_enqueue_lock" do
    it "acquires lock for :until_executed strategy" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, lock_ttl: 600
      end
      stub_const("LockJob", job_class)
      job = LockJob.new

      result = described_class.acquire_enqueue_lock("test-key", job)
      expect(result).to eq(:acquired)
      expect(Pgbus::JobLock).to have_received(:acquire!).with(
        "test-key", job_class: "LockJob", job_id: job.job_id, state: "queued", ttl: 600
      )
    end

    it "skips lock for :while_executing strategy" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :while_executing
      end
      job = job_class.new

      result = described_class.acquire_enqueue_lock("test-key", job)
      expect(result).to eq(:acquired)
      expect(Pgbus::JobLock).not_to have_received(:acquire!)
    end

    it "returns :locked when lock is already held" do
      allow(Pgbus::JobLock).to receive(:acquire!).and_return(false)

      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed
      end
      job = job_class.new

      result = described_class.acquire_enqueue_lock("test-key", job)
      expect(result).to eq(:locked)
    end
  end

  describe ".release_lock" do
    it "releases the lock" do
      described_class.release_lock("test-key")
      expect(Pgbus::JobLock).to have_received(:release!).with("test-key")
    end

    it "does nothing for nil key" do
      described_class.release_lock(nil)
      expect(Pgbus::JobLock).not_to have_received(:release!)
    end
  end
end
