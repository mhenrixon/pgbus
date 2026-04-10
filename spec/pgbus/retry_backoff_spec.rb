# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::RetryBackoff do
  describe ".compute_delay" do
    context "with global config defaults" do
      it "returns base delay on first retry (attempt=1)" do
        delay = described_class.compute_delay(attempt: 1, jitter: 0)
        expect(delay).to eq(5)
      end

      it "doubles the delay on each subsequent attempt" do
        delays = (1..4).map { |n| described_class.compute_delay(attempt: n, jitter: 0) }
        expect(delays).to eq([5, 10, 20, 40])
      end

      it "caps at retry_backoff_max" do
        delay = described_class.compute_delay(attempt: 10, jitter: 0)
        expect(delay).to eq(Pgbus.configuration.retry_backoff_max)
      end

      it "applies jitter when non-zero" do
        delays = Array.new(100) { described_class.compute_delay(attempt: 1) }
        # With default jitter (0.15), all delays should be within ±15% of base
        base = Pgbus.configuration.retry_backoff
        min_expected = (base * (1 - Pgbus.configuration.retry_backoff_jitter)).floor
        max_expected = (base * (1 + Pgbus.configuration.retry_backoff_jitter)).ceil
        expect(delays).to all(be_between(min_expected, max_expected))
      end

      it "never returns a negative delay" do
        delay = described_class.compute_delay(attempt: 0, jitter: 0)
        expect(delay).to be >= 0
      end
    end

    context "with custom config" do
      it "uses provided base and max" do
        delay = described_class.compute_delay(
          attempt: 3, base: 10, max: 50, jitter: 0
        )
        # 10 * 2^(3-1) = 40, capped at 50
        expect(delay).to eq(40)
      end

      it "caps at custom max" do
        delay = described_class.compute_delay(
          attempt: 4, base: 10, max: 50, jitter: 0
        )
        # 10 * 2^(4-1) = 80, capped at 50
        expect(delay).to eq(50)
      end
    end

    context "with per-job-class override" do
      let(:job_class) do
        klass = Class.new
        klass.extend(Pgbus::RetryBackoff::JobMixin::ClassMethods)
        klass.pgbus_retry_backoff(base: 15, max: 120, jitter: 0)
        klass
      end

      it "reads backoff config from the job class" do
        delay = described_class.compute_delay_for_job(job_class, attempt: 2)
        # 15 * 2^(2-1) = 30
        expect(delay).to eq(30)
      end

      it "caps at the job-level max" do
        delay = described_class.compute_delay_for_job(job_class, attempt: 5)
        # 15 * 2^4 = 240, capped at 120
        expect(delay).to eq(120)
      end
    end

    context "with a job class that has no override" do
      let(:plain_job_class) { Class.new }

      it "falls back to global config" do
        delay = described_class.compute_delay_for_job(plain_job_class, attempt: 1, jitter: 0)
        expect(delay).to eq(Pgbus.configuration.retry_backoff)
      end
    end
  end
end
