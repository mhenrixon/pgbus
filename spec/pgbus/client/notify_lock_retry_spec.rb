# frozen_string_literal: true

require "spec_helper"

# Residual of #403: Postgres deadlocks inside pgmq.enable_notify_insert
# (DROP TRIGGER) while two processes race the first durable stream
# broadcast. 0.16.1 already skips DROP when the trigger is current, but
# the check-then-act still races. getzazu/app#3817 / #3827.
RSpec.describe Pgbus::Client::NotifyLockRetry do
  def ensure_pg_error_class(name)
    stub_const("PG", Module.new) unless defined?(PG)
    return if defined?(PG) && PG.const_defined?(name, false)

    stub_const("PG::#{name}", Class.new(StandardError))
  end

  def deadlock_error
    PGMQ::Errors::ConnectionError.new(
      "Database connection error: ERROR:  deadlock detected\n" \
      "DETAIL:  Process 1 waits for AccessExclusiveLock on relation 3487593"
    )
  end

  def lock_timeout_error
    PGMQ::Errors::ConnectionError.new(
      "Database connection error: ERROR:  canceling statement due to lock timeout"
    )
  end

  def lock_not_available_error
    PGMQ::Errors::ConnectionError.new(
      "Database connection error: ERROR:  lock not available"
    )
  end

  def statement_timeout_lock_error
    PGMQ::Errors::ConnectionError.new(
      "Database connection error: ERROR:  canceling statement due to statement timeout\n" \
      "CONTEXT:  while locking tuple"
    )
  end

  def missing_queue_error
    PGMQ::Errors::ConnectionError.new(
      'Database connection error: ERROR:  Queue "pgbus_entity_x_pl_metrics" ' \
      "does not exist. Create it first using pgmq.create()"
    )
  end

  before do
    real_pgmq_connection_error
    ensure_pg_error_class("TRDeadlockDetected")
    ensure_pg_error_class("LockNotAvailable")
    ensure_pg_error_class("QueryCanceled")
  end

  describe ".retryable?" do
    it "treats a PGMQ ConnectionError whose message is a deadlock as retryable" do
      expect(described_class.retryable?(deadlock_error)).to be(true)
    end

    it "treats lock timeout and lock-not-available ConnectionErrors as retryable" do
      expect(described_class.retryable?(lock_timeout_error)).to be(true)
      expect(described_class.retryable?(lock_not_available_error)).to be(true)
    end

    it "treats statement-timeout lock waits (QueryCanceled) as retryable" do
      # Pooled connections: lock_timeout is often above statement_timeout,
      # so a DROP TRIGGER waiter dies as QueryCanceled, not LockWaitTimeout.
      expect(described_class.retryable?(statement_timeout_lock_error)).to be(true)
      expect(described_class.retryable?(
        PG::QueryCanceled.new(
          "ERROR:  canceling statement due to statement timeout\nCONTEXT:  while locking tuple"
        )
      )).to be(true)
    end

    it "does not treat a bare statement timeout as retryable" do
      expect(described_class.retryable?(
        PGMQ::Errors::ConnectionError.new(
          "Database connection error: ERROR:  canceling statement due to statement timeout"
        )
      )).to be(false)
    end

    it "treats PG::TRDeadlockDetected and PG::LockNotAvailable as retryable" do
      expect(described_class.retryable?(PG::TRDeadlockDetected.new("deadlock detected"))).to be(true)
      expect(described_class.retryable?(PG::LockNotAvailable.new("lock not available"))).to be(true)
    end

    it "treats ActiveRecord::Deadlocked and ActiveRecord::LockWaitTimeout as retryable" do
      expect(described_class.retryable?(ActiveRecord::Deadlocked.new("deadlock detected"))).to be(true)
      expect(described_class.retryable?(ActiveRecord::LockWaitTimeout.new("canceling statement due to lock timeout"))).to be(true)
    end

    it "walks the cause chain of a wrapped ConnectionError" do
      cause = PG::TRDeadlockDetected.new("deadlock detected")
      wrapped = PGMQ::Errors::ConnectionError.new("Database connection error")
      allow(wrapped).to receive(:cause).and_return(cause)

      expect(described_class.retryable?(wrapped)).to be(true)
    end

    it "does not treat a missing-queue ConnectionError as retryable" do
      expect(described_class.retryable?(missing_queue_error)).to be(false)
    end

    it "does not treat a permission ConnectionError as retryable" do
      expect(described_class.retryable?(
        PGMQ::Errors::ConnectionError.new("permission denied for schema pgmq")
      )).to be(false)
    end

    it "does not treat generic StandardError as retryable" do
      expect(described_class.retryable?(StandardError.new("boom"))).to be(false)
    end
  end

  describe "#with_notify_lock_retry" do
    let(:host) do
      Class.new do
        include Pgbus::Client::NotifyLockRetry

        def sleep(*)
        end
      end.new
    end

    before do
      allow(host).to receive(:sleep)
      allow(Pgbus.logger).to receive(:warn)
    end

    def retry_notify
      host.send(:with_notify_lock_retry, "pl_metrics") { yield }
    end

    it "returns the block result when the first attempt succeeds" do
      expect(retry_notify { :broadcasted }).to eq(:broadcasted)
      expect(host).not_to have_received(:sleep)
    end

    it "retries a deadlock and succeeds on the next attempt" do
      attempts = 0

      result = retry_notify do
        attempts += 1
        raise deadlock_error if attempts == 1

        :broadcasted
      end

      expect(result).to eq(:broadcasted)
      expect(attempts).to eq(2)
      expect(host).to have_received(:sleep).with(described_class::DELAYS.first).once
    end

    it "retries lock-timeout, lock-not-available, and statement-timeout lock waits" do
      [lock_timeout_error, lock_not_available_error, statement_timeout_lock_error].each do |error|
        attempts = 0

        result = retry_notify do
          attempts += 1
          raise error if attempts == 1

          :ok
        end

        expect(result).to eq(:ok)
        expect(attempts).to eq(2)
      end
    end

    it "re-raises after the retry budget is exhausted" do
      attempts = 0
      error = deadlock_error

      expect do
        retry_notify do
          attempts += 1
          raise error
        end
      end.to raise_error(PGMQ::Errors::ConnectionError, /deadlock detected/)

      expect(attempts).to eq(described_class::ATTEMPTS)
      expect(host).to have_received(:sleep).exactly(described_class::ATTEMPTS - 1).times
    end

    it "does not retry a missing-queue ConnectionError" do
      attempts = 0

      expect do
        retry_notify do
          attempts += 1
          raise missing_queue_error
        end
      end.to raise_error(PGMQ::Errors::ConnectionError, /does not exist/)

      expect(attempts).to eq(1)
      expect(host).not_to have_received(:sleep)
    end

    it "does not retry a bare statement timeout" do
      attempts = 0

      expect do
        retry_notify do
          attempts += 1
          raise PGMQ::Errors::ConnectionError,
                "Database connection error: ERROR:  canceling statement due to statement timeout"
        end
      end.to raise_error(PGMQ::Errors::ConnectionError, /statement timeout/)

      expect(attempts).to eq(1)
      expect(host).not_to have_received(:sleep)
    end

    it "does not retry an unrelated StandardError" do
      attempts = 0

      expect do
        retry_notify do
          attempts += 1
          raise StandardError, "boom"
        end
      end.to raise_error(StandardError, "boom")

      expect(attempts).to eq(1)
    end

    it "logs the lock failure before retrying" do
      warnings = []
      allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }

      attempts = 0
      retry_notify do
        attempts += 1
        raise deadlock_error if attempts == 1

        :ok
      end

      expect(warnings.first).to match(
        /Retrying stream-queue notify setup after a Postgres lock failure.*attempt 1\/3 stream=pl_metrics/
      )
    end
  end
end
