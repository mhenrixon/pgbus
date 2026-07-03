# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client::ConnectionHealth do
  # Deterministic monotonic clock. The latch reads time via an injected
  # `clock` proc so backoff windows can be advanced without real sleeping.
  # `clock_at` is a mutable box the test moves forward with `advance`.
  subject(:health) { described_class.new(clock: -> { clock_at[0] }) }

  let(:clock_at) { [1000.0] }

  before do
    stub_const("PGMQ::Errors", Module.new)
    stub_const("PGMQ::Errors::ConnectionError", Class.new(StandardError))
  end

  # Advance the injected monotonic clock by `seconds`.
  def advance(seconds)
    clock_at[0] += seconds
  end

  # A block that raises a ConnectionError, simulating a dead database.
  def failing_op
    -> { raise PGMQ::Errors::ConnectionError, "no connection to the server" }
  end

  # Run a failing op, swallowing the expected error. Used to drive the latch
  # through failures when the raise itself isn't under assertion. Tolerates the
  # circuit error too, since once the breaker opens a call fails fast with it.
  def fail_quietly(latch = health)
    latch.run_guarded(&failing_op)
  rescue PGMQ::Errors::ConnectionError, Pgbus::ConnectionCircuitOpenError
    nil
  end

  # Drive the latch to the open state via OPEN_THRESHOLD consecutive failures.
  def trip_open!
    described_class::OPEN_THRESHOLD.times do
      expect { health.run_guarded(&failing_op) }.to raise_error(PGMQ::Errors::ConnectionError)
    end
  end

  describe "#run_guarded" do
    context "when closed (healthy)" do
      it "yields and returns the block's value" do
        expect(health.run_guarded { :result }).to eq(:result)
      end

      it "reports the closed state" do
        expect(health).to be_closed
        expect(health).not_to be_open
      end

      it "does not count non-ConnectionError failures toward the threshold" do
        described_class::OPEN_THRESHOLD.times do
          expect { health.run_guarded { raise ArgumentError, "boom" } }.to raise_error(ArgumentError)
        end

        expect(health).to be_closed
      end

      it "resets the consecutive-failure count on a success" do
        (described_class::OPEN_THRESHOLD - 1).times do
          expect { health.run_guarded(&failing_op) }.to raise_error(PGMQ::Errors::ConnectionError)
        end
        health.run_guarded { :ok }

        # One more failure must NOT trip it — the counter reset.
        expect { health.run_guarded(&failing_op) }.to raise_error(PGMQ::Errors::ConnectionError)
        expect(health).to be_closed
      end
    end

    context "when opening" do
      it "opens after OPEN_THRESHOLD consecutive ConnectionErrors" do
        trip_open!
        expect(health).to be_open
      end

      it "re-raises the underlying ConnectionError on the tripping call (does not swallow)" do
        (described_class::OPEN_THRESHOLD - 1).times { fail_quietly }
        expect { health.run_guarded(&failing_op) }.to raise_error(PGMQ::Errors::ConnectionError)
      end
    end

    context "when open" do
      before { trip_open! }

      it "fails fast with ConnectionCircuitOpenError without yielding" do
        yielded = false
        expect { health.run_guarded { yielded = true } }.to raise_error(Pgbus::ConnectionCircuitOpenError)
        expect(yielded).to be(false)
      end

      it "stays open until the backoff window elapses" do
        advance(described_class::BASE_BACKOFF - 0.001)
        expect { health.run_guarded { :x } }.to raise_error(Pgbus::ConnectionCircuitOpenError)
      end
    end

    context "with a half-open probe (after backoff)" do
      before { trip_open! }

      it "admits a probe once the backoff window elapses" do
        advance(described_class::BASE_BACKOFF)

        expect(health.run_guarded { :probe_ok }).to eq(:probe_ok)
      end

      it "closes and resets on a successful probe" do
        advance(described_class::BASE_BACKOFF)
        health.run_guarded { :probe_ok }

        expect(health).to be_closed
      end

      it "re-opens with doubled backoff on a failed probe" do
        advance(described_class::BASE_BACKOFF)
        expect { health.run_guarded(&failing_op) }.to raise_error(PGMQ::Errors::ConnectionError)
        expect(health).to be_open

        # After only the FIRST backoff window, still open (needs the DOUBLED window).
        advance(described_class::BASE_BACKOFF)
        expect { health.run_guarded { :x } }.to raise_error(Pgbus::ConnectionCircuitOpenError)

        # After the doubled window (total 2x from re-open), a probe is admitted.
        advance(described_class::BASE_BACKOFF)
        expect(health.run_guarded { :probe_ok }).to eq(:probe_ok)
      end
    end

    # Regression: a probe that raises a NON-ConnectionError (e.g.
    # Pgbus::ReadTimeoutError from a hung socket — the read paths wrap the
    # timeout INSIDE the guard) must re-open the breaker, not leave it wedged
    # in half-open forever. (issue #197 adversarial review, BUG 1)
    context "when a half-open probe raises a non-ConnectionError" do
      before { trip_open! }

      it "re-opens the breaker instead of wedging in half-open" do
        advance(described_class::BASE_BACKOFF)
        expect { health.run_guarded { raise "hung socket" } }.to raise_error("hung socket")

        expect(health).to be_open
      end

      it "still admits a probe after the (doubled) backoff window" do
        advance(described_class::BASE_BACKOFF)
        begin
          health.run_guarded { raise "hung socket" }
        rescue RuntimeError
          nil
        end

        # Advancing far past any window must eventually admit a probe — proving
        # the latch is not permanently wedged.
        advance(described_class::MAX_BACKOFF * 2)
        expect(health.run_guarded { :recovered }).to eq(:recovered)
        expect(health).to be_closed
      end
    end

    # Regression: a slow read admitted while CLOSED can complete AFTER other
    # threads have tripped the breaker. Its success must not force the breaker
    # back closed and wipe a freshly-opened state. (issue #197 review, BUG 2)
    context "when a straggler success returns after the breaker tripped" do
      it "does not re-close a breaker opened by other operations" do
        # Simulate: this call was admitted while closed (:run), the breaker
        # tripped open in the meantime, then this call's block succeeds.
        trip_open!
        expect(health).to be_open

        # A stale :run success arriving now must leave the open breaker alone.
        health.send(:record_success, :run)

        expect(health).to be_open
      end
    end

    context "with backoff doubling and cap" do
      before { trip_open! }

      it "doubles the backoff on each successive re-open, capped at MAX_BACKOFF" do
        expected = described_class::BASE_BACKOFF
        12.times do
          advance(expected)
          expect { health.run_guarded(&failing_op) }.to raise_error(PGMQ::Errors::ConnectionError)
          expected = [expected * 2, described_class::MAX_BACKOFF].min
        end

        expect(expected).to eq(described_class::MAX_BACKOFF)
        # The window is now MAX_BACKOFF: just short stays open, exactly MAX
        # admits a probe.
        advance(described_class::MAX_BACKOFF - 1)
        expect { health.run_guarded { :x } }.to raise_error(Pgbus::ConnectionCircuitOpenError)
        advance(1)
        expect(health.run_guarded { :probe_ok }).to eq(:probe_ok)
      end
    end

    context "with state-transition callbacks" do
      it "invokes on_open once when the breaker trips" do
        opens = []
        h = described_class.new(clock: -> { clock_at[0] }, on_open: ->(backoff) { opens << backoff })
        described_class::OPEN_THRESHOLD.times { fail_quietly(h) }
        # An extra gated attempt while already open must NOT re-fire on_open.
        begin
          h.run_guarded { :x }
        rescue Pgbus::ConnectionCircuitOpenError
          nil
        end

        expect(opens).to eq([described_class::BASE_BACKOFF])
      end

      it "invokes on_close once when a probe succeeds" do
        closes = 0
        h = described_class.new(clock: -> { clock_at[0] }, on_close: -> { closes += 1 })
        described_class::OPEN_THRESHOLD.times { fail_quietly(h) }
        clock_at[0] += described_class::BASE_BACKOFF
        h.run_guarded { :ok }

        expect(closes).to eq(1)
      end
    end
  end

  describe "thread safety" do
    it "admits only ONE probe when many threads race in half-open" do
      trip_open!
      advance(described_class::BASE_BACKOFF)

      probe_count = Concurrent::AtomicFixnum.new(0)
      open_errors = Concurrent::AtomicFixnum.new(0)

      threads = Array.new(20) do
        Thread.new do
          health.run_guarded do
            probe_count.increment
            sleep 0.02 # hold the probe long enough for the race to matter
            :ok
          end
        rescue Pgbus::ConnectionCircuitOpenError
          open_errors.increment
        end
      end
      threads.each(&:join)

      expect(probe_count.value).to eq(1)
      expect(open_errors.value).to eq(19)
    end

    it "is safe under concurrent failures without corrupting state" do
      threads = Array.new(10) do
        Thread.new do
          10.times { fail_quietly }
        end
      end
      threads.each(&:join)

      # After a storm of failures the latch is deterministically open, not
      # wedged in a corrupt in-between state.
      expect(health).to be_open
    end
  end
end
