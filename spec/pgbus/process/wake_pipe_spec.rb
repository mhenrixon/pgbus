# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::WakePipe do
  subject(:wake_pipe) { described_class.new(reader, wake_signal: wake_signal, logger: logger) }

  let(:reader_writer) { IO.pipe }
  let(:reader) { reader_writer[0] }
  let(:writer) { reader_writer[1] }
  let(:wake_signal) { Pgbus::Process::WakeSignal.new }
  let(:logger) { Logger.new(IO::NULL) }

  after do
    wake_pipe.stop
    [reader, writer].each { |io| io.close unless io.closed? }
  end

  def wait_until(timeout: 2)
    deadline = Time.now + timeout
    until yield
      raise "timed out waiting for condition" if Time.now > deadline

      sleep 0.01
    end
  end

  describe "protocol" do
    it "notifies the wake signal on a W byte" do
      wake_pipe.start
      writer.write(Pgbus::Process::WakePipe::WAKE)

      expect(wake_signal.wait(timeout: 2)).to be true
    end

    it "does not notify the wake signal on status bytes" do
      wake_pipe.start
      writer.write(Pgbus::Process::WakePipe::HEALTHY)
      wait_until { wake_pipe.delivering? }

      expect(wake_signal.pending?).to be false
    end

    it "marks delivering on H" do
      wake_pipe.start
      writer.write(Pgbus::Process::WakePipe::DEGRADED)
      wait_until { !wake_pipe.delivering? }

      writer.write(Pgbus::Process::WakePipe::HEALTHY)
      wait_until { wake_pipe.delivering? }

      expect(wake_pipe.delivering?).to be true
    end

    it "marks not-delivering on P" do
      wake_pipe.start
      writer.write(Pgbus::Process::WakePipe::HEALTHY)
      wait_until { wake_pipe.delivering? }

      writer.write(Pgbus::Process::WakePipe::DEGRADED)
      wait_until { !wake_pipe.delivering? }

      expect(wake_pipe.delivering?).to be false
    end

    it "handles a coalesced burst of bytes in one read" do
      wake_pipe.start
      writer.write(Pgbus::Process::WakePipe::HEALTHY + Pgbus::Process::WakePipe::WAKE + Pgbus::Process::WakePipe::WAKE)

      expect(wake_signal.wait(timeout: 2)).to be true
      expect(wake_pipe.delivering?).to be true
    end
  end

  describe "optimistic default" do
    # Mirrors NotifyListener's optimistic @delivering default: before the hub
    # says anything the pipe assumes wakes flow, so a just-forked worker isn't
    # mistaken for degraded and pinned to fast polling.
    it "starts out delivering" do
      expect(wake_pipe.delivering?).to be true
    end
  end

  describe "supervisor death (EOF)" do
    it "marks not-delivering and stops the watcher when the write end closes" do
      wake_pipe.start
      writer.close

      wait_until { !wake_pipe.delivering? }
      expect(wake_pipe.delivering?).to be false
    end
  end

  describe "#stop" do
    it "joins the watcher thread" do
      wake_pipe.start
      wake_pipe.stop

      expect(wake_pipe.running?).to be false
    end

    it "is safe to call before start" do
      expect { wake_pipe.stop }.not_to raise_error
    end
  end

  describe "#start" do
    it "is idempotent" do
      wake_pipe.start
      expect { wake_pipe.start }.not_to raise_error
      wake_pipe.stop
    end

    it "returns self for chaining" do
      expect(wake_pipe.start).to be(wake_pipe)
    end
  end

  context "when the reader is nil (scope :fork or supervisor never armed it)" do
    subject(:wake_pipe) { described_class.new(nil, wake_signal: wake_signal, logger: logger) }

    it "reports not-delivering" do
      expect(wake_pipe.delivering?).to be false
    end

    it "start is a no-op" do
      expect(wake_pipe.start).to be(wake_pipe)
      expect(wake_pipe.running?).to be false
    end
  end
end
