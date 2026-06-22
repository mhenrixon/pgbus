# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Coalescer do
  subject(:coalescer) { described_class.new(scheduler: scheduler, flush: flush) }

  # A controllable scheduler: records scheduled flushes and runs them on
  # demand instead of after a real wall-clock delay. Mirrors the surface
  # the Coalescer needs: schedule(delay_seconds) { ... } -> handle.
  let(:scheduler) do
    Class.new do
      attr_reader :scheduled

      def initialize = (@scheduled = [])

      def schedule(_delay_seconds, &block)
        @scheduled << block
        @scheduled.size # a simple handle
      end

      def run_all
        pending = @scheduled.dup
        @scheduled.clear
        pending.each(&:call)
      end
    end.new
  end

  # Records the final flushed broadcasts.
  let(:flushed) { [] }
  let(:flush) do
    lambda do |stream_name:, target:, payload:, opts:|
      flushed << { stream: stream_name, target: target, payload: payload, opts: opts }
    end
  end

  def submit(stream: "chat", target: "cursor", payload: "x", window_ms: 50, **opts)
    coalescer.submit(stream_name: stream, target: target, payload: payload, window_ms: window_ms, opts: opts)
  end

  it "schedules a single flush for the first submit of a (stream, target)" do
    submit(payload: "a")
    expect(scheduler.scheduled.size).to eq(1)
  end

  it "keeps only the latest payload within the window (last-write-wins)" do
    submit(payload: "a")
    submit(payload: "b")
    submit(payload: "c")

    scheduler.run_all

    expect(flushed.size).to eq(1)
    expect(flushed.first[:payload]).to eq("c")
  end

  it "does NOT schedule a second flush while one is pending for the same key" do
    submit(payload: "a")
    submit(payload: "b")
    expect(scheduler.scheduled.size).to eq(1)
  end

  it "schedules independent flushes for different targets on the same stream" do
    submit(target: "cursor", payload: "a")
    submit(target: "typing", payload: "b")
    expect(scheduler.scheduled.size).to eq(2)

    scheduler.run_all
    expect(flushed.map { |f| [f[:target], f[:payload]] }).to contain_exactly(%w[cursor a], %w[typing b])
  end

  it "schedules independent flushes for the same target on different streams" do
    submit(stream: "room1", payload: "a")
    submit(stream: "room2", payload: "b")
    expect(scheduler.scheduled.size).to eq(2)
  end

  it "allows a new window after the previous one flushed" do
    submit(payload: "a")
    scheduler.run_all
    expect(flushed.map { |f| f[:payload] }).to eq(["a"])

    submit(payload: "b")
    expect(scheduler.scheduled.size).to eq(1)
    scheduler.run_all
    expect(flushed.map { |f| f[:payload] }).to eq(%w[a b])
  end

  it "carries the broadcast opts (visible_to/exclude/durable/event) through to flush" do
    submit(payload: "a", visible_to: :admins, exclude: "c1", event: "reactive")
    scheduler.run_all
    expect(flushed.first[:opts]).to eq(visible_to: :admins, exclude: "c1", event: "reactive")
  end

  it "uses the latest submit's opts on a coalesced flush" do
    submit(payload: "a", event: "first")
    submit(payload: "b", event: "second")
    scheduler.run_all
    expect(flushed.first[:payload]).to eq("b")
    expect(flushed.first[:opts]).to eq(event: "second")
  end

  it "passes the window (ms -> seconds) to the scheduler" do
    allow(scheduler).to receive(:schedule).and_call_original
    submit(window_ms: 250)
    expect(scheduler).to have_received(:schedule).with(0.25)
  end

  it "does not flush a key that received no submit" do
    submit(target: "cursor", payload: "a")
    scheduler.run_all
    expect(flushed.map { |f| f[:target] }).to eq(["cursor"])
  end
end
