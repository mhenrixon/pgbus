# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::SignalHandler do
  let(:handler_class) do
    Class.new do
      include Pgbus::Process::SignalHandler

      attr_reader :graceful_called, :immediate_called

      def initialize
        @graceful_called = false
        @immediate_called = false
      end

      def graceful_shutdown
        @graceful_called = true
      end

      def immediate_shutdown
        @immediate_called = true
      end
    end
  end

  let(:handler) { handler_class.new }

  after { handler.restore_signals }

  describe "#setup_signals" do
    it "creates a signal_queue" do
      handler.setup_signals
      expect(handler.signal_queue).to be_a(Queue)
    end

    it "stores previous signal handlers for restoration" do
      handler.setup_signals
      previous = handler.instance_variable_get(:@previous_handlers)
      expect(previous.keys).to contain_exactly("INT", "TERM", "QUIT")
    end
  end

  describe "#process_signals" do
    before { handler.setup_signals }

    it "calls graceful_shutdown when INT is in the queue" do
      handler.signal_queue << "INT"
      handler.process_signals
      expect(handler.graceful_called).to be true
    end

    it "calls graceful_shutdown when TERM is in the queue" do
      handler.signal_queue << "TERM"
      handler.process_signals
      expect(handler.graceful_called).to be true
    end

    it "calls immediate_shutdown when QUIT is in the queue" do
      handler.signal_queue << "QUIT"
      handler.process_signals
      expect(handler.immediate_called).to be true
    end

    it "processes multiple signals in order" do
      handler.signal_queue << "INT"
      handler.signal_queue << "QUIT"
      handler.process_signals
      expect(handler.graceful_called).to be true
      expect(handler.immediate_called).to be true
    end

    it "does nothing when the queue is empty" do
      handler.process_signals
      expect(handler.graceful_called).to be false
      expect(handler.immediate_called).to be false
    end
  end

  describe "#restore_signals" do
    it "restores previous signal handlers" do
      handler.setup_signals
      expect { handler.restore_signals }.not_to raise_error
    end

    it "does nothing when setup_signals was never called" do
      expect { handler.restore_signals }.not_to raise_error
    end
  end
end
