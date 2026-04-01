# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Lifecycle do
  subject(:lifecycle) { described_class.new }

  describe "#initialize" do
    it "starts in :starting state" do
      expect(lifecycle.state).to eq(:starting)
      expect(lifecycle).to be_starting
    end
  end

  describe "#transition_to!" do
    it "transitions from starting to running" do
      lifecycle.transition_to!(:running)
      expect(lifecycle).to be_running
    end

    it "transitions from running to draining" do
      lifecycle.transition_to!(:running)
      lifecycle.transition_to!(:draining)
      expect(lifecycle).to be_draining
    end

    it "transitions from running to paused" do
      lifecycle.transition_to!(:running)
      lifecycle.transition_to!(:paused)
      expect(lifecycle).to be_paused
    end

    it "transitions from paused to running" do
      lifecycle.transition_to!(:running)
      lifecycle.transition_to!(:paused)
      lifecycle.transition_to!(:running)
      expect(lifecycle).to be_running
    end

    it "transitions from draining to stopped" do
      lifecycle.transition_to!(:running)
      lifecycle.transition_to!(:draining)
      lifecycle.transition_to!(:stopped)
      expect(lifecycle).to be_stopped
    end

    it "raises InvalidTransition for invalid transitions" do
      expect { lifecycle.transition_to!(:draining) }.to raise_error(Pgbus::Process::InvalidTransition)
    end

    it "raises for unknown states" do
      expect { lifecycle.transition_to!(:exploding) }.to raise_error(ArgumentError, /Unknown state/)
    end

    it "does not allow transitions from stopped" do
      lifecycle.transition_to!(:running)
      lifecycle.transition_to!(:stopped)
      expect { lifecycle.transition_to!(:running) }.to raise_error(Pgbus::Process::InvalidTransition)
    end
  end

  describe "#transition_to (non-raising)" do
    it "returns the new state on success" do
      expect(lifecycle.transition_to(:running)).to eq(:running)
    end

    it "returns false on invalid transition" do
      expect(lifecycle.transition_to(:draining)).to be false
    end
  end

  describe "predicate methods" do
    it "#active? returns true for running and paused" do
      lifecycle.transition_to!(:running)
      expect(lifecycle).to be_active

      lifecycle.transition_to!(:paused)
      expect(lifecycle).to be_active
    end

    it "#can_process? returns true only for running" do
      lifecycle.transition_to!(:running)
      expect(lifecycle).to be_can_process

      lifecycle.transition_to!(:paused)
      expect(lifecycle).not_to be_can_process
    end

    it "#terminal? returns true only for stopped" do
      lifecycle.transition_to!(:running)
      expect(lifecycle).not_to be_terminal

      lifecycle.transition_to!(:stopped)
      expect(lifecycle).to be_terminal
    end
  end

  describe "callbacks" do
    it "fires specific transition callbacks" do
      called = false
      lifecycle.on(:starting_to_running) { called = true }
      lifecycle.transition_to!(:running)
      expect(called).to be true
    end

    it "fires :any callbacks with old and new state" do
      states = []
      lifecycle.on(:any) { |old, new_state| states << [old, new_state] }
      lifecycle.transition_to!(:running)
      lifecycle.transition_to!(:draining)
      expect(states).to eq([%i[starting running], %i[running draining]])
    end

    it "does not fire callbacks on failed transitions" do
      called = false
      lifecycle.on(:starting_to_draining) { called = true }
      lifecycle.transition_to(:draining) # invalid, returns false
      expect(called).to be false
    end
  end
end
