# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

RSpec.describe Pgbus::Web::Streamer::MasterHubBoot do
  subject(:boot) do
    described_class.new(
      socket_path: socket_path,
      hub_factory: hub_factory,
      poll_interval: 0.02,
      deadline: 0.5,
      logger: logger
    )
  end

  let(:tmpdir) { Dir.mktmpdir("pgbus-hub-boot") }
  let(:socket_path) { File.join(tmpdir, "hub.sock") }
  let(:logger) { Logger.new(IO::NULL) }
  let(:hub) { instance_double(Pgbus::Web::Streamer::MasterHub, start: nil, stop: nil) }
  let(:factory_calls) { [] }
  let(:hub_factory) do
    lambda do |socket_path:|
      factory_calls << socket_path
      hub
    end
  end

  after do
    boot.stop
    FileUtils.remove_entry(tmpdir) if File.directory?(tmpdir)
  end

  def wait_until(timeout: 2)
    deadline = Time.now + timeout
    until yield
      raise "timed out waiting for condition" if Time.now > deadline

      sleep 0.01
    end
  end

  around do |example|
    original = ENV.fetch("PGBUS_STREAMS_HUB_SOCKET", nil)
    example.run
  ensure
    original ? ENV["PGBUS_STREAMS_HUB_SOCKET"] = original : ENV.delete("PGBUS_STREAMS_HUB_SOCKET")
  end

  describe "#start" do
    it "exports the socket path immediately (workers must inherit it across fork)" do
      allow(boot).to receive(:configuration_ready?).and_return(false)

      boot.start

      expect(ENV.fetch("PGBUS_STREAMS_HUB_SOCKET", nil)).to eq(socket_path)
    end

    it "starts the hub once the configuration becomes ready (post-preload)" do
      ready = false
      allow(boot).to receive(:configuration_ready?) { ready }
      allow(boot).to receive(:master_scope?).and_return(true)

      boot.start
      sleep 0.05
      expect(factory_calls).to be_empty

      ready = true
      wait_until { factory_calls.any? }

      expect(factory_calls).to eq([socket_path])
      expect(hub).to have_received(:start)
    end

    it "gives up quietly after the deadline when configuration never appears" do
      allow(boot).to receive(:configuration_ready?).and_return(false)

      boot.start
      sleep 0.7

      expect(factory_calls).to be_empty
      expect(ENV.fetch("PGBUS_STREAMS_HUB_SOCKET", nil)).to eq(socket_path)
    end

    it "does not start the hub when the resolved scope is :process" do
      allow(boot).to receive_messages(configuration_ready?: true, master_scope?: false)

      boot.start
      sleep 0.1

      expect(factory_calls).to be_empty
    end

    it "logs and survives a hub factory failure (workers fall back)" do
      allow(logger).to receive(:error)
      failing = described_class.new(
        socket_path: socket_path,
        hub_factory: ->(socket_path:) { raise StandardError, "no db" }, # rubocop:disable Lint/UnusedBlockArgument
        poll_interval: 0.02, deadline: 0.5, logger: logger
      )
      allow(failing).to receive_messages(configuration_ready?: true, master_scope?: true)

      failing.start
      sleep 0.2

      expect(logger).to have_received(:error)
      failing.stop
    end
  end

  describe "#stop" do
    it "stops a started hub" do
      allow(boot).to receive_messages(configuration_ready?: true, master_scope?: true)
      boot.start
      wait_until { factory_calls.any? }

      boot.stop

      expect(hub).to have_received(:stop)
    end

    it "cancels a still-waiting poller" do
      allow(boot).to receive(:configuration_ready?).and_return(false)
      boot.start

      boot.stop
      sleep 0.1

      expect(factory_calls).to be_empty
    end
  end
end
