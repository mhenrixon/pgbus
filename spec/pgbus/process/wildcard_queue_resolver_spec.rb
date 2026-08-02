# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::WildcardQueueResolver do
  subject(:resolved) { described_class.resolve(config: config) }

  let(:config) do
    Pgbus::Configuration.new.tap { |c| c.queue_prefix = "pgbus_test" }
  end
  let(:conn) { double("connection") }
  let(:registry) { instance_double(Pgbus::EventBus::Registry, event_queue_names: Set.new) }

  before do
    allow(ActiveRecord::Base).to receive(:connection).and_return(conn)
    allow(Pgbus::StreamQueue).to receive_messages(reset_cache!: nil, known_names: Set.new)
    allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(registry)
    allow(conn).to receive(:select_values).and_return([])
  end

  it "returns pgmq.meta queues stripped of the configured prefix" do
    allow(conn).to receive(:select_values).and_return(%w[pgbus_test_default pgbus_test_mailers])

    expect(resolved).to eq(%w[default mailers])
  end

  it "excludes dead-letter queues" do
    allow(conn).to receive(:select_values)
      .and_return(%W[pgbus_test_default pgbus_test_default#{Pgbus::DEAD_LETTER_SUFFIX}])

    expect(resolved).to eq(%w[default])
  end

  it "excludes registered stream queues" do
    allow(Pgbus::StreamQueue).to receive(:known_names).and_return(Set.new(%w[pgbus_test_chat_42]))
    allow(conn).to receive(:select_values).and_return(%w[pgbus_test_default pgbus_test_chat_42])

    expect(resolved).to eq(%w[default])
  end

  it "resets the stream cache before reading known names, so fresh streams are excluded" do
    resolved

    expect(Pgbus::StreamQueue).to have_received(:reset_cache!).ordered
    expect(Pgbus::StreamQueue).to have_received(:known_names).ordered
  end

  it "excludes registered event-subscriber queues" do
    allow(registry).to receive(:event_queue_names).and_return(Set.new(%w[pgbus_test_orders_handler]))
    allow(conn).to receive(:select_values).and_return(%w[pgbus_test_default pgbus_test_orders_handler])

    expect(resolved).to eq(%w[default])
  end

  it "returns an empty array when nothing matches" do
    expect(resolved).to eq([])
  end

  context "when the config routes through a separate database (connects_to)" do
    let(:bus_conn) { double("bus connection") }

    before do
      config.connects_to = { database: { writing: :pgbus } }
      allow(Pgbus::BusRecord).to receive(:connection).and_return(bus_conn)
      allow(bus_conn).to receive(:select_values).and_return(%w[pgbus_test_default])
    end

    it "reads pgmq.meta over the BusRecord connection" do
      expect(resolved).to eq(%w[default])
      expect(bus_conn).to have_received(:select_values)
    end
  end
end
