# frozen_string_literal: true

require "spec_helper"

# Unit coverage for DataSource#drained_queue_names — the set of physical queue
# names some configured worker capsule or EventBus handler will drain. The
# HealthAnalyzer intersects the STALLED backlog with this set so it never flags
# a queue nobody is configured to drain as a "silent worker wedge" (issue #367).
RSpec.describe Pgbus::Web::DataSource do
  subject(:data_source) { described_class.new(client: mock_client) }

  let(:mock_client) { build_mock_client }

  around do |example|
    original = Pgbus.configuration
    Pgbus.instance_variable_set(:@configuration, Pgbus::Configuration.new)
    example.run
  ensure
    Pgbus.instance_variable_set(:@configuration, original)
  end

  before do
    Pgbus.configuration.queue_prefix = "pgbus"
    # No EventBus handlers unless a test adds them.
    allow(data_source).to receive(:handler_queue_physical_names).and_return([])
    # Non-priority default: a logical queue maps to one prefixed physical table.
    allow(mock_client).to receive(:physical_queue_names) { |logical| ["pgbus_#{logical}"] }
  end

  describe "#drained_queue_names" do
    it "returns nil (drains everything) when a capsule uses the wildcard" do
      Pgbus.configuration.workers = [{ queues: %w[*], threads: 5 }]

      expect(data_source.drained_queue_names).to be_nil
    end

    it "returns the physical names of explicitly-configured capsule queues" do
      Pgbus.configuration.workers = [{ queues: %w[critical default], threads: 5 }]

      expect(data_source.drained_queue_names)
        .to eq(Set["pgbus_critical", "pgbus_default"])
    end

    it "expands a logical queue to its priority _pN sub-tables (priority mode)" do
      Pgbus.configuration.workers = [{ queues: %w[critical], threads: 5 }]
      # In priority mode the client's strategy expands "critical" to the physical
      # tables that actually exist — the bare pgbus_critical is never created.
      allow(mock_client).to receive(:physical_queue_names)
        .with("critical").and_return(%w[pgbus_critical_p0 pgbus_critical_p1])

      expect(data_source.drained_queue_names)
        .to eq(Set["pgbus_critical_p0", "pgbus_critical_p1"])
    end

    it "unions in EventBus handler queues (drained by consumer processes)" do
      Pgbus.configuration.workers = [{ queues: %w[default], threads: 5 }]
      allow(data_source).to receive(:handler_queue_physical_names)
        .and_return(["pgbus_task_completion_handler"])

      expect(data_source.drained_queue_names)
        .to eq(Set["pgbus_default", "pgbus_task_completion_handler"])
    end

    it "returns an empty set when nothing is configured to drain" do
      Pgbus.configuration.workers = nil

      expect(data_source.drained_queue_names).to eq(Set.new)
    end

    it "still returns nil when one capsule is explicit and another is a wildcard" do
      Pgbus.configuration.workers = [
        { queues: %w[critical], threads: 5 },
        { queues: %w[*], threads: 3 }
      ]

      expect(data_source.drained_queue_names).to be_nil
    end

    it "fails open to nil (never raises) when a capsule queue name is malformed" do
      Pgbus.configuration.workers = [{ queues: %w[critical], threads: 5 }]
      allow(mock_client).to receive(:physical_queue_names)
        .and_raise(ArgumentError, "Queue name cannot be blank")
      allow(Pgbus.logger).to receive(:debug)

      expect { data_source.drained_queue_names }.not_to raise_error
      expect(data_source.drained_queue_names).to be_nil
    end
  end
end
