# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Metrics::Backends::Prometheus do
  subject(:backend) { described_class.new }

  describe "#increment" do
    it "accumulates a counter keyed by name and tags" do
      backend.increment("pgbus_messages_sent", 1, { queue: "default" })
      backend.increment("pgbus_messages_sent", 2, { queue: "default" })

      text = backend.render
      expect(text).to include('pgbus_messages_sent{queue="default"} 3')
    end

    it "keeps distinct tag sets on separate series" do
      backend.increment("pgbus_messages_sent", 1, { queue: "a" })
      backend.increment("pgbus_messages_sent", 5, { queue: "b" })

      text = backend.render
      expect(text).to include('pgbus_messages_sent{queue="a"} 1')
      expect(text).to include('pgbus_messages_sent{queue="b"} 5')
    end

    it "renders a bare metric when there are no tags" do
      backend.increment("pgbus_worker_recycled")

      expect(backend.render).to include("pgbus_worker_recycled 1")
    end

    it "emits a # TYPE counter line" do
      backend.increment("pgbus_messages_sent", 1, { queue: "default" })

      expect(backend.render).to include("# TYPE pgbus_messages_sent counter")
    end
  end

  describe "#gauge" do
    it "records the last value and emits a gauge type" do
      backend.gauge("pgbus_queue_depth", 10, { queue: "default" })
      backend.gauge("pgbus_queue_depth", 4, { queue: "default" })

      text = backend.render
      expect(text).to include("# TYPE pgbus_queue_depth gauge")
      expect(text).to include('pgbus_queue_depth{queue="default"} 4')
    end
  end

  describe "#histogram" do
    it "exposes _sum and _count summary series" do
      backend.histogram("pgbus_job_duration_ms", 10, { queue: "default" })
      backend.histogram("pgbus_job_duration_ms", 30, { queue: "default" })

      text = backend.render
      expect(text).to include("# TYPE pgbus_job_duration_ms summary")
      expect(text).to include('pgbus_job_duration_ms_sum{queue="default"} 40')
      expect(text).to include('pgbus_job_duration_ms_count{queue="default"} 2')
    end
  end

  describe "label escaping" do
    it "escapes backslashes, double quotes, and newlines in label values" do
      backend.increment("pgbus_event_count", 1, { handler: %(a"b\\c\nd) })

      expect(backend.render).to include('handler="a\\"b\\\\c\\nd"')
    end
  end

  describe "thread safety" do
    it "does not lose increments under concurrent writers" do
      threads = Array.new(8) do
        Thread.new do
          100.times { backend.increment("pgbus_messages_sent", 1, { queue: "default" }) }
        end
      end
      threads.each(&:join)

      expect(backend.render).to include('pgbus_messages_sent{queue="default"} 800')
    end
  end
end
