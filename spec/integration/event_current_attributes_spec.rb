# frozen_string_literal: true

require "integration_helper"
require "active_job"

# Current attributes ride the event envelope publish → handler (issue #431):
# the publisher captures Current under pgbus_current, PGMQ's topic fan-out
# copies it to every subscriber queue, the consumer's handler pipeline restores
# it around handle, and the outbox relay carries it unchanged.
RSpec.describe "Current attributes publish → handler (issue #431)", :integration do
  let(:queue) { "event_current_q" }
  let(:audit_queue) { "event_current_audit_q" }
  let(:seen) { [] }
  let(:current_class) { Class.new(ActiveSupport::CurrentAttributes) { attribute :tenant, :request_id } }
  let(:handler_class) do
    sink = seen
    Class.new(Pgbus::EventBus::Handler) do
      define_singleton_method(:name) { "EventCurrentSpec::Handler" }
      define_method(:handle) do |event|
        sink << { tenant: EventCurrentSpec::Current.tenant, request_id: EventCurrentSpec::Current.request_id,
                  payload: event.payload, context: event.context }
      end
    end
  end
  let(:audit_class) do
    Class.new(Pgbus::EventBus::Handler) do
      define_singleton_method(:name) { "EventCurrentSpec::AuditHandler" }
      define_method(:handle) { |_event| :noop }
    end
  end
  let(:registry) { Pgbus::EventBus::Registry.instance }
  let(:client) { Pgbus.client }
  let(:consumer) { Pgbus::Process::Consumer.new(topics: ["orders.#"], threads: 1) }

  before do
    stub_const("EventCurrentSpec", Module.new)
    stub_const("EventCurrentSpec::Current", current_class)
    stub_const("EventCurrentSpec::Handler", handler_class)
    stub_const("EventCurrentSpec::AuditHandler", audit_class)
    Pgbus.configuration.current_attributes = ["EventCurrentSpec::Current"]

    registry.clear!
    registry.subscribe("orders.created", handler_class, queue_name: queue)
    registry.subscribe("orders.#", audit_class, queue_name: audit_queue)
    registry.setup_all!
    [queue, audit_queue].each { |q| client.purge_queue(q) }
    consumer.send(:setup_subscriptions)
  end

  after do
    Pgbus.configuration.current_attributes = nil
    ActiveSupport::CurrentAttributes.clear_all
    [queue, audit_queue].each { |q| client.purge_queue(q) }
    registry.clear!
  end

  it "restores the publisher's Current inside handle and reverts it after" do
    EventCurrentSpec::Current.tenant = "acme"
    EventCurrentSpec::Current.request_id = "req-1"
    Pgbus.publish("orders.created", { "order_id" => 7 })
    ActiveSupport::CurrentAttributes.clear_all

    message = client.read_message(queue)
    consumer.send(:handle_message, message, queue)

    expect(seen.size).to eq(1)
    expect(seen.first[:tenant]).to eq("acme")
    expect(seen.first[:request_id]).to eq("req-1")
    expect(seen.first[:payload]).to eq("order_id" => 7)
    expect(seen.first[:context]).to include("EventCurrentSpec::Current" => hash_including("tenant" => "acme"))
    expect(EventCurrentSpec::Current.tenant).to be_nil
  end

  it "fans the context out to every subscriber queue" do
    EventCurrentSpec::Current.tenant = "acme"
    Pgbus.publish("orders.created", { "order_id" => 1 })
    ActiveSupport::CurrentAttributes.clear_all

    [queue, audit_queue].each do |q|
      raw = JSON.parse(client.read_message(q).message)
      expect(raw["pgbus_current"]).to include("EventCurrentSpec::Current" => hash_including("tenant" => "acme"))
      expect(raw["payload"]).to eq("order_id" => 1)
    end
  end

  it "publishes an untagged envelope when nothing is assigned or the feature is off" do
    Pgbus.publish("orders.created", { "n" => 1 })
    Pgbus.configuration.current_attributes = nil
    EventCurrentSpec::Current.tenant = "acme"
    Pgbus.publish("orders.created", { "n" => 2 })
    ActiveSupport::CurrentAttributes.clear_all

    2.times do
      raw = JSON.parse(client.read_message(queue).message)
      expect(raw).not_to have_key("pgbus_current")
    end
  end

  it "carries the context across the outbox hop and coexists with the fair key" do
    Pgbus.configuration.event_fair_share = ->(e) { e.payload["order_id"].to_s }
    EventCurrentSpec::Current.tenant = "outboxed"
    Pgbus::OutboxEntry.transaction do
      Pgbus::Outbox.publish_event("orders.created", { "order_id" => 9 })
    end
    ActiveSupport::CurrentAttributes.clear_all

    expect(Pgbus::Outbox::Poller.new.poll_and_publish).to eq(1)

    message = client.read_message(queue)
    raw = JSON.parse(message.message)
    expect(raw["pgbus_current"]).to include("EventCurrentSpec::Current" => hash_including("tenant" => "outboxed"))
    expect(raw["pgbus_fair_key"]).to eq("9")

    consumer.send(:handle_message, message, queue)
    expect(seen.first[:tenant]).to eq("outboxed")
  ensure
    Pgbus.configuration.event_fair_share = nil
  end
end
