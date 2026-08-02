# frozen_string_literal: true

require "integration_helper"

# Pins the strict-priority contract of multi-queue reads (issue #381 item 3).
#
# The capsule DSL documents "list order = strict priority", but the default
# multi-queue path (Worker#fetch_multi → Client#read_multi) is a single
# UNION ALL of pgmq.read() subqueries with an outer LIMIT — the ordering
# holds because Postgres's Append node fills the LIMIT from the subqueries
# in the order they are written. Nothing in pgmq-ruby or Postgres promises
# that. THIS SPEC IS THE CANARY: if a planner change or a pgmq-ruby
# refactor ever breaks Append-order filling, this fails, and fetch_multi
# must switch to ordered per-queue reads (the fetch_prioritized pattern).
#
# Known side effect, documented on Client#read_multi: subqueries that
# execute claim messages (set vt) even when the outer LIMIT discards their
# rows — those messages go invisible for one visibility timeout without
# being processed. Deliberately NOT asserted here (it is planner-dependent
# behavior, not a contract).
RSpec.describe "Multi-queue read priority contract (issue #381)", :integration do
  let(:queues) { %w[prio_a prio_b prio_c] }

  before do
    queues.each do |q|
      Pgbus.client.ensure_queue(q)
      Pgbus.client.purge_queue(q)
      3.times { |i| Pgbus.client.send_message(q, { "q" => q, "n" => i }) }
    end
  end

  after do
    queues.each { |q| Pgbus.client.purge_queue(q) }
  end

  it "fills a limited read_multi from earlier-listed queues first" do
    messages = Pgbus.client.read_multi(queues, qty: 5, limit: 5, vt: 30)

    expect(messages.size).to eq(5)
    by_queue = messages.group_by(&:queue_name)

    # All of the first-listed queue's messages win; the last-listed queue
    # contributes nothing at this limit. Expected keys through
    # config.queue_name so the physical (normalized) names are compared.
    expect(by_queue.fetch(Pgbus.configuration.queue_name("prio_a"), []).size).to eq(3)
    expect(by_queue.fetch(Pgbus.configuration.queue_name("prio_b"), []).size).to eq(2)
    expect(by_queue.fetch(Pgbus.configuration.queue_name("prio_c"), [])).to be_empty
  end

  it "honors a reversed list order" do
    messages = Pgbus.client.read_multi(queues.reverse, qty: 4, limit: 4, vt: 30)

    by_queue = messages.group_by(&:queue_name)

    expect(by_queue.fetch(Pgbus.configuration.queue_name("prio_c"), []).size).to eq(3)
    expect(by_queue.fetch(Pgbus.configuration.queue_name("prio_b"), []).size).to eq(1)
    expect(by_queue.fetch(Pgbus.configuration.queue_name("prio_a"), [])).to be_empty
  end
end
