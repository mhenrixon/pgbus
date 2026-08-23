# frozen_string_literal: true

require "integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Pins the scheduling contract of Client#read_batch_fair (issue #426): a
# weighted, work-conserving interleave across fair-share keys within one
# queue. See Pgbus::Client::FairRead for the rule; these are its canaries.
RSpec.describe "Fair share reads (issue #426)", :integration do
  def tagged(key, n, weight: nil)
    payload = { "n" => n, "pgbus_fair_key" => key }
    payload["pgbus_fair_weight"] = weight if weight
    payload
  end

  def send_tagged(key, count, weight: nil, delay: 0)
    count.times { |i| Pgbus.client.send_message(queue, tagged(key, i, weight: weight), delay: delay) }
  end

  def keys_of(messages)
    messages.map { |m| JSON.parse(m.message)["pgbus_fair_key"] }.tally
  end

  describe "on a plain queue" do
    let(:queue) { "fair_share_q" }

    before do
      Pgbus.client.ensure_queue(queue)
      Pgbus.client.purge_queue(queue)
      Pgbus.client.ensure_fair_index(queue)
    end

    after { Pgbus.client.purge_queue(queue) }

    it "splits a batch equally across keys of equal weight" do
      send_tagged("a", 100)
      send_tagged("b", 20)

      messages = Pgbus.client.read_batch_fair(queue, qty: 10, vt: 30)

      expect(messages.size).to eq(10)
      expect(keys_of(messages)).to eq("a" => 5, "b" => 5)
    end

    it "splits a batch proportionally to weight" do
      send_tagged("heavy", 50, weight: 3)
      send_tagged("light", 50)

      messages = Pgbus.client.read_batch_fair(queue, qty: 8, vt: 30)

      expect(keys_of(messages)).to eq("heavy" => 6, "light" => 2)
    end

    it "is work-conserving: a lone key fills the whole batch" do
      send_tagged("only", 100)

      messages = Pgbus.client.read_batch_fair(queue, qty: 10, vt: 30)

      expect(messages.size).to eq(10)
      expect(keys_of(messages)).to eq("only" => 10)
    end

    it "gives a small key its share immediately even behind a huge backlog of another key" do
      send_tagged("whale", 500)
      send_tagged("minnow", 3)

      messages = Pgbus.client.read_batch_fair(queue, qty: 6, vt: 30)

      expect(keys_of(messages)).to eq("whale" => 3, "minnow" => 3)
    end

    it "groups unkeyed messages as their own key" do
      send_tagged("a", 20)
      5.times { |i| Pgbus.client.send_message(queue, { "n" => i }) }

      messages = Pgbus.client.read_batch_fair(queue, qty: 6, vt: 30)
      tally = messages.map { |m| JSON.parse(m.message)["pgbus_fair_key"] }.tally

      expect(tally).to eq("a" => 3, nil => 3)
    end

    it "reads oldest-visible first within a key" do
      send_tagged("a", 5)

      messages = Pgbus.client.read_batch_fair(queue, qty: 3, vt: 30)

      expect(messages.map { |m| JSON.parse(m.message)["n"] }).to eq([0, 1, 2])
    end

    it "claims the messages it returns: read_ct increments and vt advances" do
      send_tagged("a", 2)

      first = Pgbus.client.read_batch_fair(queue, qty: 2, vt: 30)
      second = Pgbus.client.read_batch_fair(queue, qty: 2, vt: 30)

      expect(first.size).to eq(2)
      expect(first.map { |m| m.read_ct.to_i }).to all(eq(1))
      expect(first).to all(be_a(PGMQ::Message))
      expect(second).to be_empty
    end

    it "skips invisible messages and keys whose messages are all invisible" do
      send_tagged("claimed", 10)
      Pgbus.client.read_batch(queue, qty: 10, vt: 60) # claim every "claimed" message
      send_tagged("delayed", 10, delay: 120)
      send_tagged("ready", 4)

      messages = Pgbus.client.read_batch_fair(queue, qty: 10, vt: 30)

      expect(keys_of(messages)).to eq("ready" => 4)
    end

    it "never hands the same message to two concurrent readers" do
      send_tagged("a", 30)
      send_tagged("b", 30)

      results = Array.new(4) do
        Thread.new { Pgbus.client.read_batch_fair(queue, qty: 10, vt: 30).map { |m| m.msg_id.to_i } }
      end.map(&:value)

      ids = results.flatten
      expect(ids.size).to eq(ids.uniq.size)
      expect(ids.size).to be <= 40
    end

    it "creates the fair index idempotently on an existing populated queue" do
      send_tagged("a", 3)
      Pgbus.client.instance_variable_get(:@fair_indexes_ensured)&.clear
      Pgbus.client.ensure_fair_index(queue)
      Pgbus.client.instance_variable_get(:@fair_indexes_ensured)&.clear
      Pgbus.client.ensure_fair_index(queue)

      index_name = "q_#{Pgbus.configuration.queue_name(queue)}_fair_idx"
      row = ActiveRecord::Base.connection.select_one(<<~SQL)
        SELECT i.indexdef, x.indisvalid
          FROM pg_indexes i
          JOIN pg_index x ON x.indexrelid = (quote_ident(i.schemaname) || '.' || quote_ident(i.indexname))::regclass
         WHERE i.schemaname = 'pgmq' AND i.indexname = '#{index_name}'
      SQL
      expect(row).not_to be_nil
      expect(row["indisvalid"]).to be(true)
      expect(row["indexdef"]).to include("pgbus_fair_key")
    end
  end

  describe "end to end through ActiveJob" do
    let(:queue) { "fair_e2e_q" }
    let(:performed) { [] }
    let(:job_class) do
      name = queue
      sink = performed
      Class.new(ActiveJob::Base) do
        self.queue_adapter = :pgbus
        queue_as name
        define_singleton_method(:name) { "FairShareSpec::Job" }
        define_method(:perform) { |tenant| sink << tenant }
      end
    end

    before do
      ActiveJob::Base.logger = Logger.new(IO::NULL)
      stub_const("FairShareSpec", Module.new)
      stub_const("FairShareSpec::Job", job_class)
      Pgbus.configuration.fair_share = ->(job) { [job.arguments.first, job.arguments.first == "heavy" ? 3 : 1] }
      Pgbus.client.ensure_queue(queue)
      Pgbus.client.purge_queue(queue)
    end

    after do
      Pgbus.configuration.fair_share = nil
      Pgbus.client.purge_queue(queue)
    end

    it "tags enqueued jobs, interleaves them on read, and the executor runs them unchanged" do
      8.times { job_class.perform_later("heavy") }
      4.times { job_class.perform_later("light") }
      ActiveJob.perform_all_later([job_class.new("bulk"), job_class.new("bulk")])

      messages = Pgbus.client.read_batch_fair(queue, qty: 5, vt: 30)
      payloads = messages.map { |m| JSON.parse(m.message) }

      expect(payloads.map { |p| p["pgbus_fair_key"] }.tally).to eq("heavy" => 3, "light" => 1, "bulk" => 1)
      expect(payloads.find { |p| p["pgbus_fair_key"] == "heavy" }["pgbus_fair_weight"]).to eq(3)
      expect(payloads.find { |p| p["pgbus_fair_key"] == "light" }).not_to have_key("pgbus_fair_weight")

      executor = Pgbus::ActiveJob::Executor.new(client: Pgbus.client, config: Pgbus.configuration)
      results = messages.map { |m| executor.execute(m, queue) }

      expect(results).to all(eq(:success))
      expect(performed.tally).to eq("heavy" => 3, "light" => 1, "bulk" => 1)
    end
  end

  describe "with priority_levels" do
    let(:queue) { "fair_prio_q" }

    around do |example|
      Pgbus.configuration.priority_levels = 2
      Pgbus.configuration.fair_share = ->(_job) { "unused-here" }
      Pgbus.reset_client!
      example.run
    ensure
      Pgbus.configuration.priority_levels = nil
      Pgbus.configuration.fair_share = nil
      Pgbus.reset_client!
    end

    before do
      Pgbus.client.ensure_queue(queue)
      Pgbus.client.physical_queue_names(queue).each { |pq| Pgbus.client.purge_queue(pq, prefixed: false) }
    end

    after { Pgbus.client.physical_queue_names(queue).each { |pq| Pgbus.client.purge_queue(pq, prefixed: false) } }

    it "drains p0 before p1 and is fair within each level" do
      4.times { |i| Pgbus.client.send_message(queue, tagged("a", i), priority: 0) }
      4.times { |i| Pgbus.client.send_message(queue, tagged("b", i), priority: 0) }
      4.times { |i| Pgbus.client.send_message(queue, tagged("c", i), priority: 1) }

      pairs = Pgbus.client.read_batch_prioritized(queue, qty: 6, vt: 30)
      p0 = Pgbus.configuration.priority_queue_name(queue, 0)
      p1 = Pgbus.configuration.priority_queue_name(queue, 1)

      expect(pairs.map(&:first).uniq).to eq([p0])
      expect(keys_of(pairs.map(&:last))).to eq("a" => 3, "b" => 3)

      rest = Pgbus.client.read_batch_prioritized(queue, qty: 6, vt: 30)
      expect(rest.map(&:first).uniq).to eq([p0, p1])
      expect(rest.size).to eq(6)
    end
  end
end
