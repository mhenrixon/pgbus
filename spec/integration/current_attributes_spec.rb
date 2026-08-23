# frozen_string_literal: true

require "integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Current attributes ride the job payload across enqueue → perform (issue #430):
# the pgbus adapter stores what the mixin serialized, the executor deserializes
# it back, and every path that re-sends a payload (retry_on, blocked promotion,
# failed-event retry) keeps it without knowing it is there.
RSpec.describe "Current attributes across enqueue → perform (issue #430)", :integration do
  let(:queue) { "current_attrs_q" }
  let(:seen) { [] }
  let(:current_class) { Class.new(ActiveSupport::CurrentAttributes) { attribute :tenant, :request_id } }
  let(:job_class) do
    name = queue
    sink = seen
    Class.new(ActiveJob::Base) do
      include Pgbus::Concurrency
      include Pgbus::ActiveJob::CurrentAttributes

      self.queue_adapter = :pgbus
      queue_as name
      define_singleton_method(:name) { "CurrentAttrsSpec::Job" }
      define_method(:perform) { |*| sink << [CurrentAttrsSpec::Current.tenant, CurrentAttrsSpec::Current.request_id] }
    end
  end
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: Pgbus.client, config: Pgbus.configuration) }

  def read_one
    Pgbus.client.read_batch(queue, qty: 1, vt: 30).first
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("CurrentAttrsSpec", Module.new)
    stub_const("CurrentAttrsSpec::Current", current_class)
    stub_const("CurrentAttrsSpec::Job", job_class)
    Pgbus.configuration.current_attributes = :auto
    Pgbus.client.ensure_queue(queue)
    Pgbus.client.purge_queue(queue)
  end

  after do
    Pgbus.configuration.current_attributes = nil
    ActiveSupport::CurrentAttributes.clear_all
    Pgbus.client.purge_queue(queue)
  end

  it "persists Current at enqueue and restores it around perform in the executor" do
    CurrentAttrsSpec::Current.tenant = "acme"
    CurrentAttrsSpec::Current.request_id = "r-1"
    job_class.perform_later
    ActiveSupport::CurrentAttributes.clear_all

    message = read_one
    payload = JSON.parse(message.message)
    expect(payload.dig("pgbus_current", "CurrentAttrsSpec::Current", "tenant")).to eq("acme")

    expect(executor.execute(message, queue)).to eq(:success)
    expect(seen).to eq([%w[acme r-1]])
    expect(CurrentAttrsSpec::Current.tenant).to be_nil
  end

  it "keeps the originally captured context on a retry_on re-enqueue" do
    klass = Class.new(job_class) do
      define_singleton_method(:name) { "CurrentAttrsSpec::RetryJob" }
      retry_on ArgumentError, wait: 0, attempts: 3
      define_method(:perform) { |*| raise ArgumentError, "flaky" }
    end
    stub_const("CurrentAttrsSpec::RetryJob", klass)
    CurrentAttrsSpec::Current.tenant = "acme"
    klass.perform_later
    CurrentAttrsSpec::Current.tenant = "someone-else"

    first = read_one
    expect(executor.execute(first, queue)).to eq(:success) # retry_on swallowed the raise and re-enqueued

    retried = Pgbus.client.read_batch(queue, qty: 5, vt: 30).find { |m| m.msg_id != first.msg_id }
    expect(retried).not_to be_nil
    expect(JSON.parse(retried.message).dig("pgbus_current", "CurrentAttrsSpec::Current", "tenant")).to eq("acme")
  end

  it "survives concurrency-blocked promotion" do
    Pgbus::Semaphore.delete_all
    Pgbus::BlockedExecution.delete_all
    klass = Class.new(job_class) do
      define_singleton_method(:name) { "CurrentAttrsSpec::LimitedJob" }
      limits_concurrency to: 1, key: ->(*) { "current-attrs-limited" }, on_conflict: :block
    end
    stub_const("CurrentAttrsSpec::LimitedJob", klass)

    CurrentAttrsSpec::Current.tenant = "first"
    klass.perform_later
    CurrentAttrsSpec::Current.tenant = "second"
    klass.perform_later # blocked — parked in pgbus_blocked_executions with its own context
    ActiveSupport::CurrentAttributes.clear_all

    expect(executor.execute(read_one, queue)).to eq(:success) # completion promotes the blocked job
    promoted = read_one
    expect(promoted).not_to be_nil
    expect(JSON.parse(promoted.message).dig("pgbus_current", "CurrentAttrsSpec::Current", "tenant")).to eq("second")
    expect(executor.execute(promoted, queue)).to eq(:success)
    expect(seen).to eq([["first", nil], ["second", nil]])
  end

  it "tags every payload of perform_all_later" do
    CurrentAttrsSpec::Current.tenant = "bulk"
    ActiveJob.perform_all_later([job_class.new, job_class.new])

    tenants = Pgbus.client.read_batch(queue, qty: 2, vt: 30).map do |m|
      JSON.parse(m.message).dig("pgbus_current", "CurrentAttrsSpec::Current", "tenant")
    end
    expect(tenants).to eq(%w[bulk bulk])
  end

  it "raises at enqueue for an unserializable attribute and accepts it once excluded" do
    klass = Class.new(ActiveSupport::CurrentAttributes) { attribute :request, :tenant }
    stub_const("CurrentAttrsSpec::Other", klass)
    CurrentAttrsSpec::Other.request = Object.new
    CurrentAttrsSpec::Other.tenant = "acme"

    expect { job_class.perform_later }.to raise_error(Pgbus::CurrentAttributesError, /CurrentAttrsSpec::Other#request/)

    Pgbus.configuration.current_attributes = { "CurrentAttrsSpec::Other" => { except: [:request] } }
    expect { job_class.perform_later }.not_to raise_error
    payload = JSON.parse(read_one.message)
    expect(payload.dig("pgbus_current", "CurrentAttrsSpec::Other")).to include("tenant" => "acme")
    expect(payload.dig("pgbus_current", "CurrentAttrsSpec::Other")).not_to have_key("request")
  end
end
