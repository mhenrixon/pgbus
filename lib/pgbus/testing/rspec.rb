# frozen_string_literal: true

require_relative "../testing"

RSpec::Matchers.define :have_published_event do |expected_routing_key|
  supports_block_expectations

  chain(:with_payload) { |payload| @expected_payload = payload }
  chain(:with_headers) { |headers| @expected_headers = headers }
  chain(:exactly)      { |count|   @expected_count = count }

  match do |block|
    @before = Pgbus::Testing.store.events(routing_key: expected_routing_key).dup
    block.call
    @after = Pgbus::Testing.store.events(routing_key: expected_routing_key)
    @new_events = @after - @before

    return false if @new_events.empty?

    if @expected_payload
      @new_events = @new_events.select { |e| values_match?(@expected_payload, e.payload) }
      return false if @new_events.empty?
    end

    if @expected_headers
      @new_events = @new_events.select { |e| values_match?(@expected_headers, e.headers) }
      return false if @new_events.empty?
    end

    return false if @expected_count && @new_events.size != @expected_count

    true
  end

  match_when_negated do |block|
    @before = Pgbus::Testing.store.events(routing_key: expected_routing_key).dup
    block.call
    @after = Pgbus::Testing.store.events(routing_key: expected_routing_key)
    @new_events = @after - @before

    @new_events = @new_events.select { |e| values_match?(@expected_payload, e.payload) } if @expected_payload

    @new_events = @new_events.select { |e| values_match?(@expected_headers, e.headers) } if @expected_headers

    if @expected_count
      @new_events.size != @expected_count
    else
      @new_events.empty?
    end
  end

  failure_message do
    parts = ["expected block to publish a #{expected_routing_key.inspect} event"]
    parts << "with payload #{@expected_payload.inspect}" if @expected_payload
    parts << "with headers #{@expected_headers.inspect}" if @expected_headers
    parts << "exactly #{@expected_count} time(s)" if @expected_count

    published = @new_events&.size || 0
    parts << "but #{published} matching event(s) were published"

    if published.positive? && @expected_payload
      actual_payloads = @after.map(&:payload)
      parts << "actual payloads: #{actual_payloads.inspect}"
    end

    parts.join(", ")
  end

  failure_message_when_negated do
    "expected block not to publish a #{expected_routing_key.inspect} event, but #{@new_events.size} were published"
  end
end

RSpec.configure do |config|
  config.include Pgbus::Testing::Assertions
end
