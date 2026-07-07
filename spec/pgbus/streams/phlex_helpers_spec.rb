# frozen_string_literal: true

require "spec_helper"
require "active_support/all"
require "action_view"
require "phlex/rails"
require "pgbus/streams/phlex_helpers"

# Pgbus::Streams::PhlexHelpers bridges the Rails view helper pgbus_stream_from
# into a Phlex component, so apps stop hand-registering it (issue #334). It
# mirrors phlex-rails' own Phlex::Rails::Helpers::TurboStreamFrom exactly.
RSpec.describe Pgbus::Streams::PhlexHelpers do
  it "registers pgbus_stream_from as a Phlex output helper" do
    expect(described_class.instance_methods).to include(:pgbus_stream_from)
  end

  it "delegates pgbus_stream_from to the Rails view context (phlex-rails output-helper contract)" do
    # register_output_helper generates a method that calls
    # view_context.pgbus_stream_from(...) and wraps the result in raw(). Exercise
    # that delegation with a stubbed view_context + raw, which is the whole
    # contract — the actual markup is produced by the Rails view helper.
    view_context = Object.new
    captured = nil
    view_context.define_singleton_method(:pgbus_stream_from) do |*names, **|
      captured = names.first
      "<pgbus-stream-source></pgbus-stream-source>"
    end

    component = Class.new { include Pgbus::Streams::PhlexHelpers }.new
    component.define_singleton_method(:view_context) { view_context }
    component.define_singleton_method(:raw) { |html| html }

    result = component.pgbus_stream_from("chat-room")

    expect(captured).to eq("chat-room")
    expect(result).to include("<pgbus-stream-source")
  end
end
