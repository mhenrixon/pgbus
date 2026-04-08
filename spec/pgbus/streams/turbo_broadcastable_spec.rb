# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::TurboBroadcastable do
  # Fake Turbo::StreamsChannel with the same broadcast_stream_to signature
  # as the real turbo-rails version. We do not load turbo-rails in unit
  # tests — the patch is exercised via a minimal stand-in, and the
  # integration test in Phase 5 uses the real gem.
  let(:fake_turbo_module) do
    Module.new do
      def self.name
        "Turbo::StreamsChannel"
      end

      class << self
        # rubocop:disable RSpec/InstanceVariable -- this is inside an anonymous
        # Module.new, not the example group's context
        def broadcast_stream_to(*streamables, content:)
          @broadcasts ||= []
          @broadcasts << { streamables: streamables, content: content }
          :action_cable_called
        end
        # rubocop:enable RSpec/InstanceVariable

        def stream_name_from(streamables)
          # Mirror Turbo::Streams::StreamName#stream_name_from
          return streamables.map { |s| stream_name_from([s]) }.join(":") if streamables.length != 1

          s = streamables.first
          if s.is_a?(Array)
            s.map { |x| stream_name_from([x]) }.join(":")
          elsif s.respond_to?(:to_gid_param)
            s.to_gid_param
          else
            s.to_s
          end
        end

        attr_reader :broadcasts

        def reset_broadcasts!
          @broadcasts = []
        end
      end
    end
  end

  before do
    stub_const("Turbo", Module.new)
    stub_const("Turbo::StreamsChannel", fake_turbo_module)
    fake_turbo_module.reset_broadcasts!
  end

  describe ".install_turbo_broadcastable_patch!" do
    it "prepends the patch onto Turbo::StreamsChannel's singleton class" do
      Pgbus::Streams.install_turbo_broadcastable_patch!
      expect(Turbo::StreamsChannel.singleton_class.include?(described_class)).to be true
    end

    it "is idempotent — calling twice does not double-prepend" do
      Pgbus::Streams.install_turbo_broadcastable_patch!
      Pgbus::Streams.install_turbo_broadcastable_patch!
      expect(
        Turbo::StreamsChannel.singleton_class.ancestors.count(described_class)
      ).to eq(1)
    end

    it "is a no-op when Turbo::StreamsChannel is not defined" do
      hide_const("Turbo::StreamsChannel")
      expect { Pgbus::Streams.install_turbo_broadcastable_patch! }.not_to raise_error
    end
  end

  describe "patched broadcast_stream_to" do
    before do
      Pgbus::Streams.install_turbo_broadcastable_patch!
      allow(Pgbus).to receive(:stream).and_return(
        instance_double(Pgbus::Streams::Stream, broadcast: 1248)
      )
    end

    it "routes the broadcast through Pgbus.stream(...).broadcast instead of ActionCable" do
      Turbo::StreamsChannel.broadcast_stream_to("room:42", content: "<turbo-stream>X</turbo-stream>")

      expect(Pgbus).to have_received(:stream).with("room:42")
      expect(fake_turbo_module.broadcasts).to be_empty
    end

    it "derives the stream name from a GlobalID-like streamable" do
      account = double("Account", to_gid_param: "gid://app/Account/42")
      Turbo::StreamsChannel.broadcast_stream_to(account, content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("gid://app/Account/42")
    end

    it "joins multiple streamables with colons (turbo-rails parity)" do
      account = double("Account", to_gid_param: "gid://app/Account/42")
      Turbo::StreamsChannel.broadcast_stream_to(account, :messages, content: "<turbo-stream/>")

      expect(Pgbus).to have_received(:stream).with("gid://app/Account/42:messages")
    end
  end
end
