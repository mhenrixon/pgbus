# frozen_string_literal: true

require "spec_helper"

# The helper lives under app/helpers/ but isn't autoloaded in non-Rails
# specs. Require it explicitly.
require_relative "../../app/helpers/pgbus/streams_helper" unless defined?(Pgbus::StreamsHelper)

RSpec.describe Pgbus::StreamsHelper do
  # A dummy view class to host the helper so `described_class` behaves
  # like a normal module-under-test.
  let(:view_class) do
    Class.new do
      include Pgbus::StreamsHelper
    end
  end
  let(:view) { view_class.new }

  before do
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    # Intercept the watermark lookup so the helper doesn't reach the
    # real Pgbus::Client (which tries to hit the database). Stubbing
    # at the Stream instance level is more robust than stubbing
    # Pgbus.stream — the latter is a module method that gets memoised
    # in surprising ways under Zeitwerk.
    allow_any_instance_of(Pgbus::Streams::Stream).to receive(:current_msg_id).and_return(1247)
    Thread.current[:pgbus_streams_watermark_cache] = nil
  end

  after do
    Pgbus.configuration.streams_signed_name_secret = nil
    Thread.current[:pgbus_streams_watermark_cache] = nil
  end

  describe "#pgbus_stream_from" do
    it "renders a <pgbus-stream-source> element with signed name and watermark" do
      html = view.pgbus_stream_from("chat")

      expect(html).to include("<pgbus-stream-source")
      expect(html).to include('signed-stream-name="')
      expect(html).to include('since-id="1247"')
      expect(html).to include('src="/pgbus/streams/')
      expect(html).to include("</pgbus-stream-source>")
    end

    it "uses Pgbus::Streams::SignedName to produce the signed name" do
      html = view.pgbus_stream_from("chat")

      match = html.match(/signed-stream-name="([^"]+)"/)
      expect(match).not_to be_nil
      token = match[1]
      expect(Pgbus::Streams::SignedName.verify!(token)).to eq("chat")
    end

    it "includes the channel=Turbo::StreamsChannel compat shim" do
      html = view.pgbus_stream_from("chat")
      expect(html).to include('channel="Turbo::StreamsChannel"')
    end

    it "derives the stream name from a GlobalID-like object" do
      account = double("Account", to_gid_param: "gid://app/Account/42")

      html = view.pgbus_stream_from(account)

      expect(html).to include('since-id="1247"')
      match = html.match(/signed-stream-name="([^"]+)"/)
      expect(Pgbus::Streams::SignedName.verify!(match[1])).to eq("gid://app/Account/42")
    end

    it "joins multiple streamables with colons (turbo-rails compatible)" do
      account = double("Account", to_gid_param: "gid://app/Account/42")

      html = view.pgbus_stream_from(account, :messages)

      match = html.match(/signed-stream-name="([^"]+)"/)
      expect(Pgbus::Streams::SignedName.verify!(match[1])).to eq("gid://app/Account/42:messages")
    end

    it "caches the watermark per-thread across multiple calls in the same render" do
      # Count how many times current_msg_id is called. any_instance_of's
      # have_received doesn't track calls across instances, so we reach
      # for a side-effect counter instead.
      call_count = 0
      allow_any_instance_of(Pgbus::Streams::Stream).to receive(:current_msg_id) do
        call_count += 1
        1247
      end

      view.pgbus_stream_from("chat")
      view.pgbus_stream_from("chat")
      view.pgbus_stream_from("chat")

      expect(call_count).to eq(1)
    end

    it "passes through arbitrary HTML attributes" do
      html = view.pgbus_stream_from("chat", "data-controller" => "messages", "class" => "hidden")
      expect(html).to include('data-controller="messages"')
      expect(html).to include('class="hidden"')
    end

    it "escapes HTML-unsafe characters in attribute values" do
      html = view.pgbus_stream_from("chat", "data-whatever" => '">evil<')
      expect(html).not_to include('">evil<')
      expect(html).to include("&quot;&gt;evil&lt;")
    end
  end
end
