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

    it "uses config.streams_path when set, bypassing engine mount detection" do
      Pgbus.configuration.streams_path = "/pgbus/streams"
      html = view.pgbus_stream_from("chat")

      match = html.match(/src="([^"]+)"/)
      expect(match[1]).to start_with("/pgbus/streams/")
    ensure
      Pgbus.configuration.streams_path = nil
    end

    it "normalizes a trailing slash on config.streams_path" do
      Pgbus.configuration.streams_path = "/pgbus/streams/"
      html = view.pgbus_stream_from("chat")

      match = html.match(/src="([^"]+)"/)
      expect(match[1]).not_to include("//")
      expect(match[1]).to start_with("/pgbus/streams/")
    ensure
      Pgbus.configuration.streams_path = nil
    end

    it "builds the src URL from the engine's url_helpers so custom mounts work" do
      # The helper delegates src construction to a private
      # pgbus_stream_src(signed_name) method that asks
      # Pgbus::Engine.routes.url_helpers.streams_path for the
      # engine's actual mount point and appends /:signed_name. Stub
      # the private method to simulate what it would return when
      # the engine is mounted at a custom location like
      # /admin/dashboard — loading Pgbus::Engine for real in a
      # unit spec triggers `isolate_namespace Pgbus` which requires
      # a full Rails stack. The fallback path (rescue NameError)
      # is covered by every other example in this file, since the
      # plain Class.new view_class has no url_helpers wired in.
      allow(view).to receive(:pgbus_stream_src) do |signed_name|
        "/admin/dashboard/streams/#{signed_name}"
      end

      html = view.pgbus_stream_from("chat")

      match = html.match(/src="([^"]+)"/)
      expect(match[1]).to start_with("/admin/dashboard/streams/")
      expect(match[1]).not_to include("/pgbus/streams")
    end
  end

  describe "#pgbus_stream_from with replay: option" do
    it "sets since-id=0 when replay: :all so the client receives the full retained backlog" do
      html = view.pgbus_stream_from("chat", replay: :all)
      expect(html).to include('since-id="0"')
    end

    it "sets since-id to (current_msg_id - N) when replay: is an integer" do
      # current_msg_id is stubbed to 1247 in the outer before block.
      # replay: 10 → since_id = 1247 - 10 = 1237
      html = view.pgbus_stream_from("chat", replay: 10)
      expect(html).to include('since-id="1237"')
    end

    it "clamps replay: N to 0 when N > current_msg_id (fresh queue with a huge replay)" do
      allow_any_instance_of(Pgbus::Streams::Stream).to receive(:current_msg_id).and_return(5)
      html = view.pgbus_stream_from("chat", replay: 1000)
      expect(html).to include('since-id="0"')
    end

    it "uses the watermark (current behavior) when replay: is omitted" do
      html = view.pgbus_stream_from("chat")
      expect(html).to include('since-id="1247"')
    end

    it "uses the watermark when replay: :watermark is explicit" do
      html = view.pgbus_stream_from("chat", replay: :watermark)
      expect(html).to include('since-id="1247"')
    end

    it "rejects negative replay: values" do
      expect { view.pgbus_stream_from("chat", replay: -5) }
        .to raise_error(ArgumentError, /replay/)
    end

    it "rejects unknown replay: symbols" do
      expect { view.pgbus_stream_from("chat", replay: :something) }
        .to raise_error(ArgumentError, /replay/)
    end

    it "does not pass the replay: keyword through as an HTML attribute" do
      html = view.pgbus_stream_from("chat", replay: :all)
      expect(html).not_to include("replay=")
    end

    it "caches the watermark lookup when used with replay: (avoids double-query)" do
      call_count = 0
      allow_any_instance_of(Pgbus::Streams::Stream).to receive(:current_msg_id) do
        call_count += 1
        1247
      end

      view.pgbus_stream_from("chat", replay: 10)
      view.pgbus_stream_from("chat", replay: :all)
      view.pgbus_stream_from("chat") # watermark path

      expect(call_count).to eq(1)
    end
  end
end
