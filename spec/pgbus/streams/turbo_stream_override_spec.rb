# frozen_string_literal: true

require "spec_helper"
require_relative "../../../app/helpers/pgbus/streams_helper" unless defined?(Pgbus::StreamsHelper)

RSpec.describe Pgbus::Streams::TurboStreamOverride do
  # Fake Turbo::StreamsHelper with the same turbo_stream_from signature
  # as the real turbo-rails version. We don't load turbo-rails in unit
  # tests — the override is exercised via a minimal stand-in.
  let(:fake_turbo_streams_helper) do
    Module.new do
      def self.name
        "Turbo::StreamsHelper"
      end

      def turbo_stream_from(*_streamables, **attributes)
        tag = %(<turbo-cable-stream-source channel="Turbo::StreamsChannel")
        tag += attributes.map { |k, v| %( #{k}="#{v}") }.join
        "#{tag}></turbo-cable-stream-source>"
      end
    end
  end

  # A view class that includes both Turbo's helper and pgbus's helper,
  # simulating what a real Rails view stack looks like.
  let(:view_class) do
    turbo_mod = fake_turbo_streams_helper
    Class.new do
      include turbo_mod
      include Pgbus::StreamsHelper
    end
  end

  let(:view) { view_class.new }

  before do
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    allow_any_instance_of(Pgbus::Streams::Stream).to receive(:current_msg_id).and_return(42)
    Thread.current[:pgbus_streams_watermark_cache] = nil
  end

  after do
    Pgbus.configuration.streams_signed_name_secret = nil
    Thread.current[:pgbus_streams_watermark_cache] = nil
  end

  describe ".install_turbo_stream_override!" do
    it "prepends the override onto Turbo::StreamsHelper" do
      stub_const("Turbo::StreamsHelper", fake_turbo_streams_helper)

      Pgbus::Streams.install_turbo_stream_override!
      expect(fake_turbo_streams_helper.ancestors).to include(described_class)
    end

    it "is idempotent — calling twice does not double-prepend" do
      stub_const("Turbo::StreamsHelper", fake_turbo_streams_helper)

      Pgbus::Streams.install_turbo_stream_override!
      Pgbus::Streams.install_turbo_stream_override!
      expect(fake_turbo_streams_helper.ancestors.count(described_class)).to eq(1)
    end

    it "is a no-op when Turbo::StreamsHelper is not defined" do
      hide_const("Turbo::StreamsHelper") if defined?(Turbo::StreamsHelper)
      expect { Pgbus::Streams.install_turbo_stream_override! }.not_to raise_error
    end
  end

  describe "#turbo_stream_from override" do
    before do
      turbo_mod = Module.new do
        def self.signed_stream_verifier_key
          nil
        end
      end
      stub_const("Turbo", turbo_mod)
      stub_const("Turbo::StreamsHelper", fake_turbo_streams_helper)
      Pgbus::Streams.install_turbo_stream_override!
    end

    let(:patched_view_class) do
      turbo_mod = fake_turbo_streams_helper
      Class.new do
        include turbo_mod
        include Pgbus::StreamsHelper
      end
    end

    let(:patched_view) { patched_view_class.new }

    it "renders <pgbus-stream-source> instead of <turbo-cable-stream-source>" do
      html = patched_view.turbo_stream_from("hotwire-livereload")

      expect(html).to include("<pgbus-stream-source")
      expect(html).not_to include("<turbo-cable-stream-source")
    end

    it "includes the signed-stream-name attribute" do
      html = patched_view.turbo_stream_from("hotwire-livereload")

      match = html.match(/signed-stream-name="([^"]+)"/)
      expect(match).not_to be_nil
      expect(Pgbus::Streams::SignedName.verify!(match[1])).to eq("hotwire-livereload")
    end

    it "includes the since-id watermark" do
      html = patched_view.turbo_stream_from("hotwire-livereload")
      expect(html).to include('since-id="42"')
    end

    it "works with GlobalID-like objects" do
      account = double("Account", to_gid_param: "gid://app/Account/7")

      html = patched_view.turbo_stream_from(account)

      match = html.match(/signed-stream-name="([^"]+)"/)
      expect(Pgbus::Streams::SignedName.verify!(match[1])).to eq("gid://app/Account/7")
    end

    it "works with multiple streamables joined by colons" do
      account = double("Account", to_gid_param: "gid://app/Account/7")

      html = patched_view.turbo_stream_from(account, :messages)

      match = html.match(/signed-stream-name="([^"]+)"/)
      expect(Pgbus::Streams::SignedName.verify!(match[1])).to eq("gid://app/Account/7:messages")
    end

    it "falls through to original turbo_stream_from when streams are disabled" do
      Pgbus.configuration.streams_enabled = false
      html = patched_view.turbo_stream_from("hotwire-livereload")

      expect(html).to include("<turbo-cable-stream-source")
      expect(html).not_to include("<pgbus-stream-source")
    ensure
      Pgbus.configuration.streams_enabled = true
    end

    it "works in Rack middleware context where only Turbo::StreamsHelper is included" do
      # Simulates hotwire-livereload's middleware: ActionController::Base.helpers
      # includes Turbo::StreamsHelper but the host app doesn't explicitly
      # include Pgbus::StreamsHelper. The override carries StreamsHelper via
      # its own `include`, so pgbus_stream_from is always available.
      turbo_only_class = Class.new { include Turbo::StreamsHelper }
      turbo_only_view = turbo_only_class.new

      html = turbo_only_view.turbo_stream_from("hotwire-livereload")

      expect(html).to include("<pgbus-stream-source")
      expect(html).not_to include("<turbo-cable-stream-source")
    end
  end
end
