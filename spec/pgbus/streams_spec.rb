# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams do
  describe Pgbus::Streams::Stream do
    subject(:stream) { described_class.new("chat", client: client) }

    let(:client) do
      instance_double(
        Pgbus::Client,
        ensure_stream_queue: nil,
        send_message: 1248,
        stream_current_msg_id: 1247,
        read_after: []
      )
    end

    describe "#name" do
      it "returns the logical stream name" do
        expect(stream.name).to eq("chat")
      end
    end

    describe "#broadcast" do
      it "ensures the stream queue exists, then publishes the payload wrapped for PGMQ's JSONB column" do
        stream.broadcast("<turbo-stream>X</turbo-stream>")
        expect(client).to have_received(:ensure_stream_queue).with("chat")
        # The HTML is wrapped as {"html" => ...} because PGMQ stores messages
        # as JSONB and won't accept raw HTML. Dispatcher unwraps before
        # delivering to the SSE client.
        expect(client).to have_received(:send_message).with("chat", { "html" => "<turbo-stream>X</turbo-stream>" })
      end

      it "returns the assigned msg_id" do
        expect(stream.broadcast("X")).to eq(1248)
      end

      it "only ensures the queue once across multiple broadcasts on the same stream object" do
        stream.broadcast("A")
        stream.broadcast("B")
        stream.broadcast("C")
        expect(client).to have_received(:ensure_stream_queue).once
      end

      it "carries exclude: in the wrapped payload for actor-echo suppression" do
        stream.broadcast("<turbo-stream>X</turbo-stream>", exclude: "conn-abc123")
        expect(client).to have_received(:send_message)
          .with("chat", { "html" => "<turbo-stream>X</turbo-stream>", "exclude" => "conn-abc123" })
      end

      it "omits exclude from the payload when nil (the common path)" do
        stream.broadcast("<turbo-stream>X</turbo-stream>", exclude: nil)
        expect(client).to have_received(:send_message)
          .with("chat", { "html" => "<turbo-stream>X</turbo-stream>" })
      end

      it "omits exclude from the payload when blank" do
        stream.broadcast("<turbo-stream>X</turbo-stream>", exclude: "")
        expect(client).to have_received(:send_message)
          .with("chat", { "html" => "<turbo-stream>X</turbo-stream>" })
      end

      it "composes exclude with visible_to" do
        stream.broadcast("X", visible_to: :admins, exclude: "conn-9")
        expect(client).to have_received(:send_message)
          .with("chat", { "html" => "X", "visible_to" => "admins", "exclude" => "conn-9" })
      end

      it "carries a typed event name in the wrapped payload" do
        stream.broadcast("<turbo-stream/>", event: "presence")
        expect(client).to have_received(:send_message)
          .with("chat", { "html" => "<turbo-stream/>", "event" => "presence" })
      end

      it "omits event from the payload when nil (default turbo-stream path)" do
        stream.broadcast("<turbo-stream/>", event: nil)
        expect(client).to have_received(:send_message)
          .with("chat", { "html" => "<turbo-stream/>" })
      end

      it "omits event when it is the default turbo-stream (no need to carry it)" do
        stream.broadcast("<turbo-stream/>", event: "turbo-stream")
        expect(client).to have_received(:send_message)
          .with("chat", { "html" => "<turbo-stream/>" })
      end

      it "composes event with visible_to and exclude" do
        stream.broadcast("X", event: "reactive", visible_to: :admins, exclude: "c1")
        expect(client).to have_received(:send_message).with(
          "chat",
          { "html" => "X", "visible_to" => "admins", "exclude" => "c1", "event" => "reactive" }
        )
      end
    end

    describe "#broadcast_render" do
      # A Phlex-like renderable: Phlex::HTML#call returns the rendered
      # markup string. This is the shape the issue's example uses
      # (Chat::Message.new(...)).
      let(:phlex_like) do
        Class.new do
          def initialize(text) = (@text = text)
          def call = "<div class=\"msg\">#{@text}</div>"
        end
      end

      it "renders the component, wraps it in a turbo-stream action tag, and broadcasts" do
        stream.broadcast_render(renderable: phlex_like.new("hi"), action: :append, target: "messages")
        expect(client).to have_received(:send_message).with(
          "chat",
          { "html" => '<turbo-stream action="append" target="messages"><template><div class="msg">hi</div></template></turbo-stream>' }
        )
      end

      it "defaults the action to replace" do
        stream.broadcast_render(renderable: phlex_like.new("x"), target: "item_1")
        expect(client).to have_received(:send_message).with(
          "chat",
          { "html" => '<turbo-stream action="replace" target="item_1"><template><div class="msg">x</div></template></turbo-stream>' }
        )
      end

      it "accepts a pre-rendered HTML string as the renderable" do
        stream.broadcast_render(renderable: "<p>plain</p>", action: :prepend, target: "list")
        expect(client).to have_received(:send_message).with(
          "chat",
          { "html" => '<turbo-stream action="prepend" target="list"><template><p>plain</p></template></turbo-stream>' }
        )
      end

      it "omits the <template> for content-less actions like remove" do
        stream.broadcast_render(action: :remove, target: "item_9")
        expect(client).to have_received(:send_message).with(
          "chat",
          { "html" => '<turbo-stream action="remove" target="item_9"></turbo-stream>' }
        )
      end

      it "HTML-escapes the action and target attributes" do
        stream.broadcast_render(renderable: "x", action: :replace, target: 'a"b')
        expect(client).to have_received(:send_message).with(
          "chat",
          { "html" => '<turbo-stream action="replace" target="a&quot;b"><template>x</template></turbo-stream>' }
        )
      end

      it "passes exclude through to the broadcast (composes with #165)" do
        stream.broadcast_render(renderable: "x", target: "t", exclude: "conn-7")
        expect(client).to have_received(:send_message).with(
          "chat",
          {
            "html" => '<turbo-stream action="replace" target="t"><template>x</template></turbo-stream>',
            "exclude" => "conn-7"
          }
        )
      end

      it "passes visible_to through to the broadcast" do
        stream.broadcast_render(renderable: "x", target: "t", visible_to: :admins)
        expect(client).to have_received(:send_message).with(
          "chat",
          {
            "html" => '<turbo-stream action="replace" target="t"><template>x</template></turbo-stream>',
            "visible_to" => "admins"
          }
        )
      end

      it "renders an object that responds to render_in (ViewComponent/Phlex-rails shape)" do
        renderable = Class.new do
          def render_in(_view_context) = "<span>vc</span>"
        end.new
        stream.broadcast_render(renderable: renderable, target: "t")
        expect(client).to have_received(:send_message).with(
          "chat",
          { "html" => '<turbo-stream action="replace" target="t"><template><span>vc</span></template></turbo-stream>' }
        )
      end

      it "requires a target" do
        expect { stream.broadcast_render(renderable: "x", target: nil) }
          .to raise_error(ArgumentError, /target/)
      end

      it "returns the msg_id from the underlying broadcast" do
        expect(stream.broadcast_render(renderable: "x", target: "t")).to eq(1248)
      end

      it "passes a typed event through to the broadcast" do
        stream.broadcast_render(renderable: "x", target: "t", event: "reactive")
        expect(client).to have_received(:send_message).with(
          "chat",
          {
            "html" => '<turbo-stream action="replace" target="t"><template>x</template></turbo-stream>',
            "event" => "reactive"
          }
        )
      end
    end

    describe "#broadcast inside an AR transaction" do
      # Use a plain double here instead of instance_double(Pgbus::Client)
      # because instance_double walks the class's ancestor chain, and
      # stub_const("ActiveRecord::Base", ...) below replaces a constant
      # that AR-related ancestors depend on. The ancestor walk then
      # hits our fake double with unexpected :> calls.
      subject(:stream) { described_class.new("chat", client: client) }

      let(:client) do
        double(
          "Pgbus::Client",
          ensure_stream_queue: nil,
          send_message: 1248,
          stream_current_msg_id: 1247,
          read_after: []
        )
      end

      # A duck-typed stand-in for ActiveRecord::ConnectionAdapters::RealTransaction.
      # The real class has a richer surface but Stream#broadcast only uses
      # #open?, #after_commit { ... }, and #after_rollback { ... }.
      let(:transaction) do
        Class.new do
          def initialize
            @open = true
            @after_commit = []
            @after_rollback = []
          end

          attr_accessor :open

          def open?      = @open
          def closed?    = !@open
          def after_commit(&block)   = @after_commit << block
          def after_rollback(&block) = @after_rollback << block
          def commit!    = @after_commit.each(&:call).then { @open = false }
          def rollback!  = @after_rollback.each(&:call).then { @open = false }
        end.new
      end

      let(:ar_connection) do
        double("ActiveRecord::ConnectionAdapters::AbstractAdapter", current_transaction: transaction)
      end

      let(:ar_base) { double("ActiveRecord::Base", connection: ar_connection) }

      before { stub_const("ActiveRecord::Base", ar_base) }

      it "defers send_message until the transaction commits" do
        stream.broadcast("<turbo-stream>A</turbo-stream>")

        expect(client).not_to have_received(:send_message)
        transaction.commit!
        expect(client).to have_received(:send_message)
          .with("chat", { "html" => "<turbo-stream>A</turbo-stream>" })
      end

      it "drops the broadcast silently on rollback" do
        stream.broadcast("<turbo-stream>A</turbo-stream>")
        transaction.rollback!

        expect(client).not_to have_received(:send_message)
      end

      it "accumulates multiple broadcasts and fires them all on commit in order" do
        sent = []
        allow(client).to receive(:send_message) { |_name, payload| sent << payload["html"] }

        stream.broadcast("<turbo-stream>A</turbo-stream>")
        stream.broadcast("<turbo-stream>B</turbo-stream>")
        stream.broadcast("<turbo-stream>C</turbo-stream>")

        expect(sent).to be_empty
        transaction.commit!
        expect(sent).to eq([
                             "<turbo-stream>A</turbo-stream>",
                             "<turbo-stream>B</turbo-stream>",
                             "<turbo-stream>C</turbo-stream>"
                           ])
      end

      it "still ensures the queue synchronously even when the broadcast itself defers" do
        stream.broadcast("<turbo-stream>A</turbo-stream>")
        expect(client).to have_received(:ensure_stream_queue).with("chat")
      end

      it "returns nil instead of a msg_id when the send is deferred" do
        # Callers that rely on the msg_id have to opt out of transactional mode.
        # This is a documented trade-off.
        expect(stream.broadcast("X")).to be_nil
      end

      context "when the transaction is already closed" do
        it "falls back to synchronous send_message" do
          transaction.open = false
          stream.broadcast("X")
          expect(client).to have_received(:send_message).with("chat", { "html" => "X" })
        end
      end

      context "when a later broadcast's send_message raises after commit" do
        it "does not prevent earlier deferred broadcasts from firing" do
          first = nil
          calls = 0
          allow(client).to receive(:send_message) do |*|
            calls += 1
            first = :ok if calls == 1
            raise "boom" if calls == 2
          end

          stream.broadcast("A")
          stream.broadcast("B")
          stream.broadcast("C")

          # The second broadcast's callback raising should not prevent the
          # first from having fired, nor (ideally) the third. AR's behavior
          # is to stop the callback chain on the first exception, which is
          # acceptable — we just need the data state (A committed, broadcast
          # A delivered) to be consistent.
          expect { transaction.commit! }.to raise_error("boom")
          expect(first).to eq(:ok)
        end
      end
    end

    describe "#broadcast when ActiveRecord is not loaded" do
      # Same reasoning as the transaction context above — plain double so
      # the ancestor walk doesn't hit a stubbed constant.
      subject(:stream) { described_class.new("chat", client: client) }

      let(:client) do
        double(
          "Pgbus::Client",
          ensure_stream_queue: nil,
          send_message: 1248,
          stream_current_msg_id: 1247,
          read_after: []
        )
      end

      before do
        hide_const("ActiveRecord::Base") if defined?(ActiveRecord::Base)
      end

      it "falls back to synchronous send_message" do
        stream.broadcast("X")
        expect(client).to have_received(:send_message).with("chat", { "html" => "X" })
      end
    end

    describe "#current_msg_id" do
      it "delegates to the client and forwards the stream name" do
        expect(stream.current_msg_id).to eq(1247)
        expect(client).to have_received(:stream_current_msg_id).with("chat")
      end
    end

    describe "#read_after" do
      it "delegates to the client" do
        stream.read_after(after_id: 100, limit: 50)
        expect(client).to have_received(:read_after).with("chat", after_id: 100, limit: 50)
      end
    end

    describe "with an object that responds to to_gid_param" do
      let(:account) { double("Account", to_gid_param: "gid://app/Account/42") }

      it "derives a stream name from the GID-param" do
        s = described_class.new(account, client: client)
        expect(s.name).to eq("gid://app/Account/42")
      end
    end

    describe "with an array of streamables" do
      let(:account) { double("Account", to_gid_param: "gid://app/Account/42") }

      it "joins them with colons (turbo-rails compatible)" do
        s = described_class.new([account, :messages], client: client)
        expect(s.name).to eq("gid://app/Account/42:messages")
      end
    end
  end

  describe "Pgbus.stream" do
    it "returns a Pgbus::Streams::Stream" do
      expect(Pgbus.stream("chat")).to be_a(Pgbus::Streams::Stream)
    end

    it "wraps the same name on subsequent calls" do
      expect(Pgbus.stream("chat").name).to eq("chat")
    end

    it "caches Stream instances by name (returns the same object for the same name)" do
      a = Pgbus.stream("cache_hit_test")
      b = Pgbus.stream("cache_hit_test")
      expect(a).to be(b)
    end

    it "returns different instances for different names" do
      a = Pgbus.stream("cache_a")
      b = Pgbus.stream("cache_b")
      expect(a).not_to be(b)
    end

    it "treats GlobalID-equivalent inputs as the same cache key" do
      account = double("Account", to_gid_param: "gid://app/Account/42")
      a = Pgbus.stream(account)
      b = Pgbus.stream(account)
      expect(a).to be(b)
      expect(a.name).to eq("gid://app/Account/42")
    end
  end
end
