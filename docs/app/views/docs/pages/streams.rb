# frozen_string_literal: true

# The Turbo Streams replacement over Postgres SSE: a drop-in helper, the
# correctness bugs it fixes, transactional broadcasts, backlog replay, and
# presence. The fanout diagram anchors the reconnect-replay idea.
class Views::Docs::Pages::Streams < DocsUI::Page
  title "Real-time streams"
  eyebrow "Guide"

  def lead = "A drop-in Turbo Streams transport over Postgres SSE — no Action Cable, no Redis, no lost messages on reconnect."

  def content
    usage
    what_it_fixes
    transactional
    replay
    presence
    consumer
  end

  private

  def usage
    DocsUI::Section("Swap one helper") do
      md <<~'MD'
        pgbus ships a drop-in replacement for turbo-rails' `turbo_stream_from`.
        Swap the helper in your view; everything else — the model concern, the
        broadcast helpers — stays the same:
      MD
      DocsUI::Code(<<~ERB, filename: "app/views/orders/show.html.erb", lexer: :erb)
        <%# Before %>
        <%= turbo_stream_from @order %>

        <%# After — no Action Cable, no Redis %>
        <%= pgbus_stream_from @order %>
      ERB
      DocsUI::Code(<<~RUBY, filename: "app/models/order.rb")
        class Order < ApplicationRecord
          broadcasts_to ->(order) { [order.account, :orders] }
        end
      RUBY
      render Components::Diagrams::StreamsFanout.new
      DocsUI::Callout(:warning) do
        plain "Add the Puma plugin so SSE connections drain cleanly on deploy: "
        code { "plugin :pgbus_streams" }
        plain " in config/puma.rb. Streams need Puma 6.1+ or Falcon (they use rack.hijack), "
        plain "and HTTP/2 in production to lift the 6-connection-per-origin SSE limit."
      end
    end
  end

  def what_it_fixes
    DocsUI::Section("What it fixes", description: "Three well-known Action Cable correctness bugs.") do
      md <<~'MD'
        Every broadcast gets a monotonic PGMQ `msg_id`. The helper captures the
        current max at render time and embeds it as a cursor; the SSE client sends
        it as `Last-Event-ID` on reconnect, and the streamer replays anything newer
        from the live queue and the archive. That cursor model is what fixes the
        classic Action Cable gaps:
      MD
      DocsUI::Table(
        [ "Bug", "What breaks", "How pgbus fixes it" ],
        [
          [ "Page born stale", "A broadcast between render and subscribe is lost.", "A render-time msg_id watermark replays the gap." ],
          [ "Missed on reconnect", "A dropped connection misses what aired.", [ :md, "`Last-Event-ID` replays from the PGMQ archive." ] ],
          [ "No disconnect signal", "The client can't tell it dropped.", [ :md, "`pgbus:open` / `pgbus:gap-detected` / `pgbus:close` DOM events." ] ]
        ]
      )
    end
  end

  def transactional
    DocsUI::Section("Transactional broadcasts", description: "Deferred until commit — no phantom updates.") do
      md <<~'MD'
        A broadcast issued inside an open Active Record transaction is deferred
        until the transaction commits. If it rolls back, the broadcast drops —
        clients never see a change the database never persisted. No other Rails
        real-time stack can do this, because Action Cable's path goes through a
        broker with no idea of your transaction boundary.
      MD
      DocsUI::Code(<<~RUBY)
        ActiveRecord::Base.transaction do
          @order.update!(status: "shipped")
          @order.broadcast_replace_to :account    # ← deferred until commit
          RelatedService.update_counters!(@order) # ← if this raises, both roll back
        end
        # Rolled back? No client ever saw "shipped".
      RUBY
    end
  end

  def replay
    DocsUI::Section("Replaying history on connect") do
      md <<~'MD'
        By default a stream shows only broadcasts published after render (the
        page-born-stale fix). For chat-style backlog, pass `replay:`:
      MD
      DocsUI::Code(<<~ERB, lexer: :erb)
        <%= pgbus_stream_from @room, replay: 50 %>       <%# last 50 on load %>
        <%= pgbus_stream_from @room, replay: :all %>     <%# everything in retention %>
        <%= pgbus_stream_from @room, replay: :watermark %> <%# default: post-render only %>
      ERB
      DocsUI::Callout(:note) do
        plain "How far back "
        code { "replay: :all" }
        plain " reaches depends on the stream's retention ("
        code { "streams_retention" }
        plain " / "
        code { "streams_default_retention" }
        plain ", default 5 minutes). Bump it for chat streams that need days of history."
      end
    end
  end

  def presence
    DocsUI::Section("Presence", description: "\"X people are in this room.\"") do
      md <<~'MD'
        Track who is subscribed to a stream with a presence table. Join and leave
        are explicit — the controller decides who is present — and the block you
        pass is rendered and broadcast to every connected client:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:add_presence && rails db:migrate
      SHELL
      DocsUI::Code(<<~RUBY)
        Pgbus.stream(@room).presence.join(
          member_id: current_user.id.to_s,
          metadata: { name: current_user.name }
        ) { |member| render_to_string(partial: "presence/joined", locals: { member: }) }

        Pgbus.stream(@room).presence.members # => [{ "id" => "7", "metadata" => {...} }, …]
        Pgbus.stream(@room).presence.count   # => 5
      RUBY
    end
  end

  def consumer
    DocsUI::Section("On the consuming side") do
      md <<~'MD'
        [phlex-reactive](https://phlex-reactive.zoolutions.llc) uses pgbus as its
        broadcast transport, so its
        [Transport: pgbus](https://phlex-reactive.zoolutions.llc/docs/transport-pgbus)
        page is a good companion read — it shows the same primitives from a
        component author's point of view, including the pgbus-only `exclude:` and
        `visible_to:` broadcast options.
      MD
    end
  end
end
