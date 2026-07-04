# frozen_string_literal: true

module Components
  module Diagrams
    # A broadcast reaching many tabs over SSE: broadcast → stream queue →
    # listener → the open SSE connections. A reconnecting tab replays what it
    # missed from the archive via Last-Event-ID. The replay-on-reconnect edge is
    # what sets pgbus apart from Action Cable, so it's called out.
    class StreamsFanout < Components::Base
      include SvgHelpers

      def view_template
        render Figure.new(
          caption: "A broadcast fans out over SSE to every open tab; a reconnecting " \
                   "tab replays what it missed from the archive via Last-Event-ID.",
          label: "Streams fanout: broadcast to a stream queue, a listener pushes " \
                 "over SSE to open tabs, and a reconnecting tab replays from the archive."
        ) do
          diagram_svg(viewbox: "0 0 920 340") do |s|
            broadcast(s)
            listener(s)
            tabs(s)
            replay(s)
          end
        end
      end

      private

      def broadcast(s)
        node(s, x: 20, y: 130, w: 170, h: 60, title: "broadcast",
             subtitle: "→ stream queue", fill: "var(--color-base-100)")
        link(s, "M190 160 H300")
      end

      def listener(s)
        node(s, x: 300, y: 120, w: 190, h: 80, title: "listener",
             subtitle: "LISTEN/NOTIFY", fill: "var(--color-base-200)",
             stroke: "var(--color-primary)")
      end

      def tabs(s)
        %w[Tab\ A Tab\ B].each_with_index do |name, i|
          y = 90 + (i * 80)
          node(s, x: 630, y: y, w: 160, h: 52, title: name,
               subtitle: "open SSE", fill: "var(--color-base-100)")
          link(s, "M490 160 C560 160 560 #{y + 26} 630 #{y + 26}")
        end
        label(s, 560, 128, "SSE push", opacity: "0.6")
      end

      def replay(s)
        node(s, x: 300, y: 260, w: 490, h: 44,
             title: "archive replay on reconnect  (Last-Event-ID)",
             fill: "var(--color-base-100)", stroke: "var(--color-accent)")
        link(s, "M710 220 C760 232 760 260 710 260", tone: "accent", dash: "4 3")
        label(s, 545, 254, "no lost messages", tone: "accent", opacity: "0.85")
      end
    end
  end
end
