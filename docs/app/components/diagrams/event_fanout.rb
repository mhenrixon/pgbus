# frozen_string_literal: true

module Components
  module Diagrams
    # One publish, many subscribers: a topic like orders.created is matched
    # against subscription patterns (orders.#, payments.*), copied into each
    # matching subscriber's queue, and handled — with the processed-events table
    # deduplicating idempotent handlers. The pattern matching and the idempotency
    # gate are the two ideas.
    class EventFanout < Components::Base
      include SvgHelpers

      def view_template
        render Figure.new(
          caption: "A published topic is matched against subscription patterns, " \
                   "fanned to each subscriber's queue, and deduplicated per handler.",
          label: "Event fanout: publish a topic, match subscription patterns, " \
                 "deliver to subscriber queues, dedupe via processed_events."
        ) do
          svg(
            viewbox: "0 0 920 340",
            width: "100%",
            xmlns: "http://www.w3.org/2000/svg",
            class: "text-base-content",
            font_family: "ui-sans-serif, system-ui, sans-serif"
          ) do |s|
            arrow_markers(s)
            publish(s)
            router(s)
            subscribers(s)
            idempotency(s)
          end
        end
      end

      private

      def publish(s)
        node(s, x: 20, y: 140, w: 180, h: 60, title: "publish",
             subtitle: '"orders.created"', fill: "var(--color-base-100)")
        link(s, "M200 170 H280")
      end

      def router(s)
        node(s, x: 280, y: 110, w: 200, h: 120, title: "",
             fill: "var(--color-base-200)", stroke: "var(--color-primary)")
        label(s, 380, 132, "topic routing", tone: "primary", opacity: "0.95")
        s.text(x: 300, y: 162, font_size: "12.5", fill: "currentColor") { "orders.#      ✓" }
        s.text(x: 300, y: 186, font_size: "12.5", fill: "currentColor") { "orders.*      ✓" }
        s.text(x: 300, y: 210, font_size: "12.5", fill: "var(--color-base-content)") { "payments.*   ✗" }
      end

      def subscribers(s)
        %w[OrderAudit Analytics].each_with_index do |name, i|
          y = 100 + (i * 90)
          node(s, x: 560, y: y, w: 180, h: 56, title: name,
               subtitle: "subscriber queue", fill: "var(--color-base-100)")
          link(s, "M480 170 C520 170 520 #{y + 28} 560 #{y + 28}")
        end
      end

      def idempotency(s)
        node(s, x: 560, y: 280, w: 340, h: 40,
             title: "pgbus_processed_events  (event_id, handler)",
             fill: "var(--color-base-100)", stroke: "var(--color-base-300)")
        2.times do |i|
          y = 128 + (i * 90)
          link(s, "M740 #{y} C800 #{y} 780 280 740 292", dash: "4 3")
        end
        label(s, 730, 336, "idempotent! → skip if already handled", opacity: "0.7")
      end
    end
  end
end
