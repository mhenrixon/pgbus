# frozen_string_literal: true

module Components
  module Diagrams
    # One message's journey: enqueued into the q_ table, read with a visibility
    # timeout, then either archived on success or — after read_ct crosses
    # max_retries — routed to the _dlq queue. The retry loop and the DLQ exit are
    # the two things worth seeing at a glance.
    class MessageLifecycle < Components::Base
      include SvgHelpers

      def view_template
        render Figure.new(
          caption: "Success archives the message; repeated failures raise read_ct " \
                   "until it crosses max_retries and routes to the DLQ.",
          label: "Message lifecycle: send to the queue table, read under a " \
                 "visibility timeout, archive on success, or dead-letter after max_retries."
        ) do
          diagram_svg(viewbox: "0 0 920 340") do |s|
            enqueue(s)
            read(s)
            success(s)
            failure(s)
            dlq(s)
          end
        end
      end

      private

      def enqueue(s)
        node(s, x: 20, y: 130, w: 150, h: 60, title: "send_message",
             subtitle: "→ q_<queue>", fill: "var(--color-base-100)")
        link(s, "M170 160 H300")
      end

      def read(s)
        node(s, x: 300, y: 120, w: 200, h: 80, title: "read (worker)",
             subtitle: "vt: message hidden", fill: "var(--color-base-200)",
             stroke: "var(--color-primary)")
        label(s, 235, 112, "read_batch", opacity: "0.6")
      end

      def success(s)
        node(s, x: 640, y: 60, w: 260, h: 56, title: "archive → a_<queue>",
             subtitle: "success", fill: "var(--color-base-100)")
        link(s, "M500 150 C580 150 570 92 640 92")
        label(s, 575, 108, "perform_now ok", opacity: "0.6")
      end

      def failure(s)
        # failure re-hides then re-reads; read_ct increments each attempt
        link(s, "M400 200 C400 250 360 250 360 210", dash: "5 4")
        label(s, 300, 255, "raise → vt expires, read_ct++", opacity: "0.7")
      end

      def dlq(s)
        node(s, x: 640, y: 220, w: 260, h: 60, title: "<queue>_dlq",
             subtitle: "read_ct > max_retries", fill: "var(--color-accent)",
             stroke: "var(--color-accent)")
        link(s, "M500 175 C580 175 570 250 640 250", tone: "accent", dash: "5 4")
        label(s, 578, 235, "give up", tone: "accent", opacity: "0.85")
      end
    end
  end
end
