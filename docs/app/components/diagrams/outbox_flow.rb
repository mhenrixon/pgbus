# frozen_string_literal: true

module Components
  module Diagrams
    # The transactional outbox: the business row and the outbox row are written
    # in the same transaction, so a rollback takes both — no phantom event. A
    # poller later moves committed rows into PGMQ. The rollback branch is the
    # whole point, so it gets equal billing.
    class OutboxFlow < Components::Base
      include SvgHelpers

      def view_template
        render Figure.new(
          caption: "The outbox row commits with the business row — or rolls back " \
                   "with it. A poller moves only committed rows into PGMQ.",
          label: "Outbox flow: a transaction writes the business row and outbox " \
                 "row together; on commit a poller publishes to PGMQ, on rollback nothing leaks."
        ) do
          svg(
            viewbox: "0 0 920 340",
            width: "100%",
            xmlns: "http://www.w3.org/2000/svg",
            class: "text-base-content",
            font_family: "ui-sans-serif, system-ui, sans-serif"
          ) do |s|
            arrow_markers(s)
            transaction(s)
            commit_path(s)
            rollback_path(s)
          end
        end
      end

      private

      def transaction(s)
        node(s, x: 20, y: 80, w: 260, h: 180, title: "",
             fill: "var(--color-base-200)", stroke: "var(--color-primary)")
        label(s, 150, 104, "one DB transaction", tone: "primary", opacity: "0.95")
        node(s, x: 40, y: 120, w: 220, h: 44, title: "INSERT order")
        node(s, x: 40, y: 176, w: 220, h: 44, title: "INSERT outbox row")
        label(s, 150, 244, "Outbox.publish(...)", opacity: "0.6")
      end

      def commit_path(s)
        link(s, "M280 150 C360 150 360 120 440 120")
        label(s, 360, 110, "COMMIT", tone: "primary", opacity: "0.85")
        node(s, x: 440, y: 96, w: 180, h: 48, title: "outbox poller",
             subtitle: "SKIP LOCKED", fill: "var(--color-base-100)")
        link(s, "M620 120 H700")
        node(s, x: 700, y: 96, w: 180, h: 48, title: "PGMQ queue",
             fill: "var(--color-base-100)", stroke: "var(--color-primary)")
      end

      def rollback_path(s)
        link(s, "M280 210 C360 210 360 240 440 240", tone: "accent", dash: "5 4")
        label(s, 360, 232, "ROLLBACK", tone: "accent", opacity: "0.85")
        node(s, x: 440, y: 216, w: 300, h: 48, title: "nothing published",
             subtitle: "outbox row rolled back too", fill: "var(--color-base-100)",
             stroke: "var(--color-accent)")
      end
    end
  end
end
