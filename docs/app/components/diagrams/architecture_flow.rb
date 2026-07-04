# frozen_string_literal: true

module Components
  module Diagrams
    # The pgbus data path as one picture: a Rails app enqueues through
    # Pgbus::Client into PGMQ's queue tables; a supervisor forks workers that
    # read and execute, while a dispatcher runs the periodic maintenance; the
    # dashboard reads the same tables through Web::DataSource. Every colour is a
    # daisyUI CSS variable, so it re-tints with the theme switcher.
    class ArchitectureFlow < Components::Base
      include SvgHelpers

      def view_template
        render Figure.new(
          caption: "Everything goes through Pgbus::Client into PGMQ's tables; " \
                   "workers read and execute, the dashboard reads the same tables.",
          label: "Architecture: Rails app through Pgbus::Client into PGMQ queue " \
                 "tables, read by supervised workers, surfaced by the dashboard."
        ) do
          diagram_svg(viewbox: "0 0 920 380") do |s|
            producers(s)
            client(s)
            pgmq(s)
            consumers(s)
            dashboard(s)
          end
        end
      end

      private

      def producers(s)
        node(s, x: 20, y: 40, w: 170, h: 60, title: "Rails app",
             subtitle: "ActiveJob · events", fill: "var(--color-base-100)")
        label(s, 105, 128, "enqueue / publish", tone: "primary", opacity: "0.85")
        link(s, "M105 100 V150")
      end

      def client(s)
        node(s, x: 20, y: 150, w: 170, h: 56, title: "Pgbus::Client",
             subtitle: "the one PGMQ door", fill: "var(--color-base-200)",
             stroke: "var(--color-primary)")
        link(s, "M190 178 H300")
      end

      def pgmq(s)
        node(s, x: 300, y: 60, w: 280, h: 250, title: "",
             fill: "var(--color-base-200)", stroke: "var(--color-primary)")
        label(s, 440, 84, "PostgreSQL + PGMQ", tone: "primary", opacity: "0.95")
        node(s, x: 320, y: 100, w: 240, h: 44, title: "q_<queue>  (messages)")
        node(s, x: 320, y: 156, w: 240, h: 44, title: "a_<queue>  (archive)")
        node(s, x: 320, y: 212, w: 240, h: 44, title: "<queue>_dlq  (dead letters)")
        node(s, x: 320, y: 268, w: 240, h: 30, title: "pgbus_* metadata tables",
             fill: "var(--color-base-100)", stroke: "var(--color-base-300)")
      end

      def consumers(s)
        # Supervisor forks workers; a dispatcher runs periodic maintenance.
        node(s, x: 700, y: 44, w: 200, h: 40, title: "Supervisor",
             fill: "var(--color-base-100)", stroke: "var(--color-primary)")
        node(s, x: 700, y: 100, w: 200, h: 40, title: "Worker  (read → execute)")
        node(s, x: 700, y: 152, w: 200, h: 40, title: "Worker  (read → execute)")
        node(s, x: 700, y: 210, w: 200, h: 40, title: "Dispatcher",
             subtitle: "recurring · vacuum · DLQ", fill: "var(--color-base-100)")
        link(s, "M760 84 V100", dash: "4 3")
        link(s, "M810 84 C860 92 860 140 810 152", dash: "4 3")
        link(s, "M580 120 C650 120 650 120 700 120")
        link(s, "M580 176 C650 176 650 172 700 172")
        link(s, "M580 230 C650 230 650 230 700 230")
        label(s, 640, 108, "read_batch", opacity: "0.6")
      end

      def dashboard(s)
        node(s, x: 300, y: 330, w: 280, h: 40, title: "Dashboard · Web::DataSource",
             fill: "var(--color-accent)", stroke: "var(--color-accent)")
        link(s, "M440 310 V330", tone: "accent")
        label(s, 610, 356, "reads the same tables", tone: "accent", opacity: "0.85")
      end
    end
  end
end
