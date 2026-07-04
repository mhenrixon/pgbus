# frozen_string_literal: true

module Components
  module Diagrams
    # Shared SVG-drawing primitives for the flow diagrams: rounded nodes, arrowed
    # links, free text labels, and numbered steps. Every colour is a daisyUI CSS
    # variable (var(--color-*)) or currentColor, so a diagram re-tints with the
    # theme switcher — no per-theme asset. Mixed into each diagram, which draws
    # inside an <svg> block yielded as `s`.
    module SvgHelpers
      # The shared diagram canvas: the <svg> wrapper every flow diagram uses
      # (100%-wide, currentColor-tinted, system font), with the arrowhead markers
      # already defined. A diagram passes its viewbox and draws inside the block,
      # which yields the svg builder `s`.
      #
      # aria-hidden: the enclosing Figure div carries role="img" + an aria-label
      # describing the whole diagram, so the SVG's decorative <text> nodes must
      # not be re-announced (role="img" descendant suppression is inconsistent
      # across screen readers, so we hide the subtree explicitly).
      def diagram_svg(viewbox:, &block)
        svg(
          viewbox: viewbox,
          width: "100%",
          xmlns: "http://www.w3.org/2000/svg",
          class: "text-base-content",
          font_family: "ui-sans-serif, system-ui, sans-serif",
          aria: { hidden: "true" }
        ) do |s|
          arrow_markers(s)
          block.call(s)
        end
      end

      # The two arrowhead markers most diagrams use — a primary and an accent
      # tip. Call once inside the diagram's <defs>.
      def arrow_markers(s)
        s.defs do
          %w[primary accent].each do |tone|
            s.marker(id: "arrow-#{tone}", viewbox: "0 0 10 10", refx: "8", refy: "5",
                     markerwidth: "7", markerheight: "7", orient: "auto-start-reverse") do
              s.path(d: "M0 0 L10 5 L0 10 z", fill: "var(--color-#{tone})")
            end
          end
        end
      end

      # A rounded box with a title and optional subtitle. Fill/stroke are daisyUI
      # vars so the node re-tints with the theme.
      def node(s, x:, y:, w:, h:, title:, subtitle: nil,
               fill: "var(--color-base-100)", stroke: "var(--color-base-300)")
        s.rect(x: x, y: y, width: w, height: h, rx: "12",
               fill: fill, stroke: stroke, stroke_width: "1.5")
        s.text(x: x + (w / 2), y: subtitle ? y + (h / 2) - 6 : y + (h / 2) + 5,
               text_anchor: "middle", font_size: "15", font_weight: "700",
               fill: "currentColor") { title }
        return unless subtitle

        s.text(x: x + (w / 2), y: y + (h / 2) + 15, text_anchor: "middle",
               font_size: "12", fill: "var(--color-base-content)", opacity: "0.6") { subtitle }
      end

      # An arrowed path. `tone` picks the primary/accent marker + stroke colour;
      # pass `dash` for a dashed line (used for async / broadcast edges).
      def link(s, d, tone: "primary", dash: nil)
        attrs = { d: d, fill: "none", stroke: "var(--color-#{tone})", stroke_width: "2",
                  marker_end: "url(#arrow-#{tone})" }
        attrs[:stroke_dasharray] = dash if dash
        s.path(**attrs)
      end

      # Free-standing text (edge labels, callouts).
      def label(s, x, y, text, tone: "base-content", opacity: "0.7", anchor: "middle")
        s.text(x: x, y: y, text_anchor: anchor, font_size: "12",
               fill: "var(--color-#{tone})", opacity: opacity) { text }
      end

      # A numbered step inside a pipeline box: a tinted circle + inline text.
      def step(s, x:, y:, n:, text:, tone: "primary")
        s.circle(cx: x + 12, cy: y + 12, r: "11", fill: "var(--color-#{tone})", opacity: "0.15")
        s.text(x: x + 12, y: y + 16, text_anchor: "middle", font_size: "12",
               font_weight: "700", fill: "var(--color-#{tone})") { n }
        s.text(x: x + 32, y: y + 16, font_size: "12.5", fill: "currentColor") { text }
      end
    end
  end
end
