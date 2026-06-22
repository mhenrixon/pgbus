# frozen_string_literal: true

require "cgi"

module Pgbus
  module Streams
    # Turns a renderable into a complete `<turbo-stream>` action tag,
    # ready to hand to Stream#broadcast. This centralises the off-request
    # render + tag-building that every consumer would otherwise stitch
    # together by hand (the #1 footgun in server-driven UI: rendering a
    # component outside a request without a usable view context).
    #
    # pgbus deliberately has no hard dependency on turbo-rails, ActionView,
    # or Phlex, so this builder is self-contained and matches Turbo's wire
    # format directly. The browser's Turbo runtime consumes the tag; the
    # exact string is the contract, not any particular Ruby library.
    #
    # Renderable resolution (first match wins):
    #   - String              → used verbatim (already-rendered markup)
    #   - responds to :call   → Phlex::HTML#call (the issue's example shape)
    #   - responds to :render_in → ViewComponent / phlex-rails
    #     (`render_in(view_context)`; a nil context is passed because
    #     off-request there is no controller — components that need URL
    #     helpers should be rendered by the app and the string passed in)
    #   - else                → to_s
    #
    # Tag format mirrors Turbo::Streams::TagBuilder:
    #   - content actions wrap the markup in a <template>
    #   - content-less actions (remove) emit no <template>
    module Renderer
      # Turbo stream actions that carry no content (no <template> wrapper).
      CONTENTLESS_ACTIONS = %w[remove].freeze

      module_function

      # Builds a `<turbo-stream action target><template>...</template></turbo-stream>`
      # string. `renderable` may be nil for content-less actions.
      def turbo_stream_tag(action:, target:, renderable: nil)
        raise ArgumentError, "target is required" if target.nil? || target.to_s.empty?

        action = action.to_s
        attrs = %(action="#{escape(action)}" target="#{escape(target)}")

        return "<turbo-stream #{attrs}></turbo-stream>" if CONTENTLESS_ACTIONS.include?(action)

        content = render(renderable)
        "<turbo-stream #{attrs}><template>#{content}</template></turbo-stream>"
      end

      # Resolves a renderable to an HTML string. See module docs for the
      # resolution order. Returns "" for nil (a content action with no
      # renderable still emits an empty <template>).
      def render(renderable)
        return "" if renderable.nil?
        return renderable if renderable.is_a?(String)
        return renderable.call.to_s if renderable.respond_to?(:call)
        return renderable.render_in(nil).to_s if renderable.respond_to?(:render_in)

        renderable.to_s
      end

      def escape(value)
        CGI.escape_html(value.to_s)
      end
    end
  end
end
