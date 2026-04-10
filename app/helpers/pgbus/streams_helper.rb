# frozen_string_literal: true

require "cgi"

module Pgbus
  # View helper for subscribing a page to one or more pgbus streams. This
  # is the drop-in replacement for `turbo_stream_from` — same API, same
  # streamable resolution (GlobalID objects, symbols, arrays), same
  # signed-name verification path — but the rendered element speaks to
  # `Pgbus::Web::StreamApp` via SSE instead of ActionCable.
  #
  # The critical difference from `turbo_stream_from` is the `since-id`
  # attribute: it carries the current PGMQ `msg_id` watermark at render
  # time so the streamer can replay anything published in the gap between
  # the controller render and the client connecting. This is the fix for
  # rails/rails#52420.
  module StreamsHelper
    # Renders a <pgbus-stream-source> custom element. The element's JS
    # (shipped separately under app/javascript/pgbus/stream_source_element.js)
    # opens an SSE connection to /pgbus/streams/<signed-name>?since=<cursor>,
    # listens for messages, and forwards each turbo-stream HTML payload
    # into Turbo via `connectStreamSource`.
    #
    # The `replay:` option controls which messages get delivered on the
    # initial connect:
    #   - :watermark (default) — only broadcasts published after this
    #     render's moment (the page-born-stale fix). since_id = current_msg_id.
    #   - :all — deliver every message still in PGMQ retention. since_id = 0.
    #     Useful for chat rooms where the page should show backlog on load.
    #   - N (Integer) — deliver the last N messages. since_id = max(0, current_msg_id - N).
    #     Useful when the backlog is large and you want a capped history.
    def pgbus_stream_from(*streamables, replay: :watermark, **html_attributes)
      stream_name = Pgbus::Streams::Stream.name_from(streamables.length == 1 ? streamables.first : streamables)
      signed_name = Pgbus::Streams::SignedName.sign(stream_name)

      since_id = compute_since_id(stream_name, replay)

      attributes = {
        "src" => pgbus_stream_src(signed_name),
        "signed-stream-name" => signed_name,
        "since-id" => since_id.to_s,
        # Compatibility shim: turbo-rails' cable_stream_source_element reads
        # `channel` to decide which ActionCable channel to subscribe to.
        # We emit the same attribute so a page that mistakenly uses the
        # turbo-rails element still renders (with ActionCable semantics).
        "channel" => "Turbo::StreamsChannel"
      }.merge(html_attributes.transform_keys(&:to_s))

      element = render_tag("pgbus-stream-source", attributes)
      script = pgbus_stream_source_script_tag
      return element unless script

      safe_concat(script, element)
    end

    private

    def compute_since_id(stream_name, replay)
      case replay
      when :watermark
        # The page-born-stale fix: capture MAX(msg_id) at render time.
        fetch_watermark(stream_name)
      when :all
        # Replay everything still in retention. The streamer's read_after
        # handles both live and archive tables, so this pulls the full
        # backlog bounded by `streams_retention`.
        0
      when Integer
        raise ArgumentError, "replay: must be non-negative (got #{replay})" if replay.negative?

        # Clamp to 0 so a fresh queue with replay: 1000 doesn't return
        # a negative cursor. fetch_watermark goes through the per-request
        # thread-local cache, so mixing :watermark and :N in the same
        # render only queries once.
        watermark = fetch_watermark(stream_name)
        [watermark - replay, 0].max
      else
        raise ArgumentError, "replay: must be :watermark, :all, or a non-negative Integer (got #{replay.inspect})"
      end
    end

    # Build the SSE endpoint URL for the given signed stream name.
    #
    # Resolution order:
    #   1. `config.streams_path` — explicit override, useful when the
    #      engine is mounted behind an auth constraint but the SSE
    #      endpoint is mounted publicly at a separate path:
    #
    #        # config/routes.rb
    #        authenticate :user, ->(u) { u.admin? } do
    #          mount Pgbus::Engine => "/admin/jobs"
    #        end
    #        mount Pgbus::Web::StreamApp.new => "/pgbus/streams"
    #
    #        # config/initializers/pgbus.rb
    #        Pgbus.configure { |c| c.streams_path = "/pgbus/streams" }
    #
    #   2. Engine route helper — derives the path from wherever the
    #      host app mounted the engine.
    #
    #   3. Fallback `/pgbus/streams` — test-only context where the
    #      engine's url_helpers aren't wired in.
    def pgbus_stream_src(signed_name)
      base = Pgbus.configuration.streams_path
      return "#{base.delete_suffix("/")}/#{signed_name}" if base

      base = Pgbus::Engine.routes.url_helpers.streams_path
      "#{base}/#{signed_name}"
    rescue NameError
      "/pgbus/streams/#{signed_name}"
    end

    def fetch_watermark(stream_name)
      # Avoid hitting Postgres multiple times within a single render when
      # the page uses several pgbus_stream_from helpers. We cache per
      # thread-local for the duration of the current request — Rails'
      # RequestStore gem is the idiomatic fit but we don't want to add
      # a runtime dep, so a plain Thread.current hash is used. The engine
      # initializer clears this hash between requests via a Rack middleware
      # (Phase 4.5).
      cache = Thread.current[:pgbus_streams_watermark_cache] ||= {}
      cache[stream_name] ||= Pgbus.stream(stream_name).current_msg_id
    end

    # Emits a <script type="module"> tag that imports the custom element
    # definition exactly once per request. Without this, <pgbus-stream-source>
    # is an inert unknown element — no SSE connection opens. Uses a
    # thread-local flag cleared by the WatermarkCacheMiddleware.
    def pgbus_stream_source_script_tag
      cache = Thread.current[:pgbus_streams_watermark_cache] ||= {}
      return nil if cache[:script_emitted]

      cache[:script_emitted] = true
      script = '<script type="module">import "pgbus/stream_source_element"</script>'
      script.respond_to?(:html_safe) ? script.html_safe : script
    end

    # Concatenates two HTML-safe strings without losing the safety flag.
    # ActiveSupport::SafeBuffer#+ preserves safety when both operands
    # are safe. Plain string interpolation ("#{a}#{b}") creates a new
    # String, dropping html_safe — which causes Phlex and safe_join to
    # HTML-escape the output.
    def safe_concat(*parts)
      if defined?(ActiveSupport::SafeBuffer)
        buf = ActiveSupport::SafeBuffer.new
        parts.each { |p| buf.safe_concat(p) }
        buf
      else
        parts.join
      end
    end

    def render_tag(name, attributes)
      attr_string = attributes.map { |k, v| %(#{k}="#{CGI.escape_html(v.to_s)}") }.join(" ")
      html = "<#{name} #{attr_string}></#{name}>"
      html.respond_to?(:html_safe) ? html.html_safe : html
    end
  end
end
