# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Renderer do
  describe ".render" do
    it "returns a String verbatim" do
      expect(described_class.render("<p>x</p>")).to eq("<p>x</p>")
    end

    it "calls #call on a Phlex-like renderable" do
      phlex_like = Class.new do
        def call = "<div>phlex</div>"
      end.new
      expect(described_class.render(phlex_like)).to eq("<div>phlex</div>")
    end

    it "calls #render_in on a ViewComponent/phlex-rails renderable" do
      vc_like = Class.new do
        def render_in(_view_context) = "<span>vc</span>"
      end.new
      expect(described_class.render(vc_like)).to eq("<span>vc</span>")
    end

    it "prefers #call over #render_in when both are present (Phlex shape)" do
      both = Class.new do
        def call = "from_call"
        def render_in(_) = "from_render_in"
      end.new
      expect(described_class.render(both)).to eq("from_call")
    end

    it "falls back to #to_s for anything else" do
      expect(described_class.render(42)).to eq("42")
    end

    it "returns an empty string for nil" do
      expect(described_class.render(nil)).to eq("")
    end

    it "coerces a non-String #call result to a string" do
      numeric_call = Class.new do
        def call = 123
      end.new
      expect(described_class.render(numeric_call)).to eq("123")
    end
  end

  describe ".turbo_stream_tag" do
    it "wraps rendered content in a <template> inside a turbo-stream action tag" do
      result = described_class.turbo_stream_tag(action: :append, target: "messages", renderable: "<li>m</li>")
      expect(result).to eq(
        '<turbo-stream action="append" target="messages"><template><li>m</li></template></turbo-stream>'
      )
    end

    it "coerces a symbol action to a string" do
      result = described_class.turbo_stream_tag(action: :replace, target: "t", renderable: "x")
      expect(result).to start_with('<turbo-stream action="replace"')
    end

    it "omits the <template> for content-less actions (remove)" do
      result = described_class.turbo_stream_tag(action: :remove, target: "item_9")
      expect(result).to eq('<turbo-stream action="remove" target="item_9"></turbo-stream>')
    end

    it "HTML-escapes the target attribute" do
      result = described_class.turbo_stream_tag(action: :replace, target: 'a"b', renderable: "x")
      expect(result).to include('target="a&quot;b"')
    end

    it "HTML-escapes the action attribute" do
      result = described_class.turbo_stream_tag(action: 'a"b', target: "t", renderable: "x")
      expect(result).to include('action="a&quot;b"')
    end

    it "emits an empty <template> when a content action has a nil renderable" do
      result = described_class.turbo_stream_tag(action: :update, target: "t", renderable: nil)
      expect(result).to eq('<turbo-stream action="update" target="t"><template></template></turbo-stream>')
    end

    it "raises when target is nil" do
      expect { described_class.turbo_stream_tag(action: :replace, target: nil, renderable: "x") }
        .to raise_error(ArgumentError, /target/)
    end

    it "raises when target is empty" do
      expect { described_class.turbo_stream_tag(action: :replace, target: "", renderable: "x") }
        .to raise_error(ArgumentError, /target/)
    end
  end
end
