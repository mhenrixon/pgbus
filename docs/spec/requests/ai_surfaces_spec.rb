# frozen_string_literal: true

require "rails_helper"

# The agent-facing surfaces docs-kit derives from the same registry + render the
# HTML pages use: the llmstxt.org index, the full-text concatenation, and the
# search index. These lock in that they respond and reflect the authored pages.
RSpec.describe "AI surfaces", type: :request do
  let(:authored) { Doc.all.select(&:view_class) }

  describe "GET /llms.txt" do
    it "returns the llmstxt.org index with the brand and tagline" do
      get "/llms.txt"
      expect(response).to have_http_status(:ok)
      expect(response.body).to include("# pgbus")
      expect(response.body).to include("PostgreSQL-native")
    end

    it "lists every authored page as an absolute .md link" do
      get "/llms.txt"
      authored.each do |doc|
        expect(response.body).to include("/docs/#{doc.slug}.md"),
                                 "llms.txt is missing #{doc.slug}"
      end
    end

    it "advertises the live MCP endpoint" do
      get "/llms.txt"
      expect(response.body).to include("## MCP")
    end
  end

  describe "GET /llms-full.txt" do
    it "concatenates the authored pages" do
      get "/llms-full.txt"
      expect(response).to have_http_status(:ok)
      expect(response.body).to include("# Overview")
    end
  end

  describe "GET /docs/search" do
    it "answers a query" do
      get "/docs/search", params: { q: "queue" }
      expect(response).to have_http_status(:ok)
    end

    it "serves a JSON index for the command palette" do
      get "/docs/search.json", params: { q: "queue" }
      expect(response).to have_http_status(:ok)
      expect(response.media_type).to eq("application/json")
    end
  end
end
