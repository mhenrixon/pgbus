# frozen_string_literal: true

require "rails_helper"

# Exercises Pgbus::Web::Authentication through a real ApplicationController-backed
# route (the dashboard root). Every dashboard/API controller inherits the same
# before_action, so proving the three auth branches here covers them all; the
# per-controller specs then assert their own behavior with auth passing.
RSpec.describe "Dashboard authentication", type: :request do
  describe "when web_auth rejects the request" do
    before { Pgbus.configuration.web_auth = ->(_req) { false } }

    it "returns 401 Unauthorized" do
      get "/pgbus"
      expect(response).to have_http_status(:unauthorized)
    end
  end

  describe "when web_auth accepts the request" do
    before { Pgbus.configuration.web_auth = ->(_req) { true } }

    it "returns 200 OK" do
      get "/pgbus"
      expect(response).to have_http_status(:ok)
    end
  end

  describe "when web_auth is nil (unauthenticated)" do
    before { Pgbus.configuration.web_auth = nil }

    it "allows access" do
      get "/pgbus"
      expect(response).to have_http_status(:ok)
    end

    it "logs the unauthenticated-dashboard warning once" do
      Pgbus::Web::Authentication.auth_warned = false
      messages = []
      allow(Pgbus.logger).to receive(:warn) { |&block| messages << block.call }

      get "/pgbus"
      get "/pgbus"

      expect(messages).to contain_exactly(a_string_including("accessible without authentication"))
    end
  end
end
