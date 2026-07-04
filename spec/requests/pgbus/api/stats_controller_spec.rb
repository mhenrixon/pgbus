# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::Api::StatsController", type: :request do
  describe "GET /pgbus/api/stats" do
    it "returns 200 OK" do
      get "/pgbus/api/stats"
      expect(response).to have_http_status(:ok)
    end

    it "renders the data source summary_stats as JSON" do
      get "/pgbus/api/stats"
      expected = @stub_data_source.summary_stats.transform_keys(&:to_s)
      expect(response.parsed_body).to eq(expected)
    end
  end
end
