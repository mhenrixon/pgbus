# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::InsightsController", type: :request do
  describe "GET /pgbus/insights" do
    context "when latency columns are unavailable" do
      before { allow(Pgbus::JobStat).to receive(:latency_columns?).and_return(false) }

      it "renders the insights page" do
        get "/pgbus/insights"
        expect(response).to have_http_status(:ok)
      end
    end

    context "when latency columns are available" do
      before { allow(Pgbus::JobStat).to receive(:latency_columns?).and_return(true) }

      it "renders the insights page" do
        get "/pgbus/insights"
        expect(response).to have_http_status(:ok)
      end
    end

    context "when stream stats are available" do
      before do
        allow(Pgbus::JobStat).to receive(:latency_columns?).and_return(false)
        @stub_data_source.stream_stats_available = true
      end

      it "renders the insights page with stream stats" do
        get "/pgbus/insights"
        expect(response).to have_http_status(:ok)
      end
    end
  end
end
