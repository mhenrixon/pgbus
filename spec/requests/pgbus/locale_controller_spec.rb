# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::LocaleController", type: :request do
  describe "GET /pgbus/set_locale" do
    context "with a supported locale" do
      it "sets the locale cookie and redirects back to the fallback" do
        get "/pgbus/set_locale", params: { locale: "en" }
        expect(response).to redirect_to("/pgbus/")
        expect(response.cookies["pgbus_locale"]).to eq("en")
      end
    end

    context "with an unsupported locale" do
      it "redirects without setting the cookie" do
        get "/pgbus/set_locale", params: { locale: "xx" }
        expect(response).to redirect_to("/pgbus/")
        expect(response.cookies["pgbus_locale"]).to be_nil
      end
    end
  end
end
