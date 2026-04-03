# frozen_string_literal: true

require "spec_helper"

RSpec.describe "return_to_app_url configuration" do # rubocop:disable RSpec/DescribeClass
  describe "default" do
    it "defaults to nil" do
      config = Pgbus::Configuration.new
      expect(config.return_to_app_url).to be_nil
    end
  end

  describe "custom value" do
    it "accepts a string URL" do
      config = Pgbus::Configuration.new
      config.return_to_app_url = "/admin"
      expect(config.return_to_app_url).to eq("/admin")
    end

    it "accepts a full URL" do
      config = Pgbus::Configuration.new
      config.return_to_app_url = "https://example.com/admin"
      expect(config.return_to_app_url).to eq("https://example.com/admin")
    end
  end
end
