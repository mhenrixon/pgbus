# frozen_string_literal: true

require "spec_helper"

require_relative "../../../app/helpers/pgbus/application_helper"

RSpec.describe Pgbus::ApplicationHelper do
  let(:helper) { Class.new { include Pgbus::ApplicationHelper }.new }

  describe "#pgbus_time_ago" do
    it "returns dash for nil" do
      expect(helper.pgbus_time_ago(nil)).to eq("—")
    end

    it "formats seconds" do
      expect(helper.pgbus_time_ago(Time.now - 30)).to eq("30s ago")
    end

    it "formats minutes" do
      expect(helper.pgbus_time_ago(Time.now - 120)).to eq("2m ago")
    end

    it "formats hours" do
      expect(helper.pgbus_time_ago(Time.now - 7200)).to eq("2h ago")
    end

    it "formats days" do
      expect(helper.pgbus_time_ago(Time.now - 172_800)).to eq("2d ago")
    end

    it "parses string timestamps" do
      expect(helper.pgbus_time_ago(Time.now.utc.iso8601)).to match(/\d+s ago/)
    end
  end

  describe "#pgbus_number" do
    it "returns 0 for nil" do
      expect(helper.pgbus_number(nil)).to eq("0")
    end

    it "formats small numbers" do
      expect(helper.pgbus_number(42)).to eq("42")
    end

    it "formats thousands" do
      expect(helper.pgbus_number(1500)).to eq("1.5K")
    end

    it "formats millions" do
      expect(helper.pgbus_number(2_500_000)).to eq("2.5M")
    end
  end

  describe "#pgbus_json_preview" do
    it "returns dash for nil" do
      expect(helper.pgbus_json_preview(nil)).to eq("—")
    end

    it "truncates long strings" do
      long = "a" * 200
      expect(helper.pgbus_json_preview(long, max_length: 50).length).to eq(53) # 50 + "..."
    end

    it "passes short strings through" do
      expect(helper.pgbus_json_preview("short")).to eq("short")
    end
  end
end
