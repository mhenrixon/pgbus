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

  describe "#pgbus_duration" do
    it "returns dash for nil" do
      expect(helper.pgbus_duration(nil)).to eq("—")
    end

    it "formats seconds" do
      expect(helper.pgbus_duration(45)).to eq("45s")
    end

    it "formats minutes and seconds" do
      expect(helper.pgbus_duration(125)).to eq("2m 5s")
    end

    it "formats hours and minutes" do
      expect(helper.pgbus_duration(3725)).to eq("1h 2m")
    end

    it "formats days and hours" do
      expect(helper.pgbus_duration(90_000)).to eq("1d 1h")
    end
  end

  describe "#pgbus_ms_duration" do
    it "returns dash for nil" do
      expect(helper.pgbus_ms_duration(nil)).to eq("—")
    end

    it "formats milliseconds" do
      expect(helper.pgbus_ms_duration(42)).to eq("42ms")
    end

    it "formats seconds" do
      expect(helper.pgbus_ms_duration(1500)).to eq("1.5s")
    end

    it "formats minutes" do
      expect(helper.pgbus_ms_duration(120_000)).to eq("2.0m")
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
