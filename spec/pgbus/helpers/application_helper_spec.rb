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

    it "formats exact second boundary" do
      expect(helper.pgbus_ms_duration(1000)).to eq("1.0s")
    end

    it "formats exact minute boundary" do
      expect(helper.pgbus_ms_duration(60_000)).to eq("1.0m")
    end

    it "rounds just below minute boundary" do
      expect(helper.pgbus_ms_duration(59_950)).to eq("60.0s")
    end
  end

  describe "#pgbus_time_range_label" do
    it "returns '1 hour' for 60 minutes" do
      expect(helper.pgbus_time_range_label(60)).to eq("1 hour")
    end

    it "returns '24 hours' for 1440 minutes" do
      expect(helper.pgbus_time_range_label(1440)).to eq("24 hours")
    end

    it "returns '7 days' for 10080 minutes" do
      expect(helper.pgbus_time_range_label(10_080)).to eq("7 days")
    end

    it "returns '30 days' for 43200 minutes" do
      expect(helper.pgbus_time_range_label(43_200)).to eq("30 days")
    end

    it "returns hours for sub-day ranges" do
      expect(helper.pgbus_time_range_label(360)).to eq("6 hours")
    end

    it "returns minutes for sub-hour ranges" do
      expect(helper.pgbus_time_range_label(15)).to eq("15 minutes")
    end

    it "returns singular minute for 1" do
      expect(helper.pgbus_time_range_label(1)).to eq("1 minute")
    end

    it "clamps zero to 1 minute" do
      expect(helper.pgbus_time_range_label(0)).to eq("1 minute")
    end

    it "clamps negative to 1 minute" do
      expect(helper.pgbus_time_range_label(-5)).to eq("1 minute")
    end

    it "keeps non-divisible hour values as minutes" do
      expect(helper.pgbus_time_range_label(90)).to eq("90 minutes")
    end

    it "keeps non-divisible day values as hours" do
      expect(helper.pgbus_time_range_label(1500)).to eq("25 hours")
    end

    it "falls back to minutes for values not divisible by 60 above a day" do
      expect(helper.pgbus_time_range_label(1530)).to eq("1530 minutes")
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
