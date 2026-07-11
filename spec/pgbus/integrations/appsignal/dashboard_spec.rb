# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe "Pgbus::Integrations::Appsignal dashboard" do
  let(:dashboard_path) do
    File.expand_path("../../../../lib/pgbus/integrations/appsignal/dashboard.json", __dir__)
  end

  let(:dashboard_json) { JSON.parse(File.read(dashboard_path)) }

  it "exists as a file" do
    expect(File.exist?(dashboard_path)).to be(true), "dashboard.json not found at #{dashboard_path}"
  end

  it "has a metric_keys array with at least one key" do
    expect(dashboard_json["metric_keys"]).to be_an(Array)
    expect(dashboard_json["metric_keys"]).not_to be_empty
  end

  it "has a dashboard object with title and description" do
    dashboard = dashboard_json["dashboard"]
    expect(dashboard["title"]).to eq("Pgbus")
    expect(dashboard["description"]).to be_a(String)
    expect(dashboard["description"].length).to be > 10
  end

  it "defines visuals as an array of timeseries charts" do
    visuals = dashboard_json["dashboard"]["visuals"]
    expect(visuals).to be_an(Array)
    expect(visuals.length).to be >= 6

    expect(visuals).to all(include("type" => "timeseries"))
    expect(visuals).to all(include("title" => a_kind_of(String)))
    expect(visuals).to all(include("metrics" => a_kind_of(Array)))
  end

  it "only references metrics with the pgbus_ prefix or transaction_duration" do
    visuals = dashboard_json["dashboard"]["visuals"]
    metric_names = visuals.flat_map { |v| v["metrics"].map { |m| m["name"] } }.uniq

    expect(metric_names).to all(match(/\A(pgbus_|transaction_duration)/))
  end

  it "uses valid field types" do
    valid_fields = %w[gauge counter mean count p90 p95 GAUGE COUNTER MEAN COUNT P90 P95]
    visuals = dashboard_json["dashboard"]["visuals"]
    all_fields = visuals.flat_map { |v| v["metrics"].flat_map { |m| m["fields"].map { |f| f["field"] } } }

    expect(all_fields).to all(be_in(valid_fields))
  end

  it "uses valid format types" do
    valid_formats = %w[number duration size throughput percent]
    formats = dashboard_json["dashboard"]["visuals"].map { |v| v["format"] }

    expect(formats).to all(be_in(valid_formats))
  end

  it "includes essential dashboard panels" do
    titles = dashboard_json["dashboard"]["visuals"].map { |v| v["title"] }

    expect(titles).to include(
      a_string_matching(/job.*status/i),
      a_string_matching(/queue.*depth|queue.*length/i),
      a_string_matching(/throughput/i),
      a_string_matching(/duration/i),
      a_string_matching(/dead.letter|dlq/i),
      a_string_matching(/worker|process/i)
    )
  end

  # The upstream review on appsignal/public_config#80 added job_class and
  # routing_key to the tag filters and line labels — without the wildcard tag
  # entry the %placeholder% in the line label never resolves.
  it "labels job status lines with job_class, queue, and status" do
    panel = dashboard_json["dashboard"]["visuals"].find { |v| v["title"] == "Job status per queue" }

    expect(panel["line_label"]).to eq("%job_class% - %queue% - %status%")
    tag_keys = panel["metrics"].first["tags"].map { |t| t["key"] }
    expect(tag_keys).to contain_exactly("job_class", "queue", "status")
  end

  it "labels event handler lines with handler, status, and routing_key" do
    panel = dashboard_json["dashboard"]["visuals"].find { |v| v["title"] == "Event handler status" }

    expect(panel["line_label"]).to eq("%handler% - %status% - %routing_key%")
    tag_keys = panel["metrics"].first["tags"].map { |t| t["key"] }
    expect(tag_keys).to contain_exactly("handler", "status", "routing_key")
  end

  it "uses metric_keys that match actual probe or subscriber metric names" do
    keys = dashboard_json["metric_keys"]
    all_metric_names = dashboard_json["dashboard"]["visuals"]
                       .flat_map { |v| v["metrics"].map { |m| m["name"] } }.uniq

    expect(keys).to all(be_in(all_metric_names))
  end
end
