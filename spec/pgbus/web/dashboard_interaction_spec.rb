# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Dashboard interaction UX" do # rubocop:disable RSpec/DescribeClass
  let(:root) { Pathname.new(File.expand_path("../../..", __dir__)) }
  let(:views_dir) { root.join("app", "views") }
  let(:frontend_dir) { root.join("app", "frontend", "pgbus") }

  describe "auto-refresh pauses on user interaction" do
    let(:application_js) { frontend_dir.join("application.js").read }

    it "checks for open details elements before refreshing" do
      expect(application_js).to include("details[open]")
    end

    it "checks for checked checkboxes before refreshing" do
      expect(application_js).to include("data-bulk-item]:checked")
    end

    it "skips refresh when user interaction is detected" do
      expect(application_js).to include("hasUserInteraction")
    end
  end

  describe "recurring tasks table links" do
    let(:tasks_table) { views_dir.join("pgbus", "recurring_tasks", "_tasks_table.html.erb").read }

    it "uses turbo_frame _top for task links to avoid Content missing" do
      expect(tasks_table).to include("turbo_frame: \"_top\"")
    end
  end

  describe "queue show page" do
    let(:queue_show) { views_dir.join("pgbus", "queues", "show.html.erb").read }

    it "uses expandable details/summary for message rows" do
      expect(queue_show).to include("<details class=\"group\">")
      expect(queue_show).to include("<summary")
    end

    it "shows job metadata in expanded section" do
      expect(queue_show).to include("job_id")
      expect(queue_show).to include("arguments")
      expect(queue_show).to include("metadata")
    end

    it "shows full JSON payload in nested details" do
      expect(queue_show).to include("full_json_payload")
    end

    it "shows headers section when available" do
      expect(queue_show).to include("headers_section")
    end

    it "includes retry and discard actions in expanded section" do
      expect(queue_show).to include("retry_message_queue_path")
      expect(queue_show).to include("discard_message_queue_path")
    end
  end
end
