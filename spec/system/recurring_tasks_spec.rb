# frozen_string_literal: true

require "system_helper"

RSpec.describe "Recurring Tasks", type: :system do
  let(:tasks) do
    [
      { id: 1, key: "capture_stats", class_name: "CaptureStatsJob", command: nil,
        schedule: "*/5 * * * *", human_schedule: "Every 5 minutes",
        queue_name: "default", priority: 2, description: "Capture usage stats",
        enabled: true, static: true,
        next_run_at: 5.minutes.from_now, last_run_at: 2.minutes.ago,
        created_at: 1.day.ago, updated_at: 1.hour.ago },
      { id: 2, key: "cleanup_old", class_name: "CleanupJob", command: nil,
        schedule: "0 3 * * *", human_schedule: "Daily at 3:00 AM",
        queue_name: "maintenance", priority: 5, description: nil,
        enabled: false, static: false,
        next_run_at: nil, last_run_at: 1.day.ago,
        created_at: 2.days.ago, updated_at: 2.days.ago }
    ]
  end

  before do
    @stub_data_source.recurring_tasks_list = tasks
  end

  describe "index page" do
    it "lists all recurring tasks" do
      visit "/pgbus/recurring_tasks"

      expect(page).to have_css("h1", text: "Recurring Tasks")
      expect(page).to have_text("capture_stats")
      expect(page).to have_text("cleanup_old")
      expect(page).to have_text("2 tasks configured")
    end

    it "shows task details in table" do
      visit "/pgbus/recurring_tasks"

      expect(page).to have_text("CaptureStatsJob")
      expect(page).to have_text("*/5 * * * *")
      expect(page).to have_text("Every 5 minutes")
    end
  end

  describe "task link navigation" do
    it "navigates to show page without Content missing" do
      visit "/pgbus/recurring_tasks"

      click_link "capture_stats"

      expect(page).to have_css("h1", text: "capture_stats")
      expect(page).to have_text("CaptureStatsJob")
      expect(page).to have_text("Configuration")
      expect(page).not_to have_text("Content missing")
    end
  end

  describe "show page" do
    it "displays task configuration" do
      visit "/pgbus/recurring_tasks/1"

      expect(page).to have_css("h1", text: "capture_stats")
      expect(page).to have_text("*/5 * * * *")
      expect(page).to have_text("CaptureStatsJob")
      expect(page).to have_text("Capture usage stats")
    end

    it "shows enable/disable and run now buttons" do
      visit "/pgbus/recurring_tasks/1"

      expect(page).to have_button("Disable")
      expect(page).to have_button("Run Now")
    end

    it "shows enable button for disabled tasks" do
      visit "/pgbus/recurring_tasks/2"

      expect(page).to have_button("Enable")
      expect(page).not_to have_button("Run Now")
    end
  end
end
