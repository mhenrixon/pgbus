# frozen_string_literal: true

require "system_helper"

RSpec.describe "Jobs", type: :system do
  it "shows empty state for both tables" do
    visit "/pgbus/jobs"

    expect(page).to have_css("h1", text: "Jobs")
    expect(page).to have_text("No failed jobs")
    expect(page).to have_text("No enqueued jobs")
  end

  context "with failed jobs" do
    before do
      @stub_data_source.failed_events_list = [
        { "id" => 1, "queue_name" => "pgbus_default", "error_class" => "ArgumentError",
          "error_message" => "wrong number of arguments", "retry_count" => 3,
          "failed_at" => Time.now.utc.iso8601 },
        { "id" => 2, "queue_name" => "pgbus_critical", "error_class" => "TimeoutError",
          "error_message" => "connection timed out", "retry_count" => 1,
          "failed_at" => Time.now.utc.iso8601 }
      ]
    end

    it "displays failed jobs with details" do
      visit "/pgbus/jobs"

      expect(page).to have_text("ArgumentError")
      expect(page).to have_text("TimeoutError")
      expect(page).to have_text("pgbus_default")
      expect(page).to have_text("pgbus_critical")
    end

    it "shows bulk action buttons" do
      visit "/pgbus/jobs"

      expect(page).to have_button("Retry All")
      expect(page).to have_button("Discard All")
    end

    it "shows per-job action buttons" do
      visit "/pgbus/jobs"

      expect(page).to have_button("Retry", minimum: 2)
      expect(page).to have_button("Discard", minimum: 2)
    end
  end

  context "with enqueued jobs" do
    before do
      @stub_data_source.jobs_list = [
        { msg_id: 42, queue_name: "pgbus_default", read_ct: 0,
          enqueued_at: Time.now.utc.iso8601, message: '{"job_class":"TestJob"}' }
      ]
    end

    it "displays enqueued jobs" do
      visit "/pgbus/jobs"

      expect(page).to have_text("42")
      expect(page).to have_text("TestJob")
    end

    it "shows discard and retry buttons in expanded row" do
      visit "/pgbus/jobs"

      find("details summary").click

      expect(page).to have_button("Discard")
      expect(page).to have_button("Retry")
    end

    it "shows Discard All button for enqueued jobs" do
      visit "/pgbus/jobs"

      within("turbo-frame#jobs-enqueued") do
        expect(page).to have_button("Discard All")
      end
    end

    it "discard all enqueued: confirm and redirects with toast" do
      visit "/pgbus/jobs"

      within("turbo-frame#jobs-enqueued") { click_button "Discard All" }
      accept_confirm_dialog

      expect(page).to have_toast("Discarded")
      expect(@stub_data_source).to be_called(:discard_all_enqueued)
    end

    it "discard enqueued job: confirm and redirects with toast" do
      visit "/pgbus/jobs"

      find("details summary").click
      click_button "Discard"
      accept_confirm_dialog

      expect(page).to have_toast("Message discarded")
      expect(@stub_data_source).to be_called(:discard_job)
    end

    it "retry enqueued job: confirm and redirects with toast" do
      visit "/pgbus/jobs"

      find("details summary").click
      click_button "Retry"
      accept_confirm_dialog

      expect(page).to have_toast("Message visibility reset")
      expect(@stub_data_source).to be_called(:retry_job)
    end
  end

  describe "failed job actions" do
    before do
      @stub_data_source.failed_events_list = [
        { "id" => 1, "queue_name" => "pgbus_default", "error_class" => "ArgumentError",
          "error_message" => "bad args", "retry_count" => 3,
          "failed_at" => Time.now.utc.iso8601 }
      ]
    end

    it "retry failed job: no confirm needed, shows toast" do
      visit "/pgbus/jobs"

      within("turbo-frame#jobs-failed") { click_button "Retry", match: :first }

      expect(page).to have_toast("re-enqueued")
      expect(@stub_data_source).to be_called(:retry_failed_event)
    end

    it "discard failed job: confirm and shows toast" do
      visit "/pgbus/jobs"

      within("turbo-frame#jobs-failed") { click_button "Discard", match: :first }
      accept_confirm_dialog

      expect(page).to have_toast("discarded")
      expect(@stub_data_source).to be_called(:discard_failed_event)
    end
  end
end
