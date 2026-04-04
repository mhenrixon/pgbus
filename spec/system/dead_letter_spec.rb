# frozen_string_literal: true

require "system_helper"

RSpec.describe "Dead Letter Queue", type: :system do
  it "shows empty state" do
    visit "/pgbus/dlq"

    expect(page).to have_css("h1", text: "Dead Letter Queue")
    expect(page).to have_text("Dead letter queue is empty")
  end

  context "with DLQ messages" do
    before do
      @stub_data_source.dlq_messages_list = [
        { msg_id: 99, queue_name: "pgbus_default_dlq", read_ct: 6,
          enqueued_at: Time.now.utc.iso8601, message: '{"job_class":"FailedJob"}' }
      ]
    end

    it "displays DLQ messages" do
      visit "/pgbus/dlq"

      expect(page).to have_text("99")
      expect(page).to have_text("pgbus_default")
      expect(page).to have_text("FailedJob")
    end

    it "shows bulk action buttons" do
      visit "/pgbus/dlq"

      expect(page).to have_button("Retry All")
      expect(page).to have_button("Discard All")
    end

    it "shows per-message actions" do
      visit "/pgbus/dlq"

      expect(page).to have_button("Retry")
      expect(page).to have_button("Discard")
    end

    it "shows checkboxes for DLQ messages" do
      visit "/pgbus/dlq"

      expect(page).to have_css("input[data-bulk-item]", count: 1)
      expect(page).to have_css("input[data-bulk-select-all]")
    end

    it "retry DLQ message: no confirm, shows toast" do
      visit "/pgbus/dlq"

      find("details summary").click
      click_button "Retry"

      expect(page).to have_toast("re-enqueued")
      expect(@stub_data_source).to be_called(:retry_dlq_message)
    end

    it "discard DLQ message: confirm and shows toast" do
      visit "/pgbus/dlq"

      find("details summary").click
      click_button "Discard"
      accept_confirm_dialog

      expect(page).to have_toast("discarded")
      expect(@stub_data_source).to be_called(:discard_dlq_message)
    end

    it "retry all: confirm and shows toast" do
      visit "/pgbus/dlq"

      click_button "Retry All"
      accept_confirm_dialog

      expect(page).to have_toast("Re-enqueued")
      expect(@stub_data_source).to be_called(:retry_all_dlq)
    end

    it "discard all: confirm and shows toast" do
      visit "/pgbus/dlq"

      click_button "Discard All"
      accept_confirm_dialog

      expect(page).to have_toast("Discarded")
      expect(@stub_data_source).to be_called(:discard_all_dlq)
    end
  end
end
