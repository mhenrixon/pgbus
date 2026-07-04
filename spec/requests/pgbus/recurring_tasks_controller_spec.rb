# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::RecurringTasksController", type: :request do
  describe "GET /pgbus/recurring_tasks" do
    it "renders the recurring tasks index" do
      get "/pgbus/recurring_tasks"
      expect(response).to have_http_status(:ok)
    end

    it "renders the recurring_tasks turbo frame" do
      get "/pgbus/recurring_tasks", params: { frame: "recurring_tasks" }
      expect(response).to have_http_status(:ok)
    end
  end

  describe "GET /pgbus/recurring_tasks/:id" do
    context "when the task exists" do
      before { @stub_data_source.recurring_tasks_list = [{ id: "daily", enabled: true }] }

      it "renders the task detail" do
        get "/pgbus/recurring_tasks/daily"
        expect(response).to have_http_status(:ok)
      end
    end

    context "when the task is unknown" do
      it "redirects to the index with an alert" do
        get "/pgbus/recurring_tasks/missing"
        expect(response).to redirect_to("/pgbus/recurring_tasks")
        expect(flash[:alert]).to eq("Task not found")
      end
    end
  end

  describe "POST /pgbus/recurring_tasks/:id/toggle" do
    context "when the task exists" do
      before { @stub_data_source.recurring_tasks_list = [{ id: "daily", enabled: true }] }

      it "toggles the task and redirects with a notice" do
        post "/pgbus/recurring_tasks/daily/toggle"
        expect(response).to redirect_to("/pgbus/recurring_tasks")
        expect(flash[:notice]).to be_present
        expect(@stub_data_source.calls[:toggle_recurring_task]).to eq([["daily"]])
      end
    end

    context "when the task is unknown" do
      it "redirects with a failure alert" do
        post "/pgbus/recurring_tasks/missing/toggle"
        expect(response).to redirect_to("/pgbus/recurring_tasks")
        expect(flash[:alert]).to be_present
      end
    end
  end

  describe "POST /pgbus/recurring_tasks/:id/enqueue" do
    it "enqueues the task now and redirects with a notice" do
      post "/pgbus/recurring_tasks/daily/enqueue"
      expect(response).to redirect_to("/pgbus/recurring_tasks")
      expect(flash[:notice]).to eq("Task enqueued")
      expect(@stub_data_source.calls[:enqueue_recurring_task_now]).to eq([["daily"]])
    end
  end
end
