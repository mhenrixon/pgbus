# frozen_string_literal: true

module Pgbus
  class RecurringTasksController < ApplicationController
    def index
      case params[:frame]
      when "recurring_tasks"
        @recurring_tasks = data_source.recurring_tasks
        render_frame("pgbus/recurring_tasks/tasks_table")
      else
        @recurring_tasks = data_source.recurring_tasks
      end
    end

    def show
      @task = data_source.recurring_task(params[:id])
      redirect_to pgbus.recurring_tasks_path, alert: "Task not found" unless @task
    end

    def toggle
      if data_source.toggle_recurring_task(params[:id])
        redirect_to pgbus.recurring_tasks_path, notice: "Task toggled"
      else
        redirect_to pgbus.recurring_tasks_path, alert: "Failed to toggle task"
      end
    end

    def enqueue
      if data_source.enqueue_recurring_task_now(params[:id])
        redirect_to pgbus.recurring_tasks_path, notice: "Task enqueued"
      else
        redirect_to pgbus.recurring_tasks_path, alert: "Failed to enqueue task"
      end
    end
  end
end
