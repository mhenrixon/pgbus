# frozen_string_literal: true

module Pgbus
  class JobsController < ApplicationController
    def index
      if params[:frame] == "failed"
        @failed = data_source.failed_events(page: page_param, per_page: per_page)
        render_frame("pgbus/jobs/failed_table")
      elsif params[:frame] == "enqueued"
        @jobs = params[:status] == "failed" ? [] : data_source.jobs(queue_name: params[:queue], page: page_param, per_page: per_page)
        render_frame("pgbus/jobs/enqueued_table")
      else
        @jobs = params[:status] == "failed" ? [] : data_source.jobs(queue_name: params[:queue], page: page_param, per_page: per_page)
        @failed = data_source.failed_events(page: page_param, per_page: per_page)
      end
    end

    def show
      @job = data_source.failed_event(params[:id])
    end

    def retry
      if data_source.retry_failed_event(params[:id])
        redirect_to jobs_path, notice: "Job re-enqueued."
      else
        redirect_to jobs_path, alert: "Could not retry job."
      end
    end

    def discard
      if data_source.discard_failed_event(params[:id])
        redirect_to jobs_path, notice: "Job discarded."
      else
        redirect_to jobs_path, alert: "Could not discard job."
      end
    end

    def retry_all
      count = data_source.retry_all_failed
      redirect_to jobs_path, notice: "Re-enqueued #{count} jobs."
    end

    def discard_all
      count = data_source.discard_all_failed
      redirect_to jobs_path, notice: "Discarded #{count} jobs."
    end

    def discard_all_enqueued
      count = data_source.discard_all_enqueued
      redirect_to jobs_path, notice: t("pgbus.jobs.index.discard_all_enqueued_notice", count: count)
    end
  end
end
