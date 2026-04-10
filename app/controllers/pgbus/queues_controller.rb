# frozen_string_literal: true

module Pgbus
  class QueuesController < ApplicationController
    def index
      @queues = data_source.queues_with_metrics
    end

    def show
      @queue = data_source.queue_detail(params[:name])
      redirect_to queues_path, alert: "Queue not found." and return unless @queue

      @paused = data_source.queue_paused?(params[:name])
      @messages = data_source.jobs(queue_name: params[:name], page: page_param, per_page: per_page)
      @health = data_source.queue_health_detail(params[:name])
    end

    def purge
      data_source.purge_queue(params[:name])
      redirect_to queue_path(name: params[:name]), notice: "Queue purged."
    end

    def destroy
      data_source.drop_queue(params[:name])
      redirect_to queues_path, notice: t("pgbus.queues.destroy.success", name: params[:name])
    end

    def pause
      data_source.pause_queue(params[:name], reason: params[:reason])
      redirect_to queue_path(name: params[:name]), notice: "Queue paused."
    end

    def resume
      data_source.resume_queue(params[:name])
      redirect_to queue_path(name: params[:name]), notice: "Queue resumed."
    end

    def retry_message
      if data_source.retry_job(params[:name], params[:msg_id])
        redirect_back fallback_location: queue_path(name: params[:name]), notice: t("pgbus.queues.show.message_retried")
      else
        redirect_back fallback_location: queue_path(name: params[:name]), alert: t("pgbus.queues.show.message_retry_failed")
      end
    end

    def discard_message
      if data_source.discard_job(params[:name], params[:msg_id])
        redirect_back fallback_location: queue_path(name: params[:name]), notice: t("pgbus.queues.show.message_discarded")
      else
        redirect_back fallback_location: queue_path(name: params[:name]), alert: t("pgbus.queues.show.message_discard_failed")
      end
    end
  end
end
