# frozen_string_literal: true

module Pgbus
  class QueuesController < ApplicationController
    def index
      @queues = data_source.queues_with_metrics
    end

    def show
      @queue = data_source.queue_detail(params[:name])
      @messages = data_source.jobs(queue_name: params[:name], page: page_param, per_page: per_page)
    end

    def purge
      data_source.purge_queue(params[:name])
      redirect_to queue_path(name: params[:name]), notice: "Queue purged."
    end
  end
end
