# frozen_string_literal: true

module Pgbus
  class DeadLetterController < ApplicationController
    def index
      @messages = data_source.dlq_messages(page: page_param, per_page: per_page)
    end

    def show
      @message = data_source.dlq_messages(page: 1, per_page: 1).find { |m| m[:msg_id] == params[:id].to_i }
    end

    def retry
      queue_name = params[:queue_name]
      if data_source.retry_dlq_message(queue_name, params[:id])
        redirect_to dead_letter_index_path, notice: "Message re-enqueued to original queue."
      else
        redirect_to dead_letter_index_path, alert: "Could not retry message."
      end
    end

    def discard
      queue_name = params[:queue_name]
      if data_source.discard_dlq_message(queue_name, params[:id])
        redirect_to dead_letter_index_path, notice: "Message discarded."
      else
        redirect_to dead_letter_index_path, alert: "Could not discard message."
      end
    end

    def retry_all
      redirect_to dead_letter_index_path, notice: "All DLQ messages re-enqueued."
    end

    def discard_all
      redirect_to dead_letter_index_path, notice: "All DLQ messages discarded."
    end
  end
end
