# frozen_string_literal: true

module Pgbus
  class DeadLetterController < ApplicationController
    def index
      @messages = data_source.dlq_messages(page: page_param, per_page: per_page)
    end

    def show
      @message = data_source.dlq_message_detail(params[:id].to_i)
    end

    def retry
      queue_name = params[:queue_name].to_s
      unless queue_name.end_with?(Pgbus.configuration.dead_letter_queue_suffix)
        return redirect_to dead_letter_index_path, alert: "Invalid DLQ queue."
      end

      if data_source.retry_dlq_message(queue_name, params[:id])
        redirect_to dead_letter_index_path, notice: "Message re-enqueued to original queue."
      else
        redirect_to dead_letter_index_path, alert: "Could not retry message."
      end
    end

    def discard
      queue_name = params[:queue_name].to_s
      unless queue_name.end_with?(Pgbus.configuration.dead_letter_queue_suffix)
        return redirect_to dead_letter_index_path, alert: "Invalid DLQ queue."
      end

      if data_source.discard_dlq_message(queue_name, params[:id])
        redirect_to dead_letter_index_path, notice: "Message discarded."
      else
        redirect_to dead_letter_index_path, alert: "Could not discard message."
      end
    end

    def retry_all
      count = data_source.retry_all_dlq
      redirect_to dead_letter_index_path, notice: "Re-enqueued #{count} DLQ messages."
    end

    def discard_all
      count = data_source.discard_all_dlq
      redirect_to dead_letter_index_path, notice: "Discarded #{count} DLQ messages."
    end
  end
end
