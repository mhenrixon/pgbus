# frozen_string_literal: true

module Pgbus
  class BatchesController < ApplicationController
    def index
      @batches = data_source.batches
      render_frame("pgbus/batches/batches_table") if params[:frame] == "list"
    end

    def show
      @batch = data_source.batch_detail(params[:id])
      redirect_to batches_path, alert: t("pgbus.batches.show.not_found") unless @batch
    end
  end
end
