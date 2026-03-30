# frozen_string_literal: true

module Pgbus
  class ProcessesController < ApplicationController
    def index
      @processes = data_source.processes
      render_frame("pgbus/processes/processes_table") if params[:frame] == "list"
    end
  end
end
