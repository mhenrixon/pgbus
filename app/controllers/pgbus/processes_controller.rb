# frozen_string_literal: true

module Pgbus
  class ProcessesController < ApplicationController
    def index
      @processes = data_source.processes
    end
  end
end
