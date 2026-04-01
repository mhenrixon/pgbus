# frozen_string_literal: true

module Pgbus
  class LocksController < ApplicationController
    def index
      @locks = data_source.job_locks
    end
  end
end
