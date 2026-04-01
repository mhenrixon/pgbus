# frozen_string_literal: true

module Pgbus
  class OutboxController < ApplicationController
    def index
      @stats = data_source.outbox_stats
      @entries = data_source.outbox_entries(page: page_param, per_page: per_page)
    end
  end
end
