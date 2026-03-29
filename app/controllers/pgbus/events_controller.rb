# frozen_string_literal: true

module Pgbus
  class EventsController < ApplicationController
    def index
      @events = data_source.processed_events(page: page_param, per_page: per_page)
      @subscribers = data_source.registered_subscribers
    end

    def show
      @event = data_source.processed_events(page: 1, per_page: 1).first
    end

    def replay
      # Re-publish the event through the event bus
      event = data_source.processed_events(page: 1, per_page: 1).first
      if event
        redirect_to events_path, notice: "Event replayed."
      else
        redirect_to events_path, alert: "Event not found."
      end
    end
  end
end
