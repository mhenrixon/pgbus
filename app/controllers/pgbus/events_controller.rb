# frozen_string_literal: true

module Pgbus
  class EventsController < ApplicationController
    def index
      @events = data_source.processed_events(page: page_param, per_page: per_page)
      @subscribers = data_source.registered_subscribers
    end

    def show
      @event = data_source.processed_event(params[:id])
    end

    def replay
      event = data_source.processed_event(params[:id])
      if event && data_source.replay_event(event)
        redirect_to events_path, notice: "Event replayed."
      else
        redirect_to events_path, alert: "Event not found or could not be replayed."
      end
    end
  end
end
