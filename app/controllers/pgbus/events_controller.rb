# frozen_string_literal: true

module Pgbus
  class EventsController < ApplicationController
    def index
      @events = data_source.processed_events(page: page_param, per_page: per_page)
      @subscribers = data_source.registered_subscribers
      @pending = data_source.pending_events(page: page_param, per_page: per_page)
    end

    def show
      @event = data_source.processed_event(params[:id])
    end

    def replay
      event = data_source.processed_event(params[:id])
      if event && data_source.replay_event(event)
        redirect_to events_path, notice: t("pgbus.events.flash.replayed")
      else
        redirect_to events_path, alert: t("pgbus.events.flash.replay_failed")
      end
    end

    def discard
      queue_name = params[:queue_name].to_s
      if data_source.discard_event(queue_name, params[:id])
        redirect_to events_path, notice: t("pgbus.events.flash.discarded")
      else
        redirect_to events_path, alert: t("pgbus.events.flash.discard_failed")
      end
    end

    def mark_handled
      queue_name = params[:queue_name].to_s
      # Resolve the handler class server-side from the subscriber registry
      # instead of trusting params[:handler_class] — otherwise a crafted POST
      # could create ProcessedEvent markers for arbitrary class names and
      # corrupt replay/idempotency state.
      handler_class = data_source.handler_class_for_queue(queue_name)
      if handler_class && data_source.mark_event_handled(queue_name, params[:id], handler_class)
        redirect_to events_path, notice: t("pgbus.events.flash.marked_handled")
      else
        redirect_to events_path, alert: t("pgbus.events.flash.mark_handled_failed")
      end
    end

    def edit_payload
      queue_name = params[:queue_name].to_s
      new_payload = params[:payload].to_s
      if data_source.edit_event_payload(queue_name, params[:id], new_payload)
        redirect_to events_path, notice: t("pgbus.events.flash.payload_updated")
      else
        redirect_to events_path, alert: t("pgbus.events.flash.payload_update_failed")
      end
    end

    def reroute
      source_queue = params[:queue_name].to_s
      target_queue = params[:target_queue].to_s
      # Reject rerouting to any queue that isn't a registered handler —
      # the UI dropdown is not a real server-side constraint, so a crafted
      # POST could otherwise move messages into arbitrary queues.
      unless data_source.handler_queue_physical_names.include?(target_queue)
        return redirect_to events_path, alert: t("pgbus.events.flash.reroute_failed")
      end

      if data_source.reroute_event(source_queue, params[:id], target_queue)
        redirect_to events_path, notice: t("pgbus.events.flash.rerouted")
      else
        redirect_to events_path, alert: t("pgbus.events.flash.reroute_failed")
      end
    end

    def discard_selected
      selections = Array(params[:messages]).reject { |s| s[:queue_name].blank? || s[:msg_id].blank? }
      if selections.empty?
        redirect_to events_path, alert: t("pgbus.events.flash.none_selected")
        return
      end

      count = data_source.discard_selected_events(
        selections.map { |s| { queue_name: s[:queue_name].to_s, msg_id: s[:msg_id] } }
      )
      redirect_to events_path, notice: t("pgbus.events.flash.discarded_selected", count: count)
    end
  end
end
