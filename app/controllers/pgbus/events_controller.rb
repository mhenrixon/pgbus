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
      return reject_unknown_queue(:discard_failed) unless registered_queue?(queue_name)

      if data_source.discard_event(queue_name, params[:id])
        redirect_to events_path, notice: t("pgbus.events.flash.discarded")
      else
        redirect_to events_path, alert: t("pgbus.events.flash.discard_failed")
      end
    end

    def mark_handled
      queue_name = params[:queue_name].to_s
      return reject_unknown_queue(:mark_handled_failed) unless registered_queue?(queue_name)

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
      return reject_unknown_queue(:payload_update_failed) unless registered_queue?(queue_name)

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
      # Reject rerouting to/from any queue that isn't a registered handler —
      # the UI dropdown is not a real server-side constraint, so a crafted
      # POST could otherwise touch arbitrary queues.
      allowed = registered_queue?(source_queue) && registered_queue?(target_queue)
      return reject_unknown_queue(:reroute_failed) unless allowed

      if data_source.reroute_event(source_queue, params[:id], target_queue)
        redirect_to events_path, notice: t("pgbus.events.flash.rerouted")
      else
        redirect_to events_path, alert: t("pgbus.events.flash.reroute_failed")
      end
    end

    def discard_selected
      # Guard against non-hash entries in params[:messages] — a crafted POST
      # can otherwise raise NoMethodError on [:queue_name]. Mirrors the
      # hardening already used in jobs_controller#discard_selected_enqueued.
      # Also whitelist queue_name per selection so operators can't archive
      # arbitrary non-handler queues through this endpoint.
      allowed = registered_queues
      selections = Array(params[:messages]).filter_map do |s|
        next unless s.respond_to?(:[])

        queue_name = s[:queue_name]
        msg_id = s[:msg_id]
        next if queue_name.blank? || msg_id.blank?
        next unless allowed.include?(queue_name.to_s)

        { queue_name: queue_name.to_s, msg_id: msg_id }
      end
      if selections.empty?
        redirect_to events_path, alert: t("pgbus.events.flash.none_selected")
        return
      end

      count = data_source.discard_selected_events(selections)
      redirect_to events_path, notice: t("pgbus.events.flash.discarded_selected", count: count)
    end

    private

    def registered_queues
      @registered_queues ||= data_source.handler_queue_physical_names
    end

    def registered_queue?(name)
      registered_queues.include?(name)
    end

    def reject_unknown_queue(flash_key)
      redirect_to events_path, alert: t("pgbus.events.flash.#{flash_key}")
    end
  end
end
