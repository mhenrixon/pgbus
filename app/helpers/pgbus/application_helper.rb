# frozen_string_literal: true

require "time"

module Pgbus
  module ApplicationHelper
    def pgbus_time_ago(time)
      return "—" unless time

      time = Time.parse(time) if time.is_a?(String)
      seconds = (Time.now - time).to_i

      case seconds
      when 0..59 then "#{seconds}s ago"
      when 60..3599 then "#{seconds / 60}m ago"
      when 3600..86_399 then "#{seconds / 3600}h ago"
      else "#{seconds / 86_400}d ago"
      end
    end

    def pgbus_number(n)
      return "0" unless n

      n = n.to_i
      case n
      when 0..999 then n.to_s
      when 1_000..999_999 then "#{(n / 1_000.0).round(1)}K"
      else "#{(n / 1_000_000.0).round(1)}M"
      end
    end

    def pgbus_status_badge(healthy)
      if healthy
        tag.span("Healthy", class: "inline-flex items-center rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-800")
      else
        tag.span("Stale", class: "inline-flex items-center rounded-full bg-red-100 px-2.5 py-0.5 text-xs font-medium text-red-800")
      end
    end

    def pgbus_queue_badge(name)
      if name.to_s.end_with?("_dlq")
        tag.span("DLQ", class: "inline-flex items-center rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-700")
      else
        tag.span("Queue", class: "inline-flex items-center rounded-full bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-700")
      end
    end

    def pgbus_parse_message(message)
      return {} unless message

      case message
      when Hash then message
      when String then JSON.parse(message)
      else {}
      end
    rescue JSON::ParserError
      {}
    end

    def pgbus_json_preview(json_string, max_length: 120)
      return "—" unless json_string

      text = json_string.is_a?(String) ? json_string : JSON.generate(json_string)
      text.length > max_length ? "#{text[0...max_length]}..." : text
    end

    def pgbus_refresh_interval
      Pgbus.configuration.web_refresh_interval
    end

    def pgbus_time_ago_future(time)
      return "—" unless time

      time = Time.parse(time) if time.is_a?(String)
      seconds = (time - Time.now).to_i

      if seconds <= 0
        "now"
      elsif seconds < 60
        "in #{seconds}s"
      elsif seconds < 3600
        "in #{seconds / 60}m"
      elsif seconds < 86_400
        "in #{seconds / 3600}h"
      else
        "in #{seconds / 86_400}d"
      end
    end

    def pgbus_recurring_health_badge(task)
      if task[:last_run_at].nil?
        tag.span("Pending",
                 class: "inline-flex items-center rounded-full bg-yellow-100 px-2.5 py-0.5 text-xs font-medium text-yellow-800")
      else
        tag.span("Active",
                 class: "inline-flex items-center rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-800")
      end
    end

    def pgbus_nav_link(label, path)
      active = request.path == path || (path != pgbus.root_path && request.path.start_with?(path))
      css = if active
              "rounded-md px-3 py-2 text-sm font-medium text-white bg-gray-800"
            else
              "rounded-md px-3 py-2 text-sm font-medium text-gray-300 hover:text-white hover:bg-gray-700"
            end
      link_to label, path, class: css
    end
  end
end
