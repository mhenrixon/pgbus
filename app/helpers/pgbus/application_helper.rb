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
        tag.span(I18n.t("pgbus.helpers.status_badge.healthy"),
                 class: "inline-flex items-center rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-800")
      else
        tag.span(I18n.t("pgbus.helpers.status_badge.stale"),
                 class: "inline-flex items-center rounded-full bg-red-100 px-2.5 py-0.5 text-xs font-medium text-red-800")
      end
    end

    def pgbus_queue_badge(name)
      if name.to_s.end_with?("_dlq")
        tag.span(I18n.t("pgbus.helpers.queue_badge.dlq"),
                 class: "inline-flex items-center rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-700")
      else
        tag.span(I18n.t("pgbus.helpers.queue_badge.queue"),
                 class: "inline-flex items-center rounded-full bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-700")
      end
    end

    def pgbus_duration(seconds)
      return "—" unless seconds

      seconds = seconds.to_i
      if seconds < 60
        "#{seconds}s"
      elsif seconds < 3600
        "#{seconds / 60}m #{seconds % 60}s"
      elsif seconds < 86_400
        "#{seconds / 3600}h #{(seconds % 3600) / 60}m"
      else
        "#{seconds / 86_400}d #{(seconds % 86_400) / 3600}h"
      end
    end

    def pgbus_ms_duration(millis)
      return "—" unless millis

      millis = millis.to_i
      if millis < 1000
        "#{millis}ms"
      elsif millis < 60_000
        "#{(millis / 1000.0).round(1)}s"
      else
        "#{(millis / 60_000.0).round(1)}m"
      end
    end

    def pgbus_paused_badge(paused)
      return unless paused

      tag.span(I18n.t("pgbus.helpers.paused_badge"),
               class: "inline-flex items-center rounded-full bg-yellow-100 px-2 py-0.5 text-xs font-medium text-yellow-800")
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
        tag.span(I18n.t("pgbus.helpers.recurring_health.pending"),
                 class: "inline-flex items-center rounded-full bg-yellow-100 px-2.5 py-0.5 text-xs font-medium text-yellow-800")
      else
        tag.span(I18n.t("pgbus.helpers.recurring_health.active"),
                 class: "inline-flex items-center rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-800")
      end
    end

    def pgbus_time_range_label(minutes)
      minutes = [minutes.to_i, 1].max

      if minutes > 1440 && (minutes % 1440).zero?
        pgbus_pluralize_unit(minutes / 1440, "day")
      elsif minutes >= 60 && (minutes % 60).zero?
        pgbus_pluralize_unit(minutes / 60, "hour")
      else
        pgbus_pluralize_unit(minutes, "minute")
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

    LOCALE_NAMES = {
      da: "Dansk",
      de: "Deutsch",
      en: "English",
      es: "Espa\u00f1ol",
      fi: "Suomi",
      fr: "Fran\u00e7ais",
      it: "Italiano",
      ja: "\u65E5\u672C\u8A9E",
      nb: "Norsk",
      nl: "Nederlands",
      pt: "Portugu\u00eas",
      sv: "Svenska"
    }.freeze

    def pgbus_locale_name(code)
      LOCALE_NAMES[code.to_sym] || code.to_s.upcase
    end

    def pgbus_locale_flag(code)
      case code.to_sym
      when :da then "\u{1F1E9}\u{1F1F0}"
      when :de then "\u{1F1E9}\u{1F1EA}"
      when :en then "\u{1F1EC}\u{1F1E7}"
      when :es then "\u{1F1EA}\u{1F1F8}"
      when :fi then "\u{1F1EB}\u{1F1EE}"
      when :fr then "\u{1F1EB}\u{1F1F7}"
      when :it then "\u{1F1EE}\u{1F1F9}"
      when :ja then "\u{1F1EF}\u{1F1F5}"
      when :nb then "\u{1F1F3}\u{1F1F4}"
      when :nl then "\u{1F1F3}\u{1F1F1}"
      when :pt then "\u{1F1F5}\u{1F1F9}"
      when :sv then "\u{1F1F8}\u{1F1EA}"
      else "\u{1F310}"
      end
    end

    private

    def pgbus_pluralize_unit(count, unit)
      count == 1 ? "1 #{unit}" : "#{count} #{unit}s"
    end
  end
end
