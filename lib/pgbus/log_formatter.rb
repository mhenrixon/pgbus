# frozen_string_literal: true

require "json"
require "logger"
require "time"

module Pgbus
  # Log formatters for Pgbus, inspired by Sidekiq::Logger::Formatters.
  #
  # Usage:
  #   Pgbus.configure do |c|
  #     c.logger.formatter = Pgbus::LogFormatter::JSON.new
  #   end
  #
  # Or via the convenience config option:
  #   Pgbus.configure do |c|
  #     c.log_format = :json
  #   end
  module LogFormatter
    module_function

    # Thread-local context for structured logging. Works like
    # Sidekiq::Context — any key/value pairs set via with_context
    # appear in the JSON output under the "ctx" key.
    def with_context(hash)
      orig = current_context.dup
      current_context.merge!(hash)
      yield
    ensure
      Thread.current[:pgbus_log_context] = orig
    end

    def current_context
      Thread.current[:pgbus_log_context] ||= {}
    end

    # Human-readable text formatter with Pgbus context.
    # Output: "INFO 2024-01-15T10:30:00.000Z pid=1234 tid=abc queue=default: message\n"
    class Text < ::Logger::Formatter
      def call(severity, time, _progname, message)
        "#{severity} #{time.utc.iso8601(3)} pid=#{::Process.pid} tid=#{tid}#{format_context}: #{message}\n"
      end

      private

      def tid
        Thread.current[:pgbus_tid] ||= (Thread.current.object_id ^ ::Process.pid).to_s(36)
      end

      def format_context
        ctx = LogFormatter.current_context
        return "" if ctx.empty?

        " #{ctx.map { |k, v| "#{k}=#{v}" }.join(" ")}"
      end
    end

    # JSON formatter for structured logging. Each log line is a single
    # JSON object followed by a newline. Extracts the [Pgbus::Component]
    # prefix from messages into a separate "component" field.
    #
    # Output fields:
    #   ts  — ISO 8601 timestamp with milliseconds
    #   pid — process ID
    #   tid — thread ID (short hex)
    #   lvl — severity (DEBUG/INFO/WARN/ERROR/FATAL)
    #   msg — the log message (with component prefix stripped)
    #   component — extracted from [Pgbus] or [Pgbus::Foo] prefix (optional)
    #   ctx — thread-local context hash (optional, only when non-empty)
    class JSON < ::Logger::Formatter
      COMPONENT_PREFIX = /\A\[([^\]]+)\]\s*/

      def call(severity, time, _progname, message)
        msg = message.to_s
        hash = {
          ts: time.utc.iso8601(3),
          pid: ::Process.pid,
          tid: tid,
          lvl: severity
        }

        if (match = msg.match(COMPONENT_PREFIX))
          hash[:component] = match[1]
          msg = msg.sub(COMPONENT_PREFIX, "")
        end

        hash[:msg] = msg

        ctx = LogFormatter.current_context
        hash[:ctx] = ctx unless ctx.empty?

        "#{::JSON.generate(hash)}\n"
      end

      private

      def tid
        Thread.current[:pgbus_tid] ||= (Thread.current.object_id ^ ::Process.pid).to_s(36)
      end
    end
  end
end
