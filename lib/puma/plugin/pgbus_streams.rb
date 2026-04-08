# frozen_string_literal: true

require "puma/plugin"

# Puma plugin that tears down the pgbus Streamer cleanly on worker
# shutdown (SIGTERM, SIGUSR2 phased restart, SIGINT). Without this
# hook, Puma closes hijacked SSE sockets abruptly during shutdown,
# which looks to the browser like a network error and causes an
# immediate reconnect attempt mid-deploy. With the hook, the Streamer
# writes a `pgbus:shutdown` sentinel event to each connection and
# closes them cleanly, and clients reconnect to the new worker via
# EventSource's built-in Last-Event-ID mechanism — picking up any
# messages that landed during the flip via the PGMQ archive replay path.
#
# Users opt in by adding `plugin :pgbus_streams` to their puma.rb:
#
#   # config/puma.rb
#   plugin :pgbus_streams
#
# Auto-registering at gem load time would be too magical and would
# break users who aren't running Puma (e.g. the gem also ships a CLI
# and a non-Rails use case). Explicit opt-in is safer.
Puma::Plugin.create do
  def start(launcher)
    launcher.events.register(:after_stopped) do
      teardown_streamer(launcher)
    end

    launcher.events.register(:before_restart) do
      teardown_streamer(launcher)
    end
  end

  def teardown_streamer(launcher)
    return unless defined?(Pgbus::Web::Streamer)

    # Go through the public API so `@current_mutex` guards both the
    # read and the clear. Bypassing it with instance_variable_get/set
    # would race with any thread that's currently inside
    # `Streamer.current` building a new Instance.
    Pgbus::Web::Streamer.reset!
  rescue StandardError => e
    log_error(launcher, e)
  end

  def log_error(launcher, error)
    message = "[Pgbus::Puma::Plugin] streamer teardown raised: #{error.class}: #{error.message}"
    if launcher.respond_to?(:log_writer)
      launcher.log_writer.log(message)
    elsif defined?(Pgbus) && Pgbus.respond_to?(:logger)
      Pgbus.logger.warn { message }
    end
  end
end
