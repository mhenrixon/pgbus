# frozen_string_literal: true

# Seeing what pgbus is doing: error reporting to your APM, structured logs,
# ActiveSupport::Notifications events, the metrics adapter, and health endpoints.
class Views::Docs::Pages::Observability < DocsUI::Page
  title "Observability"
  eyebrow "Operations"

  def lead = "Route errors to your APM, emit metrics, structure your logs, and expose health probes."

  def content
    error_reporting
    instrumentation
    metrics
    appsignal
    logging
    health
  end

  private

  def error_reporting
    DocsUI::Section("Error reporting", description: "Route caught exceptions to your APM.") do
      md <<~'MD'
        By default pgbus logs caught exceptions and continues. Push callable
        reporters onto `config.error_reporters` to forward them to Sentry,
        Honeybadger, AppSignal, or anything else. Each reporter receives
        `(exception, context_hash)`:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.error_reporters << ->(ex, ctx) { Sentry.capture_exception(ex, extra: ctx) }
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "Reporters wire into every critical rescue path — job execution, worker "
        plain "fetch/process, dispatcher maintenance, fork failures, circuit-breaker trips, "
        plain "outbox publish. "
        code { "ErrorReporter.report" }
        plain " never raises: a broken reporter can't take down the thread that called it."
      end
    end
  end

  def instrumentation
    DocsUI::Section("Instrumentation events", description: "Everything is an ActiveSupport::Notifications event.") do
      md <<~'MD'
        pgbus emits `ActiveSupport::Notifications` events on every hot path.
        Subscribe directly for a bespoke sink (New Relic, OpenTelemetry, a custom
        aggregator):
      MD
      DocsUI::Code(<<~'RUBY')
        ActiveSupport::Notifications.subscribe(/^pgbus\./) do |name, start, finish, _id, payload|
          duration_ms = (finish - start) * 1_000
          YourApm.record(name, duration_ms, payload)
        end
      RUBY
      md <<~'MD'
        The events, with payload keys documented in `lib/pgbus/instrumentation.rb`:
      MD
      DocsUI::Table(
        [ "Event", "Fires on" ],
        [
          [ [ :code, "pgbus.executor.execute" ], "Every job execution (wraps the run)." ],
          [ [ :code, "pgbus.job_completed" ], "A job finished successfully." ],
          [ [ :code, "pgbus.job_failed" ], "A job raised." ],
          [ [ :code, "pgbus.job_dead_lettered" ], "A job exceeded max_retries." ],
          [ [ :code, "pgbus.event_processed" ], "An event handler ran." ],
          [ [ :code, "pgbus.event_failed" ], "An event handler raised." ],
          [ [ :code, "pgbus.client.send_message" ], "A message was enqueued." ],
          [ [ :code, "pgbus.client.send_batch" ], "A batch was enqueued." ],
          [ [ :code, "pgbus.client.read_batch" ], "A worker read a batch." ],
          [ [ :code, "pgbus.stream.broadcast" ], "A stream broadcast fired." ],
          [ [ :code, "pgbus.outbox.publish" ], "An outbox entry was published." ],
          [ [ :code, "pgbus.recurring.enqueue" ], "A recurring task was enqueued." ],
          [ [ :code, "pgbus.worker.recycle" ], "A worker recycled itself." ]
        ]
      )
    end
  end

  def metrics
    DocsUI::Section("Metrics adapter (Prometheus / StatsD)", description: "Off by default; consumes the same events.") do
      md <<~'MD'
        The built-in metrics adapter consumes those events and forwards them to a
        backend — no hand-written subscribers. It's off by default (zero overhead)
        and runs independently of AppSignal:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.metrics_backend = :prometheus # in-process registry, scraped
          # or :statsd (DogStatsD UDP), or a custom Pgbus::Metrics::Backend instance
        end
      RUBY
      md <<~'MD'
        For Prometheus, mount the exporter — a self-contained Rack app:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/routes.rb")
        mount Pgbus::Metrics::PrometheusExporter.new => "/metrics"
      RUBY
      md <<~'MD'
        Emitted metrics are all `pgbus_`-prefixed with low-cardinality tags:
        `pgbus_queue_job_count`, `pgbus_job_duration_ms`, `pgbus_event_count`,
        `pgbus_messages_sent` / `_read`, `pgbus_stream_broadcast_count`,
        `pgbus_outbox_published`, `pgbus_recurring_enqueued`,
        `pgbus_worker_recycled`.
      MD
    end
  end

  def appsignal
    DocsUI::Section("AppSignal", description: "Auto-installs when the gem is present.") do
      md <<~'MD'
        Load the `appsignal` gem and pgbus auto-installs a subscriber and a minutely
        probe — background-job transactions for every job and handler, `pgbus_`
        counters and distributions, and gauges for queue depth, oldest-message age,
        DLQ depth, dead tuples, and MVCC horizon. Three importable dashboards ship in
        `lib/pgbus/integrations/appsignal/dashboards/`. Opt out with
        `config.appsignal_enabled = false`.
      MD
    end
  end

  def logging
    DocsUI::Section("Structured logging") do
      md <<~'MD'
        Switch to JSON logs for aggregators; the formatter extracts the `[Pgbus]`
        component into its own field and keeps thread-local context under `ctx`:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure { |c| c.log_format = :json } # or :text (default)
      RUBY
    end
  end

  def health
    DocsUI::Section("Health endpoints", description: "Liveness and readiness for Kubernetes.") do
      md <<~'MD'
        The supervisor can serve HTTP liveness and readiness probes on a dedicated
        port, so an orchestrator can tell a running-but-wedged process from a healthy
        one:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.health_port = 9394       # nil disables the endpoints
          c.health_bind = "0.0.0.0"
        end
      RUBY
    end
  end
end
