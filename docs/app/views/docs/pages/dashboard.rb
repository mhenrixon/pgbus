# frozen_string_literal: true

# Mounting and securing the dashboard, and what each panel shows.
class Views::Docs::Pages::Dashboard < DocsUI::Page
  title "Dashboard"
  eyebrow "Operations"

  def lead = "Mount the engine to see queues, jobs, failures, and dead letters — auto-refreshing over Turbo Frames."

  def content
    mount
    panels
    auth
  end

  private

  def mount
    DocsUI::Section("Mount it") do
      DocsUI::Code(<<~RUBY, filename: "config/routes.rb")
        mount Pgbus::Engine => "/pgbus"
      RUBY
      md <<~'MD'
        The dashboard is a mountable Rails engine. Every table auto-refreshes over
        Turbo Frames — no WebSocket, no Action Cable. Destructive actions use styled
        confirmation dialogs and flash as toast notifications.
      MD
    end
  end

  def panels
    DocsUI::Section("The panels") do
      DocsUI::Table(
        [ "Panel", "Shows" ],
        [
          [ "Overview", "Queue depths, enqueued count, active processes, failures, throughput rate." ],
          [ "Queues", "Per-queue metrics with purge / pause / resume / delete." ],
          [ "Jobs", "Enqueued and failed jobs, with retry / discard." ],
          [ "Dead letter", "DLQ messages with retry / discard and bulk actions." ],
          [ "Processes", "Active workers, dispatcher, consumers with heartbeat status." ],
          [ "Events", "Registered subscribers and processed events." ],
          [ "Outbox", "Transactional outbox entries pending publication." ],
          [ "Locks", "Active uniqueness locks — state, owner PID@host, age." ],
          [ "Insights", "Throughput chart, status distribution, slowest job classes." ]
        ]
      )
      DocsUI::Callout(:note) do
        plain "The Insights page reads "
        code { "pgbus_job_stats" }
        plain " — add it with "
        code { "rails generate pgbus:add_job_stats" }
        plain ". Stats are buffered and bulk-inserted; if the migration hasn't run, recording is silently skipped."
      end
    end
  end

  def auth
    DocsUI::Section("Protect it in production", description: "The dashboard has no auth of its own.") do
      md <<~'MD'
        Guard the mounted engine with a `web_auth` lambda:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.web_auth = ->(request) { request.env["warden"]&.user&.admin? }
        end
      RUBY
      md <<~'MD'
        Or inherit from your own authenticated controller — then its
        `before_action` filters and helpers apply automatically, with no
        monkey-patching:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure do |config|
          config.base_controller_class = "Admin::BaseController"
          config.return_to_app_url     = "/admin" # adds a "back to app" button
        end
      RUBY
      DocsUI::Callout(:warning) do
        plain "The dashboard exposes queue contents and destructive actions "
        plain "(purge, delete, discard). Never mount it unauthenticated in production."
      end
    end
  end
end
