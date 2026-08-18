# frozen_string_literal: true

# Running pgbus in a dedicated database instead of the primary — the SolidQueue
# pattern. Covers connects_to, the migration path, and database.yml.
class Views::Docs::Pages::SeparateDatabase < DocsUI::Page
  title "Separate database"
  eyebrow "Operations"

  def lead = "Run pgbus in a dedicated database, isolating queue churn from your primary."

  def content
    why
    configure
    migrations
    database_yml
    purge_guard
  end

  private

  def why
    DocsUI::Section("Primary or dedicated") do
      md <<~'MD'
        pgbus runs in your primary database by default. For high-volume
        deployments you can give it a dedicated database — the same pattern
        SolidQueue uses — so the queue tables' heavy write/vacuum churn doesn't
        compete with your application's working set.
      MD
    end
  end

  def configure
    DocsUI::Section("Point pgbus at the database", description: "config.connects_to.") do
      md <<~'MD'
        `config.connects_to` follows Rails' multiple-databases API. Leave it `nil`
        for the primary; set the `writing` role for a dedicated database:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.connects_to = { database: { writing: :pgbus } }
        end
      RUBY
    end
  end

  def migrations
    DocsUI::Section("Route migrations to db/pgbus_migrate") do
      md <<~'MD'
        Pass `--database=pgbus` to any pgbus generator and its migrations go to
        `db/pgbus_migrate/` instead of `db/migrate/`:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:install --database=pgbus
        rails generate pgbus:add_recurring --database=pgbus
        rails db:migrate:pgbus
      SHELL
      DocsUI::Callout(:tip) do
        plain "The "
        code { "pgbus:update" }
        plain " generator detects a separate-database config automatically (from "
        code { "connects_to" }
        plain " or by scanning your initializer), so you don't re-specify "
        code { "--database=pgbus" }
        plain " on every run."
      end
    end
  end

  def database_yml
    DocsUI::Section("Wire up database.yml") do
      md <<~'MD'
        Declare the second database with its migrations path so `db:migrate:pgbus`
        knows where to look:
      MD
      DocsUI::Code(<<~YAML, filename: "config/database.yml", lexer: :yaml)
        production:
          primary:
            <<: *default
            database: myapp_production
          pgbus:
            <<: *default
            database: myapp_pgbus_production
            migrations_paths: db/pgbus_migrate
      YAML
    end
  end

  def purge_guard
    DocsUI::Section("Boot-time connections and db:test:purge") do
      md <<~'MD'
        Any code that touches a pgbus model during Rails boot leaves an idle
        session on the dedicated database for the life of the process. Rails'
        `db:test:purge` / `db:drop` only disconnect the connection they open
        themselves, so that idle session would block the process's own
        `DROP DATABASE` (or kill it via `statement_timeout`). pgbus guards this
        automatically: every purge/drop first disconnects pgbus's own
        connection pools, so `db:test:prepare` can never wedge on a pgbus
        session.

        For your own boot-time database touches (warm-ups, probes in an
        initializer), skip them in the rake contexts where a database may not
        exist yet with `Pgbus.database_task?`:
      MD
      DocsUI::Code(<<~'RUBY', filename: "config/initializers/pgbus_warmup.rb")
        Rails.application.config.after_initialize do
          next if Pgbus.database_task? # db:*, assets:* rake tasks

          # ... touch the pgbus database ...
        end
      RUBY
    end
  end
end
