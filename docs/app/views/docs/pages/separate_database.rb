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
end
