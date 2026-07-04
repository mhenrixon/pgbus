# frozen_string_literal: true

# Getting pgbus into a Rails app: the gem, the PGMQ schema, and the migrations
# the install generator writes.
class Views::Docs::Pages::Installation < DocsUI::Page
  title "Installation"
  eyebrow "Getting started"

  def lead = "Add the gem, install the PGMQ schema, and run the generator."

  def content
    requirements
    add_gem
    schema_modes
    run_generator
    separate_db
  end

  private

  def requirements
    DocsUI::Section("Requirements") do
      DocsUI::Table(
        [ "Requirement", "Version" ],
        [
          [ [ :code, "Ruby" ], ">= 3.3" ],
          [ [ :code, "Rails" ], ">= 7.1" ],
          [ [ :code, "PostgreSQL" ], [ :md, "with [PGMQ](https://github.com/pgmq/pgmq) — extension or embedded SQL" ] ]
        ]
      )
    end
  end

  def add_gem
    DocsUI::Section("Add the gem") do
      md <<~'MD'
        Add pgbus to your Gemfile and `bundle install`:
      MD
      DocsUI::Code(<<~RUBY, filename: "Gemfile")
        gem "pgbus"
      RUBY
    end
  end

  def schema_modes
    DocsUI::Section("Install the PGMQ schema", description: "Extension, embedded, or auto.") do
      md <<~'MD'
        PGMQ provides the queue tables and functions pgbus builds on. It can be
        installed as a PostgreSQL **extension** or as **vendored, embedded SQL**
        that needs no extension privileges — useful on managed Postgres where you
        can't `CREATE EXTENSION`. The mode is `config.pgmq_schema_mode`:
      MD

      DocsUI::PropTable([
        [ [ :code, ":auto" ], "Symbol", [ :code, ":auto" ], "Try the extension, fall back to embedded SQL. The default." ],
        [ [ :code, ":extension" ], "Symbol", "—", "Require the pgmq PostgreSQL extension." ],
        [ [ :code, ":embedded" ], "Symbol", "—", "Use the vendored SQL; no extension needed." ]
      ])

      md <<~'MD'
        With the extension route, create it once in your database:
      MD
      DocsUI::Code(<<~SQL, lexer: :sql)
        CREATE EXTENSION IF NOT EXISTS pgmq;
      SQL

      DocsUI::Callout(:note) do
        plain "Two rake tasks report and list versions: "
        code { "rake pgbus:pgmq:status" }
        plain " (installed vs available) and "
        code { "rake pgbus:pgmq:versions" }
        plain " (vendored PGMQ versions). Upgrade the embedded schema with "
        code { "rails generate pgbus:upgrade_pgmq" }
        plain "."
      end
    end
  end

  def run_generator
    DocsUI::Section("Run the install generator") do
      md <<~'MD'
        The install generator writes the migrations for pgbus's metadata tables
        (queue state, processed events, job locks, and so on) and a starter
        initializer:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:install
        rails db:migrate
      SHELL

      md <<~'MD'
        Choose the PGMQ schema mode at install time if you don't want the default:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:install --pgmq-schema-mode=embedded
      SHELL

      DocsUI::Callout(:tip) do
        plain "Already running an older pgbus? "
        code { "rails generate pgbus:update" }
        plain " converts a legacy config/pgbus.yml to a Ruby initializer and adds "
        plain "any missing migrations in one pass. See "
        a(href: "/docs/configuration", class: "link") { "Configuration" }
        plain "."
      end
    end
  end

  def separate_db
    DocsUI::Section("A dedicated database") do
      md <<~'MD'
        pgbus can live in your primary database (the default) or in a dedicated
        one, like SolidQueue. Pass `--database=pgbus` to route the migrations to
        `db/pgbus_migrate/`:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:install --database=pgbus
      SHELL
      md <<~'MD'
        See [Separate database](/docs/separate-database) for the `connects_to`
        config and the `database.yml` wiring.
      MD
    end
  end
end
