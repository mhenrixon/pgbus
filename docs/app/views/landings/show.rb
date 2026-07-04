# frozen_string_literal: true

module Views
  module Landings
    # The home page. Renders inside DocsUI::Shell (the full document + drawer
    # shell): a hero with the one-line install, a feature grid, and a grouped
    # index of the authored docs. Kept to roughly one screen — the docs are the
    # product.
    class Show < Phlex::HTML
      include Phlex::Rails::Helpers::Routes

      FEATURES = [
        [ "database", "One database, no Redis",
         "Jobs and events live in the Postgres you already run, on top of PGMQ. " \
         "No Redis, no separate broker, no extra moving part to operate." ],
        [ "briefcase", "Drop-in ActiveJob adapter",
         "Point Rails at :pgbus and every existing job enqueues through PGMQ — " \
         "with dead-letter routing and retry backoff you didn't have to write." ],
        [ "radio", "Topic-routed event bus",
         "Publish once; AMQP-style patterns (orders.#, payments.*) fan out to " \
         "idempotent subscribers, deduplicated by event id." ],
        [ "recycle", "Workers that recycle",
         "max_jobs, max_memory_mb, and max_lifetime retire a worker before it " \
         "leaks — the memory-bloat problem other backends leave to you." ],
        [ "shield-alert", "Dead-letter queues",
         "After max_retries failed reads (PGMQ's read_ct), a message routes to a " \
         "_dlq queue instead of spinning forever." ],
        [ "activity", "Postgres-SSE streams",
         "Broadcast live updates over Server-Sent Events straight from Postgres — " \
         "a Turbo Streams transport with no Action Cable and no Redis." ]
      ].freeze

      def view_template
        DocsUI::Shell(
          title: nil,
          description: DocsKit.configuration.tagline
        ) do
          hero
          feature_grid
          docs_section
        end
      end

      private

      def hero
        section(class: "not-prose mb-14") do
          div(class: "flex items-center gap-3 mb-6") do
            brand_mark
            span(class: "text-sm font-semibold tracking-wide opacity-70") { "pgbus" }
          end
          h1(class: "text-4xl sm:text-5xl font-extrabold tracking-tight mb-4 leading-tight") do
            plain "Jobs & events on "
            span(class: "bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent") { "PostgreSQL" }
          end
          p(class: "text-lg sm:text-xl opacity-80 max-w-2xl mb-7") do
            plain "PostgreSQL-native job processing and event bus for Rails, built on "
            a(href: "https://github.com/pgmq/pgmq", class: "link", target: "_blank", rel: "noopener") { "PGMQ" }
            plain ". ActiveJob, topic-routed events, worker recycling, and SSE streams — "
            strong { "one database, no Redis." }
          end
          div(class: "not-prose mb-8 max-w-xl") do
            DocsUI::Code(<<~RUBY, filename: "Gemfile")
              gem "pgbus"
            RUBY
          end
          div(class: "flex flex-wrap gap-3") do
            a(href: doc_path("installation"), class: "btn btn-primary") { "Get started" }
            a(href: doc_path("architecture"), class: "btn btn-ghost") { "How it works" }
            a(href: "https://github.com/mhenrixon/pgbus",
              class: "btn btn-ghost", target: "_blank", rel: "noopener") { "GitHub ↗" }
          end
        end
      end

      def brand_mark
        div(class: "w-9 h-9 rounded-lg bg-base-200 grid place-items-center") do
          div(class: "w-5 h-5 rotate-45 rounded-sm bg-gradient-to-br from-primary to-accent")
        end
      end

      def feature_grid
        section(class: "not-prose mb-16") do
          div(class: "grid gap-4 sm:grid-cols-2") do
            FEATURES.each { feature_card(it) }
          end
        end
      end

      def feature_card((icon, title, body))
        div(class: "card bg-base-200/60 border border-base-300 p-5") do
          div(class: "flex items-start gap-3") do
            div(class: "text-primary mt-0.5") { DocsUI::Icon(icon) }
            div do
              h3(class: "font-semibold mb-1") { title }
              p(class: "text-sm opacity-70") { body }
            end
          end
        end
      end

      def docs_section
        section(class: "not-prose") do
          h2(class: "text-sm uppercase tracking-wide opacity-60 mb-4") { "Documentation" }
          Doc.grouped.each do |group, docs|
            written = docs.select(&:view_class)
            next if written.empty?

            h3(class: "text-xs font-semibold uppercase tracking-wider opacity-40 mt-5 mb-2") { group }
            div(class: "grid gap-2 sm:grid-cols-2") do
              written.each { doc_link(it) }
            end
          end
        end
      end

      def doc_link(doc)
        a(href: doc_path(doc.slug),
          class: "link link-hover text-sm py-1 flex items-center gap-2") do
          span(class: "text-primary/50") { "›" }
          plain doc.title
        end
      end
    end
  end
end
