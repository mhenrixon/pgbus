# frozen_string_literal: true

# A sample guide page. Zeitwerk resolves this compact class reference through
# the directory-implied namespaces (app/views/docs/pages/ → Views::Docs::Pages),
# so there's no need for the 4-level nested-module ceremony. Subclass
# DocsUI::Page, set the title (+ optional eyebrow/lead), and build the body from
# the DocsUI kit (Section/Code) and Markdown islands (md). The "On this page"
# TOC + scroll-spy are automatic (config default).
class Views::Docs::Pages::Installation < DocsUI::Page
  title "Installation"
  eyebrow "Guide"

  def lead = "Add the gem and render your first page."

  def content
    DocsUI::Section("Add the gem", description: "One line in your Gemfile.") do
      md <<~'MD'
        docs-kit ships the shared Phlex chrome — configure it once.
      MD
      DocsUI::Code(<<~RUBY, filename: "Gemfile")
        gem "docs-kit"
      RUBY
    end

    DocsUI::Section("Configure") do
      md <<~'MD'
        Set your brand, themes, and nav:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/docs_kit.rb")
        DocsKit.configure do |c|
          c.brand  = "Docs"
          c.themes = %w[dark light]
        end
      RUBY
    end
  end
end
