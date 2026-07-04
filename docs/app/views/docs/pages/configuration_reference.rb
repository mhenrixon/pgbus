# frozen_string_literal: true

# Every configuration option, grouped, rendered from the ConfigReference data
# constant. A drift spec keeps this list in lockstep with the real
# Pgbus::Configuration accessors.
class Views::Docs::Pages::ConfigurationReference < DocsUI::Page
  title "Configuration reference"
  eyebrow "Reference"

  def lead = "Every Pgbus.configure option, grouped by concern."

  def content
    intro
    ConfigReference::GROUPS.each { |group, options| section_for(group, options) }
  end

  private

  def intro
    DocsUI::Section("Reading this page") do
      md <<~'MD'
        Set any of these in a `Pgbus.configure` block. Durations accept an integer
        number of seconds or an `ActiveSupport::Duration`. Every option has a
        default, so you only set what you need. For the knobs most apps touch
        first, start with [Configuration](/docs/configuration).
      MD
    end
  end

  def section_for(group, options)
    DocsUI::Section(group) do
      DocsUI::PropTable(
        options.map { |o| [ [ :code, o[:name] ], o[:type], [ :code, o[:default] ], o[:desc] ] }
      )
    end
  end
end
