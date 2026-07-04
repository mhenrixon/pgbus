# frozen_string_literal: true

module Components
  # Base class for the site's own Phlex components (the flow diagrams). Pulls in
  # the docs-kit Phlex kit so a component can render the shared chrome in the
  # short kit form (DocsUI::Section(...), DocsUI::Code(...)) instead of
  # `render DocsUI::Whatever.new(...)`.
  class Base < Phlex::HTML
    include Phlex::Rails::Helpers::Routes
    include DocsUI
  end
end
