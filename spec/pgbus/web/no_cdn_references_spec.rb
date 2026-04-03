# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Dashboard views have no external CDN references" do # rubocop:disable RSpec/DescribeClass
  let(:views_dir) { Pgbus::Engine.root.join("app", "views") }
  let(:cdn_patterns) do
    [
      /cdn\.tailwindcss\.com/,
      %r{esm\.sh/},
      /cdn\.jsdelivr\.net/,
      /unpkg\.com/,
      /cdnjs\.cloudflare\.com/
    ]
  end

  it "does not reference any external CDN in ERB templates" do
    violations = []

    Dir[views_dir.join("**", "*.erb")].each do |file|
      content = File.read(file)
      cdn_patterns.each do |pattern|
        if content.match?(pattern)
          relative = Pathname.new(file).relative_path_from(views_dir)
          violations << "#{relative} matches #{pattern.source}"
        end
      end
    end

    expect(violations).to be_empty,
                          "Found external CDN references in views:\n  #{violations.join("\n  ")}"
  end
end
