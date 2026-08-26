# frozen_string_literal: true

require "spec_helper"

# The checked-in frozen lockfiles pin the pgbus path gem and are installed with
# `--frozen`/deployment in CI, so they MUST name the current gemspec version or
# the frozen install instant-fails (the Rails 7.1 leg with exit 16; docs-CI on
# any docs change). `rake release` regenerates them in the bump commit, but this
# spec catches the drift directly — and in the normal rspec run, not just when a
# frozen CI leg happens to fail. See #338 (the release-task sync) and its fix.
#
# rubocop:disable-next RSpec/DescribeClass -- a repo-hygiene guard, not a class under test
RSpec.describe "Frozen lockfile version sync" do
  %w[gemfiles/rails_7_1.gemfile.lock docs/Gemfile.lock].each do |relative|
    describe relative do
      let(:path) { File.expand_path(File.join("../..", relative), __dir__) }

      it "exists (it is a tracked, checked-in lockfile)" do
        expect(File).to exist(path)
      end

      it "pins the pgbus path gem at the current gemspec version" do
        # Matches the PATH-source stanza line: `    pgbus (X.Y.Z)`.
        pinned = File.read(path)[/^\s+pgbus \((\d+\.\d+\.\d+[^)]*)\)/, 1]

        expect(pinned).to eq(Pgbus::VERSION),
                          "#{relative} pins pgbus #{pinned.inspect} but the gemspec is " \
                          "#{Pgbus::VERSION.inspect}. `rake release` bumps this pin in place; to fix " \
                          "by hand, replace the `pgbus (...)` pin(s) in #{relative} with " \
                          "`pgbus (#{Pgbus::VERSION})` (do NOT run `bundle lock` — the docs lock's " \
                          "PLATFORMS list makes a full re-resolve fail on platform-only gems)."
      end
    end
  end
end
