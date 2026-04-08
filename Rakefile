# frozen_string_literal: true

require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec) do |t|
  t.pattern = "spec/pgbus/**/*_spec.rb"
end

require "rubocop/rake_task"

RuboCop::RakeTask.new

namespace :bench do
  desc "Run serialization benchmarks"
  task :serialization do
    ruby "benchmarks/serialization_bench.rb"
  end

  desc "Run client operation benchmarks"
  task :client do
    ruby "benchmarks/client_bench.rb"
  end

  desc "Run executor benchmarks"
  task :executor do
    ruby "benchmarks/executor_bench.rb"
  end

  desc "Run detailed memory profiling"
  task :memory do
    ruby "benchmarks/memory_profile.rb"
  end

  desc "Run integration benchmarks (requires PGBUS_DATABASE_URL)"
  task :integration do
    ruby "benchmarks/integration_bench.rb"
  end

  desc "Run streams benchmarks (requires PGBUS_DATABASE_URL; boots real Puma + SSE)"
  task :streams do
    ruby "benchmarks/streams_bench.rb"
  end

  desc "Run all benchmarks (unit-level, no DB required)"
  task all: %i[serialization client executor]
end

desc "Run all benchmarks"
task bench: "bench:all"

desc "Build gem and verify contents"
task :build do
  sh("gem build pgbus.gemspec --strict")
  gem_file = Dir["pgbus-*.gem"].first
  abort "Gem file not found after build" unless gem_file

  sh("gem unpack #{gem_file} --target /tmp/gem-verify")
  puts "\n=== Gem contents ==="
  sh("find /tmp/gem-verify -type f | sort")
  sh("rm -rf /tmp/gem-verify #{gem_file}")
end

desc "Release a new version (rake release[1.2.3] or rake release[pre] or rake release[1.2.3,force])"
task :release, %i[version force] do |_t, args|
  require_relative "lib/pgbus/version"

  def info(msg)    = puts "\e[34m→\e[0m #{msg}"
  def success(msg) = puts "\e[32m✓\e[0m #{msg}"
  def skip(msg)    = puts "\e[33m⊘\e[0m #{msg} \e[33m(skipped)\e[0m"
  def warn(msg)    = puts "\e[33m⚠\e[0m #{msg}"
  def error(msg)   = puts "\e[31m✗\e[0m #{msg}"
  def header(msg)  = puts "\n\e[1;36m#{msg}\e[0m\n#{"─" * msg.length}"

  new_version = args[:version]
  abort "\e[31mUsage: rake release[X.Y.Z] or rake release[X.Y.Z,force]\e[0m" unless new_version

  force = args[:force]&.to_s&.downcase == "force"

  dirty = `git status --porcelain`.strip
  abort "\e[31mAborting: working directory is not clean.\e[0m\n#{dirty}" unless dirty.empty?

  current = Pgbus::VERSION
  prerelease = new_version.match?(/alpha|beta|rc|pre/) || new_version == "pre"

  if new_version == "pre"
    new_version = current
    prerelease = true
  end

  tag = "v#{new_version}"
  version_file = "lib/pgbus/version.rb"

  title = "Release #{tag}"
  title += " (force)" if force
  header title
  info "Current version: #{current}"
  info "New version:     #{new_version}"
  info "Pre-release:     #{prerelease}"

  # Step 0: Force cleanup — delete existing release and tag
  if force
    header "Force cleanup"
    if system("gh release view #{tag} >/dev/null 2>&1")
      sh("gh release delete #{tag} --yes --cleanup-tag")
      success "Deleted release and remote tag #{tag}"
    else
      skip "No release #{tag} to delete"
    end

    if system("git rev-parse #{tag} >/dev/null 2>&1")
      sh("git tag -d #{tag}")
      success "Deleted local tag #{tag}"
    else
      skip "No local tag #{tag} to delete"
    end
  end

  # Step 1: Update version file
  header "Version"
  if new_version == current
    skip "Version already #{new_version}"
  else
    content = File.read(version_file)
    content.sub!(/VERSION = ".*"/, "VERSION = \"#{new_version}\"")
    File.write(version_file, content)
    success "Updated #{version_file}"
  end

  # Step 2: Verify gem builds cleanly
  header "Build verification"
  sh("gem build pgbus.gemspec --strict")
  sh("rm -f pgbus-*.gem")
  success "Gem builds cleanly"

  # Step 3: Commit version bump
  header "Git commit"
  version_changed = !`git diff #{version_file}`.strip.empty? || !`git diff --cached #{version_file}`.strip.empty?
  if version_changed
    sh("git add #{version_file}")
    sh("git commit -m 'chore: bump version to #{new_version}'")
    success "Committed version bump"
  else
    skip "No version change to commit"
  end

  # Step 4: Push to origin
  header "Git push"
  local_sha = `git rev-parse HEAD`.strip
  remote_sha = `git rev-parse origin/main 2>/dev/null`.strip
  if local_sha == remote_sha
    skip "origin/main already at #{local_sha[0..6]}"
  else
    sh("git push origin main")
    success "Pushed to origin/main"
  end

  # Step 5: Create release
  header "Release"
  tag_exists = system("git rev-parse #{tag} >/dev/null 2>&1")
  release_exists = system("gh release view #{tag} >/dev/null 2>&1")

  if release_exists
    skip "Release #{tag} already exists (use force to re-create)"
  elsif tag_exists
    info "Tag #{tag} exists, creating release from it"
    pre_flag = prerelease ? "--prerelease" : ""
    sh("gh release create #{tag} --generate-notes #{pre_flag}".strip)
    success "Release #{tag} created from existing tag"
  else
    pre_flag = prerelease ? "--prerelease" : ""
    sh("gh release create #{tag} --generate-notes --target main #{pre_flag}".strip)
    success "Release #{tag} created"
  end

  puts ""
  success "\e[1mRelease #{tag} complete!\e[0m CI will handle the rest:"
  puts "    • Run tests"
  puts "    • Build + verify gem"
  puts "    • Sign with Sigstore"
  puts "    • Publish to RubyGems"
  puts "    • Upload assets to the release"
end

namespace :dummy do
  desc "Start dummy app with stub data for dashboard QA (PORT=3003, no database required)"
  task :server do
    port = ENV.fetch("PORT", "3003")
    ENV["PGBUS_STUB_DATA"] = "1"
    ENV["RAILS_ENV"] = "development"

    puts "\n\e[32m→ Starting dummy app with stub data at http://localhost:#{port}/pgbus\e[0m"
    puts "  Dashboard:  http://localhost:#{port}/pgbus"
    puts "  Using stub data source (no database needed)"
    puts "  Press Ctrl+C to stop\n\n"
    sh("bundle exec puma spec/dummy/config.ru -p #{port}")
  end
end

load File.expand_path("lib/tasks/pgbus_streams.rake", __dir__)

task default: %i[spec rubocop pgbus:streams:lint_no_live]
