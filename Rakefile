# frozen_string_literal: true

require "bundler/gem_tasks"
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

  desc "Run all benchmarks"
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

desc "Release a new version (rake release[1.2.3] or rake release[pre])"
task :release, [:version] do |_t, args|
  require "pgbus/version"

  new_version = args[:version]
  abort "Usage: rake release[X.Y.Z] or rake release[pre]" unless new_version

  dirty = `git status --porcelain`.strip
  abort "Aborting: working directory is not clean.\n#{dirty}" unless dirty.empty?

  current = Pgbus::VERSION
  prerelease = new_version.match?(/alpha|beta|rc|pre/) || new_version == "pre"

  if new_version == "pre"
    new_version = current
    prerelease = true
  end

  tag = "v#{new_version}"

  puts "Current version: #{current}"
  puts "New version:     #{new_version}"
  puts "Tag:             #{tag}"
  puts "Pre-release:     #{prerelease}"
  puts ""

  # Update version file if needed
  version_file = "lib/pgbus/version.rb"
  if new_version != current
    content = File.read(version_file)
    content.sub!(/VERSION = ".*"/, "VERSION = \"#{new_version}\"")
    File.write(version_file, content)
    puts "Updated #{version_file}"
  end

  # Verify gem builds cleanly
  sh("gem build pgbus.gemspec --strict")
  sh("rm -f pgbus-*.gem")

  # Commit, push, and create release
  if new_version != current
    sh("git add #{version_file}")
    sh("git commit -m 'chore: bump version to #{new_version}'")
  end
  sh("git push origin main")

  pre_flag = prerelease ? "--prerelease" : ""
  sh("gh release create #{tag} --generate-notes --target main #{pre_flag}".strip)

  puts ""
  puts "Release #{tag} created! CI will handle the rest:"
  puts "  - Run tests"
  puts "  - Build + verify gem"
  puts "  - Sign with Sigstore"
  puts "  - Publish to RubyGems"
  puts "  - Upload assets to the release"
end

task default: %i[spec rubocop]
