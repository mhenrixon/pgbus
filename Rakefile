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

task default: %i[spec rubocop]
