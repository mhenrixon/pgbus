# frozen_string_literal: true

source "https://rubygems.org"

gemspec

gem "irb"
gem "rake", "~> 13.0"

group :development do
  gem "bundler-audit", "~> 0.9", require: false
  gem "i18n-tasks", "~> 1.0"
  gem "rubocop", "~> 1.21"
  gem "rubocop-rspec", "~> 3.0"
  gem "ruby-openai"
end

group :test do
  gem "rspec", "~> 3.0"
  gem "rspec-rails", "~> 7.0"

  # System tests
  gem "capybara", require: false
  gem "capybara-playwright-driver", require: false
  gem "puma"
  gem "sqlite3"

  # Performance & memory profiling
  gem "benchmark-ips", "~> 2.13"
  gem "memory_profiler", "~> 1.1"
end

group :development, :test do
  gem "actioncable", ">= 7.1", "< 9.0"
  gem "activejob", ">= 7.1", "< 9.0"
  gem "activerecord", ">= 7.1", "< 9.0"
  gem "globalid", ">= 1.0"
  gem "pg", "~> 1.5"
end
