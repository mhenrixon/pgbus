# frozen_string_literal: true

source "https://rubygems.org"

gemspec

gem "irb"
gem "rake", "~> 13.0"

group :development do
  gem "rubocop", "~> 1.21"
  gem "rubocop-rspec", "~> 3.0"
end

group :test do
  gem "rspec", "~> 3.0"
  gem "rspec-rails", "~> 7.0"
end

group :development, :test do
  gem "actioncable", ">= 7.1", "< 9.0"
  gem "activejob", ">= 7.1", "< 9.0"
  gem "activerecord", ">= 7.1", "< 9.0"
  gem "pg", "~> 1.5"
end
