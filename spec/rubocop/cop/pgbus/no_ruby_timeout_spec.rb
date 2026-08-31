# frozen_string_literal: true

require "spec_helper"
require "rubocop"
# Require RuboCop's test-support pieces individually instead of
# "rubocop/rspec/support": that file calls RSpec.configure and includes
# CopHelper into EVERY example group, whose #registry/#config methods shadow
# pgbus specs' own lets and break unrelated specs in a bare full-suite run.
require "rubocop/rspec/cop_helper"
require "rubocop/rspec/expect_offense"
require "rubocop/rspec/shared_contexts"
require_relative "../../../../rubocop/pgbus"

RSpec.describe RuboCop::Cop::Pgbus::NoRubyTimeout do
  include CopHelper
  include RuboCop::RSpec::ExpectOffense

  include_context "config"

  let(:config) { RuboCop::Config.new }

  it "flags Timeout.timeout" do
    expect_offense(<<~RUBY)
      Timeout.timeout(5) { do_work }
              ^^^^^^^ Pgbus/NoRubyTimeout: Please be careful, Timeout is dangerous.#{trailing_message}
    RUBY
  end

  it "flags the fully-qualified ::Timeout.timeout" do
    expect_offense(<<~RUBY)
      ::Timeout.timeout(3) { work }
                ^^^^^^^ Pgbus/NoRubyTimeout: Please be careful, Timeout is dangerous.#{trailing_message}
    RUBY
  end

  it "does not flag an unrelated .timeout call" do
    expect_no_offenses(<<~RUBY)
      config.timeout(5)
      http_client.timeout = 5
    RUBY
  end

  it "does not flag a method named timeout on another receiver" do
    expect_no_offenses(<<~RUBY)
      Deadline.timeout(5) { work }
    RUBY
  end

  it "offers no autocorrect (the safe replacement is context-dependent)" do
    expect(described_class.support_autocorrect?).to be(false)
  end

  # The offense message is long; assert on the leading, human-facing sentence
  # in the offense specs above and keep the rest here for completeness.
  def trailing_message
    " Timeout.timeout uses Thread#raise and can corrupt a pooled connection " \
      "mid-call. Bound the resource itself instead (statement_timeout / " \
      "tcp_user_timeout / socket timeout)."
  end
end
