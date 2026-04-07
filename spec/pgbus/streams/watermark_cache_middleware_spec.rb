# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::WatermarkCacheMiddleware do
  subject(:middleware) { described_class.new(inner_app) }

  let(:inner_app) do
    lambda do |_env|
      Thread.current[described_class::CACHE_KEY] = { "chat" => 42 }
      [200, {}, ["ok"]]
    end
  end

  before { Thread.current[described_class::CACHE_KEY] = nil }

  it "passes the call through to the wrapped app" do
    status, _headers, body = middleware.call({})
    expect(status).to eq(200)
    expect(body).to eq(["ok"])
  end

  it "clears the thread-local cache after the request returns" do
    middleware.call({})
    expect(Thread.current[described_class::CACHE_KEY]).to be_nil
  end

  it "clears the cache even if the wrapped app raises" do
    raising_app = lambda { |_env|
      Thread.current[described_class::CACHE_KEY] = { a: 1 }
      raise "boom"
    }
    mw = described_class.new(raising_app)

    expect { mw.call({}) }.to raise_error("boom")
    expect(Thread.current[described_class::CACHE_KEY]).to be_nil
  end
end
