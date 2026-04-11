# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ErrorReporter do
  let(:config) { Pgbus::Configuration.new }
  let(:error) { StandardError.new("test failure") }
  let(:context) { { queue: "default", job_class: "MyJob" } }

  before do
    config.logger = Logger.new(IO::NULL)
    allow(config.logger).to receive(:error)
  end

  describe ".report" do
    context "with no reporters configured" do
      it "logs the error and does not raise" do
        described_class.report(error, context, config: config)

        expect(config.logger).to have_received(:error)
      end
    end

    context "with a single reporter" do
      it "calls the reporter with exception and context" do
        reported = []
        config.error_reporters << ->(ex, ctx) { reported << [ex, ctx] }

        described_class.report(error, context, config: config)

        expect(reported.size).to eq(1)
        expect(reported.first[0]).to eq(error)
        expect(reported.first[1]).to include(queue: "default")
      end
    end

    context "with multiple reporters" do
      it "calls all reporters" do
        called = []
        config.error_reporters << ->(_ex, _ctx) { called << :first }
        config.error_reporters << ->(_ex, _ctx) { called << :second }

        described_class.report(error, context, config: config)

        expect(called).to eq(%i[first second])
      end
    end

    context "when a reporter raises" do
      it "continues to the next reporter and logs the handler error" do
        called = []
        config.error_reporters << ->(_ex, _ctx) { raise "handler boom" }
        config.error_reporters << ->(_ex, _ctx) { called << :second }

        described_class.report(error, context, config: config)

        expect(called).to eq([:second])
        expect(config.logger).to have_received(:error).at_least(:twice)
      end
    end

    context "when reporter accepts 3 arguments (with config)" do
      it "passes config as the third argument" do
        received_config = nil
        config.error_reporters << ->(_ex, _ctx, cfg) { received_config = cfg }

        described_class.report(error, context, config: config)

        expect(received_config).to eq(config)
      end
    end
  end
end
