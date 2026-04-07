# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Configuration::CapsuleDSL do
  describe ".parse" do
    subject(:parse) { described_class.parse(input) }

    context "with a single capsule" do
      context "with the wildcard alone" do
        let(:input) { "*" }

        it "returns one capsule with all queues and the default thread count" do
          expect(parse).to eq([{ queues: ["*"], threads: 5 }])
        end
      end

      context "with the wildcard and an explicit thread count" do
        let(:input) { "*: 5" }

        it "returns one capsule with the explicit thread count" do
          expect(parse).to eq([{ queues: ["*"], threads: 5 }])
        end
      end

      context "with a single named queue" do
        let(:input) { "default" }

        it "returns one capsule with the queue and default threads" do
          expect(parse).to eq([{ queues: ["default"], threads: 5 }])
        end
      end

      context "with a single named queue and explicit thread count" do
        let(:input) { "default: 3" }

        it "returns one capsule with the explicit thread count" do
          expect(parse).to eq([{ queues: ["default"], threads: 3 }])
        end
      end

      context "with multiple queues in priority order" do
        let(:input) { "critical, default" }

        it "returns one capsule with the queues in declaration order" do
          expect(parse).to eq([{ queues: %w[critical default], threads: 5 }])
        end
      end

      context "with multiple queues and an explicit thread count" do
        let(:input) { "critical, default: 10" }

        it "returns one capsule with all queues and the explicit count" do
          expect(parse).to eq([{ queues: %w[critical default], threads: 10 }])
        end
      end

      context "with a prefix wildcard" do
        let(:input) { "staging_*: 3" }

        it "returns the prefix wildcard as a single queue token" do
          expect(parse).to eq([{ queues: ["staging_*"], threads: 3 }])
        end
      end
    end

    context "with multiple capsules" do
      context "with two capsules separated by semicolon" do
        let(:input) { "critical: 5; default: 10" }

        it "returns two independent capsules" do
          expect(parse).to eq([
                                { queues: ["critical"], threads: 5 },
                                { queues: ["default"], threads: 10 }
                              ])
        end
      end

      context "with multi-queue capsules and a fallback wildcard" do
        let(:input) { "critical: 3; default, mailers: 10; *: 2" }

        it "returns three capsules with the queue lists in order" do
          expect(parse).to eq([
                                { queues: ["critical"], threads: 3 },
                                { queues: %w[default mailers], threads: 10 },
                                { queues: ["*"], threads: 2 }
                              ])
        end
      end
    end

    context "with whitespace variations" do
      it "ignores leading and trailing whitespace" do
        expect(described_class.parse("  *: 5  ")).to eq([{ queues: ["*"], threads: 5 }])
      end

      it "ignores whitespace around commas, colons, and semicolons" do
        expect(described_class.parse("critical , default :  5 ;  mailers : 2"))
          .to eq([
                   { queues: %w[critical default], threads: 5 },
                   { queues: ["mailers"], threads: 2 }
                 ])
      end

      it "tolerates a trailing semicolon" do
        expect(described_class.parse("*: 5;")).to eq([{ queues: ["*"], threads: 5 }])
      end
    end

    context "with no explicit thread count" do
      it "is 5 when omitted" do
        expect(described_class.parse("default")).to eq([{ queues: ["default"], threads: 5 }])
      end

      it "applies per capsule when multiple capsules omit threads" do
        expect(described_class.parse("a; b")).to eq([
                                                      { queues: ["a"], threads: 5 },
                                                      { queues: ["b"], threads: 5 }
                                                    ])
      end
    end

    context "with invalid input" do
      def expect_parse_error(input, message_regex)
        expect { described_class.parse(input) }.to raise_error(
          Pgbus::Configuration::CapsuleDSL::ParseError,
          message_regex
        )
      end

      it "rejects nil" do
        expect_parse_error(nil, /expected String/i)
      end

      it "rejects an empty string" do
        expect_parse_error("", /empty/i)
      end

      it "rejects a whitespace-only string" do
        expect_parse_error("   ", /empty/i)
      end

      it "rejects a leading semicolon (empty capsule)" do
        expect_parse_error("; default: 5", /empty capsule/i)
      end

      it "rejects a colon with no queue before it" do
        expect_parse_error(":5", /queue name.*before/i)
      end

      it "rejects a colon with no thread count after it" do
        expect_parse_error("default:", /thread count/i)
      end

      it "rejects a non-numeric thread count" do
        expect_parse_error("default: abc", /thread count.*abc/i)
      end

      it "rejects a fractional thread count" do
        expect_parse_error("default: 5.5", /thread count.*5\.5/i)
      end

      it "rejects zero threads" do
        expect_parse_error("default: 0", /positive integer.*0/i)
      end

      it "rejects negative threads" do
        expect_parse_error("default: -1", /thread count.*-1/i)
      end

      it "rejects a queue name with spaces" do
        expect_parse_error("queue with spaces: 5", /invalid character|expected/i)
      end

      it "rejects two queues separated by space instead of comma" do
        expect_parse_error("default mailers: 5", /invalid character|expected/i)
      end

      it "rejects the same queue listed in two capsules" do
        expect_parse_error("default: 5; default: 3", /default.*two capsules|appears in two/i)
      end

      it "rejects the same queue listed twice within one capsule" do
        expect_parse_error("default, default: 5", /default.*twice|duplicate/i)
      end

      it "rejects the wildcard in two capsules" do
        expect_parse_error("*: 5; *: 3", /wildcard.*two capsules|\*.*appears in two/i)
      end

      it "includes the offending input in the error message" do
        expect { described_class.parse("default: bogus") }.to raise_error(
          Pgbus::Configuration::CapsuleDSL::ParseError,
          /default: bogus/
        )
      end
    end

    context "with real-world configurations" do
      it "parses the simplest single-wildcard shape" do
        expect(described_class.parse("*: 5")).to eq([{ queues: ["*"], threads: 5 }])
      end

      it "parses a higher-thread production wildcard" do
        expect(described_class.parse("*: 15")).to eq([{ queues: ["*"], threads: 15 }])
      end

      it "parses a fast/slow capsule split for reserving capacity" do
        result = described_class.parse("default, statistics, low: 2; *: 3")
        expect(result).to eq([
                               { queues: %w[default statistics low], threads: 2 },
                               { queues: ["*"], threads: 3 }
                             ])
      end
    end
  end
end
