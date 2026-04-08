# frozen_string_literal: true

require "active_support/message_verifier"

module Pgbus
  module Streams
    # Verifies tamper-proof stream identifiers carried in URLs (the
    # `signed-stream-name` attribute set by `pgbus_stream_from`). Reuses
    # `Turbo.signed_stream_verifier_key` when turbo-rails is loaded so that
    # existing `broadcasts_to :room` calls Just Work; falls back to
    # `Pgbus.configuration.streams_signed_name_secret` otherwise.
    #
    # The signed payload is the logical stream name as a string (e.g.
    # `"gid://app/Order/42:messages"`). Verification returns that string;
    # tampered or unsigned input raises `InvalidSignedName`.
    module SignedName
      class InvalidSignedName < StandardError
      end

      class MissingSecret < StandardError
      end

      def self.verify!(token)
        raise InvalidSignedName, "signed stream name is blank" if token.nil? || token.to_s.strip.empty?

        verifier.verified(token) || raise(InvalidSignedName, "signed stream name failed verification")
      rescue ActiveSupport::MessageVerifier::InvalidSignature => e
        raise InvalidSignedName, "signed stream name failed verification: #{e.message}"
      end

      def self.sign(stream_name)
        verifier.generate(stream_name)
      end

      # Returns an ActiveSupport::MessageVerifier configured with whichever
      # secret is appropriate for this process. Memoization is intentionally
      # NOT used because tests rotate keys and the cost of constructing a
      # verifier is negligible compared to the SHA256 work it does.
      def self.verifier
        ActiveSupport::MessageVerifier.new(secret, digest: "SHA256", serializer: JSON)
      end

      def self.secret
        turbo_secret || configuration_secret || raise_missing_secret!
      end

      def self.turbo_secret
        return nil unless defined?(::Turbo)

        ::Turbo.signed_stream_verifier_key
      rescue ArgumentError
        # Real turbo-rails raises ArgumentError when the key is unset.
        nil
      end

      def self.configuration_secret
        Pgbus.configuration.streams_signed_name_secret
      end

      def self.raise_missing_secret!
        raise MissingSecret,
              "no signing secret available — set Pgbus.configuration.streams_signed_name_secret " \
              "or Turbo.signed_stream_verifier_key"
      end

      private_class_method :verifier, :secret, :turbo_secret, :configuration_secret, :raise_missing_secret!
    end
  end
end
