# frozen_string_literal: true

require "zeitwerk"

module Pgbus
  # Suffix appended to a queue name to derive its dead-letter companion
  # (e.g. "pgbus_default" -> "pgbus_default_dlq"). Hard-coded here because
  # changing it on a running deployment would orphan every existing DLQ
  # message; nothing in the codebase or in user reports has ever needed
  # this to be configurable.
  DEAD_LETTER_SUFFIX = "_dlq"

  class Error < StandardError; end
  class ConfigurationError < Error; end
  class SerializationError < Error; end
  class QueueNotFoundError < Error; end
  class DeadLetterError < Error; end
  class ConcurrencyLimitExceeded < Error; end
  class JobNotUnique < Error; end
  class SchemaNotReady < Error; end
  class ReadTimeoutError < Error; end

  module Process
    # A LISTEN connection landed on a read-only replica (pg_is_in_recovery() =>
    # t). After a failover, stale DNS can point a fresh connection at the
    # demoted master; NOTIFY fires only on the primary, so such a connection
    # connects "successfully" but never wakes. Raised by
    # Process::PrimaryValidator so the listener rejects it and retries with
    # backoff (re-resolving DNS each attempt).
    #
    # Defined here rather than in process/primary_validator.rb because Zeitwerk
    # requires each managed file to define exactly the constant matching its
    # path — a second class in that file would break eager_load. lib/pgbus.rb
    # is the (require-loaded) gem entry point, so it can define the error and
    # explicitly namespace Process without conflicting with Zeitwerk's
    # management of the lib/pgbus/process/ directory.
    class ReplicaConnectionError < Error; end
  end

  class << self
    # Process-global flag set by Worker#graceful_shutdown so the adapter
    # can report stopping? to ActiveJob::Continuation (Rails 8.1+).
    attr_writer :stopping

    def stopping
      @stopping || false
    end

    def loader
      @loader ||= begin
        loader = Zeitwerk::Loader.for_gem
        loader.inflector.inflect(
          "pgbus" => "Pgbus",
          "cli" => "CLI",
          "dsl" => "DSL",
          "capsule_dsl" => "CapsuleDSL",
          "mcp" => "MCP"
        )
        loader.ignore("#{__dir__}/generators")
        loader.ignore("#{__dir__}/active_job")
        loader.ignore("#{__dir__}/pgbus/testing")
        # The MCP diagnostic server is optional — its tool classes subclass
        # MCP::Tool from the (optional) `mcp` gem. Keeping it out of Zeitwerk
        # means we never reference the gem's constants at autoload time;
        # `Pgbus::MCP.load!` (called by the `pgbus mcp` CLI command) requires
        # the gem and loads the subsystem explicitly.
        loader.ignore("#{__dir__}/pgbus/mcp")
        loader.ignore("#{__dir__}/pgbus/mcp.rb")
        # Vendor integrations are loaded conditionally (when the vendor gem
        # is present) by lib/pgbus/engine.rb. Keeping them out of Zeitwerk
        # means we don't reference vendor constants at autoload time.
        loader.ignore("#{__dir__}/pgbus/integrations")
        # lib/puma/plugin/pgbus_streams.rb is a Puma plugin — it's required
        # explicitly by the user from config/puma.rb via `plugin :pgbus_streams`.
        # Without this ignore, Zeitwerk scans lib/puma/ under the pgbus loader
        # root and tries to autoload Puma::Plugin, which collides with the real
        # Puma::Plugin class defined by the puma gem itself.
        loader.ignore("#{__dir__}/puma")
        loader
      end
    end

    # Separate loader for app/models used only in non-Rails contexts (specs,
    # standalone scripts). When the Engine boots, Rails' autoloader takes over
    # app/models and this loader is torn down to avoid conflicts.
    def models_loader
      models_dir = File.expand_path("../app/models", __dir__)
      return nil unless File.directory?(models_dir)

      @models_loader ||= begin
        loader = Zeitwerk::Loader.new
        loader.tag = "pgbus-models"
        loader.push_dir(models_dir)
        loader.setup
        loader
      end
    end

    def teardown_models_loader!
      return unless @models_loader

      @models_loader.unregister
      @models_loader = nil
    end

    def configuration
      @configuration ||= Configuration.new
    end

    def configure
      yield configuration
    end

    def client
      @client ||= Client.new(configuration)
    end

    # Entry point for the streams subsystem — `Pgbus.stream(name).broadcast(html)`
    # or `Pgbus.stream(@order).current_msg_id`. Defined on Pgbus itself rather
    # than inside lib/pgbus/streams.rb because that file is only Zeitwerk-loaded
    # when Pgbus::Streams::Stream is first referenced — the chicken-and-egg
    # problem means `Pgbus.stream(...)` would be undefined on the first call.
    # Referencing Streams::Stream inside the method body forces Zeitwerk to
    # load lib/pgbus/streams.rb lazily on first use, which is fine.
    #
    # Caches Stream instances by logical name so high-frequency callers
    # (e.g. Turbo::StreamsChannel.broadcast_stream_to inside an
    # after_update_commit callback firing 1000x/sec) don't allocate a new
    # Stream + Mutex per broadcast. The cache is process-local; reset!
    # clears it. The cache key is the resolved name string, not the raw
    # streamables, so `Pgbus.stream(@order)` and `Pgbus.stream(@order)`
    # in the same process return the same instance.
    # `durable:` defaults to `nil` so the configuration resolves the mode:
    # `streams_durable_patterns` first (exact string or regex match), then
    # `streams_default_broadcast_mode`. Pass `durable: true`/`false`
    # explicitly to bypass the resolver for this Stream instance.
    def stream(streamables, durable: nil)
      name = Streams::Stream.name_from(streamables)
      resolved = durable.nil? ? configuration.stream_durable?(name) : durable
      cache_key = "#{name}:#{resolved ? "d" : "e"}"
      @stream_cache ||= Concurrent::Map.new
      @stream_cache.compute_if_absent(cache_key) { Streams::Stream.new(streamables, durable: resolved) }
    end

    # Compose a short, pgbus-safe stream identifier from any mix of
    # records, strings, symbols, and arrays. Delegates to
    # `Pgbus::Streams::Key.stream_key`; raises `ArgumentError` if the
    # resulting key would overflow the pgbus queue-name budget. See
    # `lib/pgbus/streams/key.rb` for the digest policy and rationale.
    #
    #   Pgbus.stream_key(chat, :messages)
    #   # => "ai_chat_3a4f9c21b7d20e18:messages"
    #
    #   Pgbus.stream(Pgbus.stream_key(chat, :messages)).broadcast(html)
    def stream_key(*parts, **)
      Streams::Key.stream_key(*parts, **)
    end

    # Accepts an already-built stream key verbatim (no re-keying), only
    # enforcing the queue-name budget. Use when you hold a key string and
    # want to pass it to both `turbo_stream_from` and a broadcaster
    # without the colon-separator guard raising on the second call.
    #
    #   key = Pgbus.stream_key(chat, :messages)
    #   Pgbus.stream(Pgbus.stream_key!(key)).broadcast(html)
    def stream_key!(key)
      Streams::Key.stream_key!(key)
    end

    def reset!
      @client&.close
      @client = nil
      @configuration = nil
      @stream_cache = nil
    end

    # Discard the inherited PGMQ client after fork.
    # Do NOT call close — the parent's @pgmq_mutex is in undefined
    # state post-fork and attempting to acquire it can deadlock.
    # The next call to Pgbus.client will lazily create a fresh one.
    def reset_client!
      @client = nil
    end

    def logger
      configuration.logger
    end

    def logger=(value)
      configuration.logger = value
    end
  end

  loader.setup

  # In non-Rails contexts, set up model autoloading via a separate loader.
  # This is torn down by the Engine initializer when Rails boots.
  models_loader unless defined?(Rails::Engine)
end

require "active_job/queue_adapters/pgbus_adapter" if defined?(ActiveJob)
require "pgbus/engine" if defined?(Rails::Engine)

# Define the optional MCP namespace and its `load!` entrypoint. This file is
# excluded from Zeitwerk (it references the optional `mcp` gem only inside
# load!), so require it explicitly here. Requiring it does NOT load the gem.
require "pgbus/mcp"
