# frozen_string_literal: true

require "net/http"
require "uri"

module SseTestSupport
  # Minimal Server-Sent Events client for the streams integration tests.
  # Keep-it-simple: one thread, Net::HTTP streaming body, hand-rolled
  # SSE parser. Exists because `EventSource` is a browser API and no
  # battle-tested Ruby SSE client exposes Last-Event-ID on reconnect
  # the way we need for testing the rails/rails#52420 fix.
  #
  # Usage:
  #
  #   client = SseTestClient.connect(url: "http://localhost:3000/pgbus/streams/...")
  #   events = client.wait_for_events(count: 2, timeout: 5)
  #   events.first.id      # => "1248"
  #   events.first.event   # => "turbo-stream"
  #   events.first.data    # => "<turbo-stream ...>"
  #   client.close
  #
  # Reconnect is manual — call `client.close` and build a new one with
  # `headers: { "Last-Event-ID" => "..." }`. The client does not do
  # automatic reconnect because the tests need deterministic control
  # over when reconnects happen.
  class SseTestClient
    Event = Struct.new(:id, :event, :data)

    attr_reader :events

    def self.connect(url:, headers: {}, timeout: 5)
      client = new(url: url, headers: headers, timeout: timeout)
      client.start
      client
    end

    def initialize(url:, headers: {}, timeout: 5)
      @uri     = URI.parse(url)
      @headers = headers
      @timeout = timeout
      @events  = []
      @mutex   = Mutex.new
      @thread  = nil
      @http    = nil
      @closed  = false
      @open_at = nil
    end

    def start
      @thread = Thread.new { run }
      wait_until_open(@timeout)
      self
    end

    def close
      @closed = true
      begin
        @http&.finish
      rescue StandardError
        # Best effort — the connection may already be closed by the peer.
      end
      @thread&.join(2)
      @thread = nil
    end

    # Blocks until `count` events have been received or `timeout` elapses.
    # Returns a snapshot slice — leaves accumulated events in @events so
    # callers can read more without losing earlier ones.
    def wait_for_events(count:, timeout: 5)
      deadline = monotonic + timeout
      loop do
        snapshot = @mutex.synchronize { @events.dup }
        return snapshot if snapshot.size >= count
        break if monotonic > deadline
        break if @closed

        sleep 0.01
      end
      @mutex.synchronize { @events.dup }
    end

    # Waits up to `timeout` seconds for no new events to arrive — useful
    # for asserting that a reconnect with Last-Event-ID delivers nothing
    # because the cursor is already caught up.
    def wait_for_quiet(seconds: 0.3)
      baseline = @mutex.synchronize { @events.size }
      sleep seconds
      current = @mutex.synchronize { @events.size }
      current == baseline
    end

    private

    def run
      @http = Net::HTTP.new(@uri.host, @uri.port)
      @http.open_timeout = @timeout
      @http.read_timeout = 30 # long enough to outlast heartbeats in tests

      request = Net::HTTP::Get.new(@uri.request_uri)
      request["Accept"] = "text/event-stream"
      request["Cache-Control"] = "no-cache"
      @headers.each { |k, v| request[k] = v }

      @http.start do |http|
        http.request(request) do |response|
          @open_at = monotonic
          parse_stream(response)
        end
      end
    rescue IOError, Errno::EPIPE, Errno::ECONNRESET, Net::ReadTimeout
      # Expected during clean shutdown or server-side close — swallow.
      # EOFError is intentionally NOT listed because it's a subclass of
      # IOError and rubocop's ShadowedException cop flags the duplicate.
    rescue StandardError => e
      warn "[SseTestClient] #{e.class}: #{e.message}" unless @closed
    ensure
      @closed = true
    end

    def parse_stream(response)
      buffer = +""
      response.read_body do |chunk|
        break if @closed

        buffer << chunk
        # SSE events are terminated by a blank line. Split on \n\n and
        # keep the trailing partial block in the buffer.
        while (index = buffer.index("\n\n"))
          block = buffer.slice!(0, index + 2)
          process_block(block.chomp("\n\n"))
        end
      end
    end

    def process_block(block)
      return if block.empty?
      return if block.lines.all? { |line| line.start_with?(":") }

      event = Event.new(id: nil, event: "message", data: "")
      recognized = false
      block.each_line do |raw|
        line = raw.chomp
        next if line.empty? || line.start_with?(":")

        next unless (match = line.match(/\A(\w+): ?(.*)\z/))

        field = match[1]
        value = match[2]
        case field
        when "id"
          event.id = value
          recognized = true
        when "event"
          event.event = value
          recognized = true
        when "data"
          event.data = event.data.empty? ? value : "#{event.data}\n#{value}"
          recognized = true
          # retry: directives and other SSE meta-fields are ignored; not an
          # event in the sense callers care about
        end
      end

      return unless recognized

      @mutex.synchronize { @events << event }
    end

    def wait_until_open(timeout)
      deadline = monotonic + timeout
      sleep 0.01 until @open_at || @closed || monotonic > deadline
      raise "SseTestClient failed to connect within #{timeout}s" unless @open_at
    end

    def monotonic
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end
end
