# frozen_string_literal: true

module Pgbus
  module Process
    # Supervisor-owned LISTEN hub (issue #381, worker_notify_scope:
    # :supervisor). Owns ONE NotifyListener — one direct PG connection for the
    # whole host — on the union of every capsule's and consumer's queue
    # channels, and fans wakes out to worker/consumer forks over per-fork
    # pipes (see WakePipe for the fork side and the W/H/P byte protocol).
    #
    # Union: explicit capsule queues from config.workers, wildcard capsules
    # via the shared WildcardQueueResolver, consumer queues via
    # Registry#queue_names_for_topics — each gated by config.role_enabled?.
    # Refreshed every REFRESH_INTERVAL_SECONDS from the supervisor's monitor
    # tick so wildcard churn and late registry subscriptions converge; a
    # transient gap is bounded by the forks' NOTIFY poll ceiling (15s), so
    # under-listening degrades latency, never liveness.
    #
    # Routing: a NOTIFY for queue Q wakes every fork whose registered set
    # contains Q, plus every wildcard fork. Over-waking costs one empty read;
    # under-waking is the only failure mode that matters, so wildcard routing
    # is unconditional.
    #
    # Status: healthy? = listener running + connected + delivering. Broadcast
    # to all pipes on change and every STATUS_REBROADCAST_SECONDS (idempotent
    # 1-byte writes), so a lost byte or a freshly full pipe can't wedge a fork
    # in the wrong mode.
    #
    # Threading: register/deregister/tick/stop run on the supervisor's main
    # loop; the wake callback runs on the listener's thread. The fork table is
    # the only shared state — guarded by @table_mutex. Pipe writes are 1-byte
    # write_nonblock calls (atomic well under PIPE_BUF) and never block: a
    # full pipe already guarantees a pending readable byte, so the wake is
    # skipped, and status repair rides the periodic rebroadcast.
    class NotifyHub
      REFRESH_INTERVAL_SECONDS = 30
      STATUS_REBROADCAST_SECONDS = 5
      RETRY_BASE_SECONDS = 5
      RETRY_MAX_SECONDS = 300

      def initialize(config:, listener_factory: nil, clock: nil, logger: Pgbus.logger)
        @config = config
        @logger = logger
        @clock = clock || -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) }
        @listener_factory = listener_factory || default_listener_factory
        @table_mutex = Mutex.new
        @forks = {}
        @listener = nil
        @last_status = nil
        @last_broadcast_at = nil
        @last_refresh_at = nil
        @retry_at = 0.0
        @retry_backoff = RETRY_BASE_SECONDS
      end

      def start
        build_listener
        @last_refresh_at = now
        self
      end

      def stop
        stop_listener_quietly
        @table_mutex.synchronize do
          @forks.each_value { |entry| close_quietly(entry[:pipe]) }
          @forks.clear
        end
        self
      end

      # Register a fork's wake pipe. +queues+ are PHYSICAL queue names (already
      # prefixed); wildcard forks pass wildcard: true and are woken for every
      # channel. The fork's current status byte is sent immediately so a
      # freshly forked worker starts in the right polling mode.
      def register_fork(pid:, queues:, pipe:, wildcard: false)
        entry = { pipe: pipe, queues: Set.new(queues), wildcard: wildcard }
        @table_mutex.synchronize { @forks[pid] = entry }
        byte = status_byte
        write_byte(entry, byte)
        # The registration write is the broadcast baseline: without stamping
        # it, the next tick would treat @last_status as unknown and re-send
        # the same byte to every fork immediately.
        @last_status = byte
        @last_broadcast_at = now
        self
      end

      def deregister_fork(pid)
        entry = @table_mutex.synchronize { @forks.delete(pid) }
        close_quietly(entry[:pipe]) if entry
        self
      end

      # Called ONLY inside a just-forked child: release the child's copies of
      # the parent's hub resources — the LISTEN socket fd (without a libpq
      # Terminate, see NotifyListener#close_inherited_socket!) and the fork
      # table's pipe write ends. Never touches parent state (fork copied it).
      def close_inherited!
        @listener&.close_inherited_socket!
        @listener = nil
        @table_mutex.synchronize do
          @forks.each_value { |entry| close_quietly(entry[:pipe]) }
          @forks.clear
        end
        self
      end

      # One supervisor monitor-loop beat: self-heal the listener, refresh the
      # LISTEN union, keep fork status fresh.
      def tick
        ensure_listener
        refresh_union_if_due
        broadcast_status
        self
      end

      def healthy?
        listener = @listener
        !!(listener&.running? && listener.connected? && listener.delivering?)
      end

      private

      attr_reader :config

      def now
        @clock.call
      end

      def default_listener_factory
        lambda do |physical_queues:, on_wake:|
          NotifyListener.new(
            physical_queues: physical_queues,
            on_wake: on_wake,
            connection_options: config.worker_notify_connection_options,
            health_check_ms: (config.polling_interval * 1000).to_i.clamp(250, 5_000),
            logger: @logger
          )
        end
      end

      def build_listener
        @listener = @listener_factory.call(
          physical_queues: desired_physical_queues,
          on_wake: method(:route)
        )
        @listener.start
      rescue StandardError => e
        @listener = nil
        @logger.error { "[Pgbus::NotifyHub] listener failed to start: #{e.class}: #{e.message}" }
      end

      # Mirrors Worker#ensure_notify_listener: restart a dead listener thread
      # on an exponential backoff so a persistent outage doesn't thrash a
      # reconnect per supervisor tick.
      def ensure_listener
        return if @listener&.running?
        return if now < @retry_at

        stop_listener_quietly
        build_listener

        @retry_backoff = if @listener&.running?
                           RETRY_BASE_SECONDS
                         else
                           [@retry_backoff * 2, RETRY_MAX_SECONDS].min
                         end
        @retry_at = now + @retry_backoff
      end

      def stop_listener_quietly
        @listener&.stop
      rescue StandardError => e
        @logger.warn { "[Pgbus::NotifyHub] failed to stop listener: #{e.class}: #{e.message}" }
      ensure
        @listener = nil
      end

      def refresh_union_if_due
        return unless @listener
        return if @last_refresh_at && (now - @last_refresh_at) < REFRESH_INTERVAL_SECONDS

        @last_refresh_at = now
        desired = desired_physical_queues.to_set
        current = @listener.listening_to.to_set { |channel| channel_to_physical(channel) }
        (desired - current).each { |q| @listener.add_queue(q) }
        (current - desired).each { |q| @listener.remove_queue(q) }
      rescue StandardError => e
        @logger.warn { "[Pgbus::NotifyHub] union refresh failed: #{e.class}: #{e.message}" }
      end

      def broadcast_status(status = status_byte)
        rebroadcast_due = @last_broadcast_at.nil? || (now - @last_broadcast_at) >= STATUS_REBROADCAST_SECONDS
        return if status == @last_status && !rebroadcast_due

        entries = @table_mutex.synchronize { @forks.values.dup }
        entries.each { |entry| write_byte(entry, status) }
        @last_status = status
        @last_broadcast_at = now
      end

      def status_byte
        healthy? ? WakePipe::HEALTHY : WakePipe::DEGRADED
      end

      # Runs on the LISTENER thread (NotifyListener on_wake callback).
      def route(channel)
        physical = channel_to_physical(channel)
        entries = @table_mutex.synchronize { @forks.values.dup }
        entries.each do |entry|
          write_byte(entry, WakePipe::WAKE) if entry[:wildcard] || entry[:queues].include?(physical)
        end
      end

      # Never block, never raise: a full pipe already guarantees a pending
      # readable byte (wake semantics are level-triggered), and a dead pipe's
      # fork is on its way through reap → deregister_fork.
      def write_byte(entry, byte)
        entry[:pipe].write_nonblock(byte)
      rescue IO::WaitWritable, Errno::EPIPE, IOError, Errno::EBADF
        nil
      end

      def desired_physical_queues
        names = []
        names.concat(capsule_queue_names) if config.role_enabled?(:workers)
        names.concat(consumer_queue_names) if config.role_enabled?(:consumers)
        names.uniq.map { |q| config.queue_name(q) }
      end

      def capsule_queue_names
        names = []
        wildcard = false
        Array(config.workers).each do |worker_config|
          queues = worker_config[:queues] || worker_config["queues"] || [config.default_queue]
          Array(queues).each do |q|
            q.to_s == "*" ? wildcard = true : names << q.to_s
          end
        end
        names.concat(resolve_wildcard) if wildcard
        names
      end

      # A resolver failure (DB blip at boot) degrades to the explicit union;
      # wildcard queues rejoin on the next successful refresh, and the forks'
      # poll ceiling keeps them processing meanwhile.
      def resolve_wildcard
        WildcardQueueResolver.resolve(config: config)
      rescue StandardError => e
        @logger.warn { "[Pgbus::NotifyHub] wildcard resolution failed: #{e.class}: #{e.message}" }
        []
      end

      def consumer_queue_names
        registry = Pgbus::EventBus::Registry.instance
        Array(config.event_consumers).flat_map do |consumer_config|
          topics = consumer_config[:topics] || consumer_config["topics"] || []
          registry.queue_names_for_topics(Array(topics))
        end
      end

      def channel_to_physical(channel)
        NotifyListener.physical_for(channel)
      end

      def close_quietly(pipe)
        pipe.close if pipe && !pipe.closed?
      rescue IOError, Errno::EBADF
        nil
      end
    end
  end
end
