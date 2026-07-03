# frozen_string_literal: true

module Pgbus
  module Process
    class Supervisor
      include SignalHandler

      FORK_WAIT = 1 # seconds between fork checks
      WATCHDOG_INTERVAL = 10 # seconds between stall checks

      # A child that crashes within this many seconds of forking is treated
      # as crash-looping and restarted with exponential backoff (base
      # RESTART_BACKOFF_BASE, doubling per consecutive crash, capped at
      # RESTART_BACKOFF_MAX). A child that ran at least this long — or that
      # exited cleanly, like a recycling worker — restarts immediately and
      # resets its crash streak.
      RESTART_STABLE_UPTIME = 30
      RESTART_BACKOFF_BASE = 1
      RESTART_BACKOFF_MAX = 60

      attr_reader :config

      def initialize(config: Pgbus.configuration)
        @config = config
        @forks = {}
        @shutting_down = false
        @last_watchdog_at = monotonic_now
        @pending_restarts = []
        @crash_counts = Hash.new(0)
      end

      def run
        setup_signals
        start_heartbeat

        Pgbus.logger.info { "[Pgbus] Supervisor starting pid=#{::Process.pid}" }

        # Fail fast on a bad database_url / connection_params. PGMQ's pool is
        # lazy, so an unreachable DB would otherwise only surface once forked
        # children crash-loop against it. verify_connection! raises
        # Pgbus::ConfigurationError with an actionable message; we let it
        # propagate so the supervisor exits instead of forking anything.
        Pgbus.client.verify_connection!

        # Bootstrap queues once in the parent process before forking children.
        # This avoids the deadlock that occurs when multiple forked children
        # race to call enable_notify_insert (DROP TRIGGER + CREATE TRIGGER)
        # concurrently on the same queue tables. Children still call
        # bootstrap_queues post-fork but the idempotent check in
        # notify_trigger_current? makes those calls cheap no-ops.
        bootstrap_queues

        boot_processes
        monitor_loop
      ensure
        shutdown
      end

      def graceful_shutdown
        Pgbus.logger.info { "[Pgbus] Supervisor: graceful shutdown requested" }
        @shutting_down = true
        signal_children("TERM")
      end

      def immediate_shutdown
        Pgbus.logger.warn { "[Pgbus] Supervisor: immediate shutdown requested" }
        @shutting_down = true
        signal_children("QUIT")
      end

      private

      def boot_processes
        # Boot workers (workers may be nil for scheduler-only or
        # dispatcher-only deployments via --workers-only / --scheduler-only /
        # --dispatcher-only CLI flags). Each role is gated by
        # config.role_enabled?, which returns true unless +config.roles+ has
        # been narrowed.
        if config.role_enabled?(:workers)
          # slot is the child's position in the config array — it keys the
          # crash-streak tracking so identically-configured siblings don't
          # share (and reset) each other's restart backoff.
          Array(config.workers).each_with_index { |worker_config, slot| fork_worker(worker_config, slot: slot) }
        end

        fork_dispatcher if config.role_enabled?(:dispatcher)
        boot_scheduler if config.role_enabled?(:scheduler)
        boot_consumers if config.role_enabled?(:consumers)
        boot_outbox_poller if config.role_enabled?(:outbox)
      end

      def fork_worker(worker_config, slot: nil)
        queues = worker_config[:queues] || worker_config["queues"] || [config.default_queue]
        threads = worker_config[:threads] || worker_config["threads"] || 5
        single_active = worker_config[:single_active_consumer] || worker_config["single_active_consumer"] || false
        priority = worker_config[:consumer_priority] || worker_config["consumer_priority"] || 0
        exec_mode = config.execution_mode_for(worker_config)
        grp_mode = worker_config[:group_mode] || worker_config["group_mode"] || config.group_mode

        # OS-level liveness channel: the child writes a byte each loop
        # iteration, the parent drains the reader in monitor_loop. This lets
        # the watchdog detect a wedged worker without the database.
        liveness_reader, liveness_writer = IO.pipe

        pid = fork do
          # Child owns the writer end. A fork copies the whole parent FD
          # table, so this child also inherits every earlier sibling's
          # liveness reader (they live in @forks). Close them — otherwise each
          # new worker accumulates N-1 stale reader FDs. (Sibling *writers*
          # are never inherited: the parent closes each writer below before
          # forking the next worker, so a dead sibling's pipe still reaches
          # EOF correctly.) Finally close this fork's own reader copy.
          close_inherited_liveness_readers
          liveness_reader.close
          restore_signals
          setup_child_process
          load_rails_app
          bootstrap_queues
          worker = Worker.new(
            queues: queues, threads: threads, config: config,
            single_active_consumer: single_active, consumer_priority: priority,
            execution_mode: exec_mode, group_mode: grp_mode,
            liveness_pipe: liveness_writer
          )
          worker.run
        end

        unless pid
          close_pipe(liveness_reader)
          close_pipe(liveness_writer)
          Pgbus.logger.error { "[Pgbus] Failed to fork worker for queues=#{queues.join(",")}" }
          return
        end

        # Parent keeps the reader, discards its writer copy so the pipe reaches
        # EOF once the (sole remaining) child writer closes.
        close_pipe(liveness_writer)
        @forks[pid] = {
          type: :worker, config: worker_config, slot: slot, spawned_at: monotonic_now,
          liveness_reader: liveness_reader, last_pipe_tick_at: monotonic_now, pipe_seen: false
        }
        Pgbus.logger.info { "[Pgbus] Forked worker pid=#{pid} queues=#{queues.join(",")} mode=#{exec_mode}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        close_pipe(liveness_reader)
        close_pipe(liveness_writer)
        ErrorReporter.report(e, { action: "fork_worker", queues: queues })
      end

      def fork_dispatcher
        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          dispatcher = Dispatcher.new(config: config)
          dispatcher.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork dispatcher" }
          return
        end

        @forks[pid] = { type: :dispatcher, spawned_at: monotonic_now }
        Pgbus.logger.info { "[Pgbus] Forked dispatcher pid=#{pid}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        ErrorReporter.report(e, { action: "fork_dispatcher" })
      end

      def boot_scheduler
        return if config.skip_recurring
        return unless recurring_tasks_configured?

        fork_scheduler
      end

      def fork_scheduler
        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          load_recurring_config
          bootstrap_queues
          scheduler = Recurring::Scheduler.new(config: config)
          scheduler.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork scheduler" }
          return
        end

        @forks[pid] = { type: :scheduler, spawned_at: monotonic_now }
        Pgbus.logger.info { "[Pgbus] Forked scheduler pid=#{pid}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        ErrorReporter.report(e, { action: "fork_scheduler" })
      end

      def recurring_tasks_configured?
        return true if config.recurring_tasks&.any?

        files = config.recurring_tasks_files
        return true if files&.any? { |f| File.exist?(f.to_s) }

        return true if config.recurring_tasks_file && File.exist?(config.recurring_tasks_file.to_s)

        if defined?(Rails) && Rails.respond_to?(:root) && Rails.root
          default_path = Rails.root.join("config", "recurring.yml")
          return File.exist?(default_path.to_s)
        end

        false
      end

      def load_recurring_config
        return if config.recurring_tasks&.any?

        files = config.recurring_tasks_files
        if files
          tasks = Recurring::ConfigLoader.load_all(files)
          config.recurring_tasks = tasks unless tasks.empty?
          return if tasks.any?
        end

        path = config.recurring_tasks_file
        path ||= defined?(Rails) && Rails.respond_to?(:root) && Rails.root ? Rails.root.join("config", "recurring.yml") : nil
        return unless path && File.exist?(path.to_s)

        config.recurring_tasks = Recurring::ConfigLoader.load(path)
      end

      def boot_consumers
        return unless config.event_consumers

        config.event_consumers.each_with_index do |consumer_config, slot|
          fork_consumer(consumer_config, slot: slot)
        end
      end

      def fork_consumer(consumer_config, slot: nil)
        topics = consumer_config[:topics] || consumer_config["topics"]
        threads = consumer_config[:threads] || consumer_config["threads"] || 3

        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          consumer = Consumer.new(topics: topics, threads: threads, config: config)
          consumer.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork consumer for topics=#{topics.join(",")}" }
          return
        end

        @forks[pid] = { type: :consumer, config: consumer_config, slot: slot, spawned_at: monotonic_now }
        Pgbus.logger.info { "[Pgbus] Forked consumer pid=#{pid} topics=#{topics.join(",")}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        ErrorReporter.report(e, { action: "fork_consumer", topics: topics })
      end

      def boot_outbox_poller
        return unless config.outbox_enabled

        fork_outbox_poller
      end

      def fork_outbox_poller
        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          poller = Outbox::Poller.new(config: config)
          poller.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork outbox poller" }
          return
        end

        @forks[pid] = { type: :outbox_poller, spawned_at: monotonic_now }
        Pgbus.logger.info { "[Pgbus] Forked outbox poller pid=#{pid}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        ErrorReporter.report(e, { action: "fork_outbox_poller" })
      end

      def monitor_loop
        loop do
          break if @shutting_down && @forks.empty?

          process_signals
          reap_children
          drain_liveness_pipes
          unless @shutting_down
            process_pending_restarts
            check_stalled_workers
          end
          interruptible_sleep(FORK_WAIT)
        end
      end

      def reap_children
        loop do
          pid, status = ::Process.waitpid2(-1, ::Process::WNOHANG)
          break unless pid

          info = @forks.delete(pid)
          next unless info

          # Close the liveness reader as the fork leaves @forks so a crash-loop
          # (restart deferred up to RESTART_BACKOFF_MAX) can't leak an FD per
          # crash. Scrub the key so the closed IO never rides into a restart.
          close_pipe(info.delete(:liveness_reader))

          if @shutting_down
            Pgbus.logger.info { "[Pgbus] Child #{info[:type]} pid=#{pid} exited (status=#{status.exitstatus})" }
          else
            Pgbus.logger.warn do
              "[Pgbus] Child #{info[:type]} pid=#{pid} exited unexpectedly (status=#{status&.exitstatus})"
            end
            schedule_restart(info, status)
          end
        rescue Errno::ECHILD
          break
        end
      end

      # Restart policy: a clean exit (worker recycling) or a crash after a
      # stable run restarts immediately with a fresh crash streak. A crash
      # within RESTART_STABLE_UPTIME of forking is a crash loop — the child
      # is dying on boot (bad config, unreachable DB, raising initializer) —
      # so back off exponentially instead of fork-crash-forking at full speed.
      # A child with no spawned_at (never set in practice) restarts
      # immediately, preserving the pre-backoff behavior.
      def schedule_restart(info, status)
        # Keyed on [type, slot], NOT the config hash — identically-configured
        # sibling workers would otherwise share one streak, letting one
        # sibling's clean recycle reset another sibling's crash backoff.
        key = [info[:type], info[:slot]]
        uptime = info[:spawned_at] ? monotonic_now - info[:spawned_at] : nil

        if status&.success? || uptime.nil? || uptime >= RESTART_STABLE_UPTIME
          @crash_counts.delete(key)
          return restart_child(info)
        end

        crashes = @crash_counts[key] += 1
        backoff = [RESTART_BACKOFF_BASE * (2**(crashes - 1)), RESTART_BACKOFF_MAX].min
        Pgbus.logger.warn do
          "[Pgbus] Child #{info[:type]} crashed after #{uptime.round(1)}s uptime " \
            "(crash ##{crashes}) — restarting in #{backoff}s"
        end
        @pending_restarts << { info: info, at: monotonic_now + backoff }
      end

      def process_pending_restarts
        now = monotonic_now
        due, pending = @pending_restarts.partition { |r| r[:at] <= now }
        @pending_restarts = pending
        due.each { |r| restart_child(r[:info]) }
      end

      def restart_child(info)
        case info[:type]
        when :worker
          fork_worker(info[:config], slot: info[:slot])
        when :dispatcher
          fork_dispatcher
        when :scheduler
          fork_scheduler
        when :consumer
          fork_consumer(info[:config], slot: info[:slot])
        when :outbox_poller
          fork_outbox_poller
        end
      end

      def check_stalled_workers
        now = monotonic_now
        return if (now - @last_watchdog_at) < WATCHDOG_INTERVAL

        @last_watchdog_at = now
        threshold = config.stall_threshold
        return unless threshold&.positive?

        worker_pids = @forks.select { |_, info| info[:type] == :worker }.keys
        return if worker_pids.empty?

        db_ages = db_loop_tick_ages(worker_pids)

        worker_pids.each do |pid|
          info = @forks[pid]
          next unless info

          kill_stalled_worker(pid, threshold) if worker_stalled?(info, db_ages[pid], now, threshold)
        end
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Supervisor watchdog check failed: #{e.message}" }
      end

      # Read each worker's loop_tick_at from the process table and return a
      # {pid => wall-clock age in seconds} map. Isolated in its own rescue so a
      # database outage degrades to the OS-pipe fallback instead of skipping
      # the whole watchdog — the exact failure this pipe channel exists to fix.
      def db_loop_tick_ages(worker_pids)
        ages = {}
        ProcessEntry.where(kind: "worker", pid: worker_pids).to_a.each do |entry|
          meta = entry.metadata
          next unless meta.is_a?(Hash)

          loop_tick = meta["loop_tick_at"]
          ages[entry.pid] = Time.current.to_f - loop_tick.to_f if loop_tick
        end
        ages
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Supervisor watchdog DB read failed, using pipe fallback: #{e.message}" }
        ages
      end

      # A worker is stalled only when EVERY liveness channel that has spoken
      # agrees it is stale (min-age / fresh-wins): a fresh DB row OR a fresh
      # pipe tick proves the loop is advancing. If no channel has any signal
      # (a slow-booting worker with no DB row and an unarmed pipe), we do not
      # kill — mirroring the pre-existing "no loop_tick_at → skip" tolerance.
      def worker_stalled?(info, db_age, now, threshold)
        pipe_age = (now - info[:last_pipe_tick_at] if info[:pipe_seen] && info[:last_pipe_tick_at])
        ages = [db_age, pipe_age].compact
        return false if ages.empty?

        ages.min > threshold
      end

      def kill_stalled_worker(pid, threshold)
        Pgbus.logger.error do
          "[Pgbus] Supervisor watchdog: worker pid=#{pid} claim loop stalled " \
            "(no liveness within threshold=#{threshold}s), sending SIGKILL"
        end
        ::Process.kill("KILL", pid)
      rescue Errno::ESRCH
        # already gone
      end

      # Drain each worker's liveness pipe. Any readable byte means the worker's
      # loop advanced since the last drain, so we stamp arrival on the parent's
      # own monotonic clock (never a worker timestamp — that would cross the
      # fork's incomparable CLOCK_MONOTONIC) and arm pipe_seen. Bounded to a
      # few reads per reader so a fast-writing worker can't wedge the 1s loop;
      # "any bytes ⇒ alive" needs no full drain. Per-reader rescue so one
      # closed reader (racing reap/shutdown) can't skip the rest.
      def drain_liveness_pipes
        now = monotonic_now
        @forks.each_value do |info|
          reader = info[:liveness_reader]
          next unless reader

          read_any = false
          begin
            2.times do
              reader.read_nonblock(4096)
              read_any = true
            end
          rescue IO::WaitReadable, EOFError
            # empty / drained, or writer closed (worker exiting — reap handles it)
          rescue IOError, Errno::EBADF
            # reader closed by a racing reap/shutdown this tick
            next
          end

          if read_any
            info[:last_pipe_tick_at] = now
            info[:pipe_seen] = true
          end
        end
      end

      def signal_children(sig)
        @forks.each_key do |pid|
          ::Process.kill(sig, pid)
        rescue Errno::ESRCH
          # Process already gone
        end
      end

      def setup_child_process
        # Reset the PGMQ client so this forked process gets a fresh
        # PG::Connection instead of inheriting the parent's (which is
        # in undefined state post-fork and not thread-safe to share).
        Pgbus.reset_client!
        %w[INT TERM QUIT].each do |sig|
          trap(sig) { @shutting_down = true }
        end
      end

      def bootstrap_queues
        Pgbus.client.ensure_all_queues
      rescue StandardError => e
        ErrorReporter.report(e, { action: "bootstrap_queues" })
      end

      def load_rails_app
        return unless defined?(Rails) && Rails.respond_to?(:application) && Rails.application

        Rails.application.eager_load! if Rails.application.respond_to?(:eager_load!)
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "supervisor",
          metadata: { pid: ::Process.pid, hostname: Socket.gethostname }
        )
        @heartbeat.start
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      # Close a pipe IO idempotently. nil (non-worker forks, already-scrubbed
      # entries) and already-closed IOs are no-ops; a rare EBADF/IOError from a
      # racing close is swallowed. FD management is single-threaded (main loop
      # only), so the closed? check-then-close needs no lock.
      def close_pipe(io)
        io.close if io && !io.closed?
      rescue IOError, Errno::EBADF
        nil
      end

      # Close every sibling liveness reader this fork inherited from the
      # parent's FD table. Called only inside a just-forked child, before it
      # becomes a Worker, so the worker never holds its siblings' pipe ends.
      def close_inherited_liveness_readers
        @forks.each_value { |info| close_pipe(info[:liveness_reader]) }
      end

      def shutdown
        # Wait for all children with timeout
        deadline = Time.now + 30

        until @forks.empty? || Time.now > deadline
          reap_children
          interruptible_sleep(0.5)
        end

        # Force kill any remaining
        signal_children("KILL") unless @forks.empty?

        # Close any liveness readers still open on un-reaped children so the
        # supervisor never leaks FDs across a restart of itself.
        @forks.each_value { |info| close_pipe(info[:liveness_reader]) }

        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Supervisor stopped" }
      end
    end
  end
end
