# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Supervisor do
  let(:mock_heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: nil, stop: nil) }
  let(:config) { Pgbus.configuration }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(mock_heartbeat)
  end

  describe "#initialize" do
    it "stores config and initializes empty forks" do
      supervisor = described_class.new

      expect(supervisor.config).to eq(config)
      expect(supervisor.forks).to eq({})
      expect(supervisor.shutting_down?).to be false
    end
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down and signals children with TERM" do
      supervisor = described_class.new(forks: { 1001 => { type: :worker }, 1002 => { type: :dispatcher } })

      allow(Process).to receive(:kill)
      supervisor.graceful_shutdown

      expect(supervisor.shutting_down?).to be true
      expect(Process).to have_received(:kill).with("TERM", 1001)
      expect(Process).to have_received(:kill).with("TERM", 1002)
    end
  end

  describe "#immediate_shutdown" do
    it "sets shutting_down and signals children with QUIT" do
      supervisor = described_class.new(forks: { 2001 => { type: :worker } })

      allow(Process).to receive(:kill)
      supervisor.immediate_shutdown

      expect(supervisor.shutting_down?).to be true
      expect(Process).to have_received(:kill).with("QUIT", 2001)
    end
  end

  describe "signal_children (private)" do
    it "handles Errno::ESRCH when a child process is already gone" do
      supervisor = described_class.new(forks: { 9999 => { type: :worker } })

      allow(Process).to receive(:kill).with("TERM", 9999).and_raise(Errno::ESRCH)

      expect { supervisor.send(:signal_children, "TERM") }.not_to raise_error
    end
  end

  describe "reap_children (private)" do
    let(:supervisor) do
      described_class.new(forks: { 3001 => { type: :worker, config: { queues: ["default"] } } })
    end
    let(:status_double) { instance_double(Process::Status, exitstatus: 1, success?: false) }

    it "restarts a child when not shutting down" do
      allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, status_double], nil)
      allow(supervisor).to receive(:fork).and_return(4001)

      supervisor.send(:reap_children)

      expect(supervisor).to have_received(:fork)
    end

    it "does NOT restart a child when shutting_down is true" do
      supervisor = described_class.new(
        forks: { 3001 => { type: :worker, config: { queues: ["default"] } } },
        shutting_down: true
      )
      allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, status_double], nil)

      supervisor.send(:reap_children)

      expect(supervisor.forks).not_to have_key(3001)
    end
  end

  # A child that dies right after forking (bad config, unreachable DB,
  # crashing initializer) must not be re-forked in a tight loop — that
  # burns CPU and floods logs. Crashes within the stable-uptime window
  # get an exponentially backed-off restart; stable children and clean
  # exits (worker recycling) restart immediately.
  describe "restart backoff for crash-looping children" do
    # Each example seeds a distinct set of child forks; inject them via the
    # constructor and re-apply the standard fork/log stubs via build_supervisor.
    let(:crash_status) { instance_double(Process::Status, exitstatus: 1, success?: false) }
    let(:clean_status) { instance_double(Process::Status, exitstatus: 0, success?: true) }
    let(:worker_config) { { queues: ["default"], threads: 5 } }

    def build_supervisor(forks)
      described_class.new(forks: forks).tap do |s|
        allow(s).to receive(:fork).and_return(6001)
        allow(Pgbus.logger).to receive(:warn)
      end
    end

    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    it "restarts immediately when the crashed child had a stable uptime" do
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: now - 120 })
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)

      supervisor.send(:reap_children)

      expect(supervisor).to have_received(:fork).once
    end

    it "restarts immediately on a clean exit even with short uptime (worker recycling)" do
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: now - 5 })
      allow(Process).to receive(:waitpid2).and_return([3001, clean_status], nil)

      supervisor.send(:reap_children)

      expect(supervisor).to have_received(:fork).once
    end

    it "delays the restart when the child crashed shortly after forking" do
      base = now
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: base - 1 })
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)

      supervisor.send(:reap_children)
      supervisor.send(:process_pending_restarts)

      expect(supervisor).not_to have_received(:fork)
    end

    it "keeps separate crash streaks for identically-configured sibling workers" do
      messages = []
      base = now
      supervisor = build_supervisor(sibling_forks(base))
      allow(Pgbus.logger).to receive(:warn) { |&blk| messages << blk.call }
      allow(supervisor).to receive(:fork).and_return(6001, 6002, 6003)
      allow(supervisor).to receive(:monotonic_now).and_return(base)

      reap(supervisor, 3002, crash_status) # slot 1 crashes fast — crash #1, 1s backoff
      reap(supervisor, 3001, clean_status) # slot 0 recycles cleanly — must NOT reset slot 1's streak

      # Slot 1's restart fires, then it crashes fast again — crash #2, 2s backoff.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)
      restarted_pid = supervisor.forks.find { |_, i| i[:slot] == 1 }.first
      reap(supervisor, restarted_pid, crash_status)

      expect(messages.join("\n")).to include("restarting in 2s")
    end

    def sibling_forks(base)
      { 3001 => { type: :worker, config: worker_config, slot: 0, spawned_at: base },
        3002 => { type: :worker, config: worker_config, slot: 1, spawned_at: base } }
    end

    def reap(supervisor, pid, status)
      allow(Process).to receive(:waitpid2).and_return([pid, status], nil)
      supervisor.send(:reap_children)
    end

    it "fires due pending restarts from the monitor loop" do
      supervisor = described_class.new(pending_restarts: [{ info: { type: :dispatcher }, at: now - 1 }])
      allow(supervisor).to receive(:fork).and_return(7001)
      allow(Pgbus.logger).to receive(:warn)
      allow(Process).to receive(:waitpid2).and_return(nil)
      # Stop the loop after one pass: flip shutdown, and let reap_children clear
      # the just-restarted fork so the `@shutting_down && @forks.empty?` guard
      # trips (mirrors the real path where the exited child is reaped).
      allow(supervisor).to receive(:interruptible_sleep) do
        supervisor.graceful_shutdown
        allow(supervisor).to receive(:reap_children) { supervisor.forks.clear }
      end
      allow(supervisor).to receive(:check_stalled_workers)

      supervisor.send(:monitor_loop)

      expect(supervisor).to have_received(:fork).once
    end

    it "does not fire pending restarts once shutting down" do
      supervisor = described_class.new(
        forks: { 8001 => { type: :worker, config: worker_config, spawned_at: now } },
        shutting_down: true,
        pending_restarts: [{ info: { type: :dispatcher }, at: now - 1 }]
      )
      allow(supervisor).to receive(:fork)
      allow(Pgbus.logger).to receive(:warn)
      allow(Process).to receive(:waitpid2).and_return([8001, crash_status], nil)
      allow(supervisor).to receive(:interruptible_sleep)

      supervisor.send(:monitor_loop)

      expect(supervisor).not_to have_received(:fork)
    end

    it "runs the pending restart once its backoff has elapsed" do
      base = now
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: base })
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)

      supervisor.send(:reap_children)
      expect(supervisor).not_to have_received(:fork)

      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)

      expect(supervisor).to have_received(:fork)
    end

    it "escalates the backoff on consecutive rapid crashes" do
      messages = []
      base = now
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: base })
      allow(Pgbus.logger).to receive(:warn) { |&blk| messages << blk.call }
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)
      supervisor.send(:reap_children)

      # First backoff elapses; the child is restarted, then crashes again fast.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)
      allow(Process).to receive(:waitpid2).and_return([6001, crash_status], nil)
      supervisor.send(:reap_children)

      joined = messages.join("\n")
      expect(joined).to include("restarting in 1s")
      expect(joined).to include("restarting in 2s")
    end

    it "resets the crash streak after a stable run" do
      messages = []
      base = now
      supervisor = build_supervisor(3001 => { type: :worker, config: worker_config, spawned_at: base })
      allow(Pgbus.logger).to receive(:warn) { |&blk| messages << blk.call }
      allow(supervisor).to receive(:monotonic_now).and_return(base)
      allow(Process).to receive(:waitpid2).and_return([3001, crash_status], nil)
      supervisor.send(:reap_children)

      # Backoff elapses, child restarts and then runs well past the stable window.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 61)
      supervisor.send(:process_pending_restarts)

      # Stable crash: restarts immediately and clears the streak...
      allow(supervisor).to receive(:monotonic_now).and_return(base + 200)
      allow(Process).to receive(:waitpid2).and_return([6001, crash_status], nil)
      supervisor.send(:reap_children)

      # ...so the next rapid crash starts back at the base backoff.
      allow(supervisor).to receive(:monotonic_now).and_return(base + 201)
      allow(Process).to receive(:waitpid2).and_return([6001, crash_status], nil)
      supervisor.send(:reap_children)

      expect(messages.join("\n").scan("restarting in 1s").size).to eq(2)
    end
  end

  describe "restart_child (private)" do
    let(:supervisor) { described_class.new }

    before do
      allow(supervisor).to receive(:fork).and_return(5001)
    end

    it "routes :worker to fork_worker" do
      info = { type: :worker, config: { queues: ["default"], threads: 5 } }
      supervisor.send(:restart_child, info)

      expect(supervisor.forks).to have_key(5001)
      expect(supervisor.forks[5001][:type]).to eq(:worker)
    end

    it "routes :dispatcher to fork_dispatcher" do
      info = { type: :dispatcher }
      supervisor.send(:restart_child, info)

      expect(supervisor.forks).to have_key(5001)
      expect(supervisor.forks[5001][:type]).to eq(:dispatcher)
    end

    it "routes :consumer to fork_consumer" do
      info = { type: :consumer, config: { topics: ["orders.#"], threads: 3 } }
      supervisor.send(:restart_child, info)

      expect(supervisor.forks).to have_key(5001)
      expect(supervisor.forks[5001][:type]).to eq(:consumer)
    end

    it "routes :scheduler to fork_scheduler" do
      info = { type: :scheduler }
      supervisor.send(:restart_child, info)

      expect(supervisor.forks).to have_key(5001)
      expect(supervisor.forks[5001][:type]).to eq(:scheduler)
    end
  end

  describe "worker liveness pipe (private)" do
    let(:supervisor) { described_class.new }
    let(:worker_config) { { queues: ["default"], threads: 5 } }

    # With fork stubbed to return a pid, the child block never runs, so the
    # parent path (create pipe, close writer, store reader) is exercised.
    before do
      allow(supervisor).to receive(:fork).and_return(7001)
    end

    describe "fork_worker" do
      it "stores a liveness_reader IO for the forked worker" do
        supervisor.send(:fork_worker, worker_config, slot: 0)

        info = supervisor.forks[7001]
        expect(info[:liveness_reader]).to be_a(IO)
        expect(info[:liveness_reader]).not_to be_closed
      end

      it "seeds last_pipe_tick_at (monotonic) and leaves pipe_seen unarmed" do
        supervisor.send(:fork_worker, worker_config, slot: 0)

        info = supervisor.forks[7001]
        expect(info[:last_pipe_tick_at]).to be_a(Float)
        expect(info[:pipe_seen]).to be(false)
      end

      it "closes both pipe ends and stores no reader when the fork fails (nil pid)" do
        allow(supervisor).to receive(:fork).and_return(nil)
        created = []
        allow(IO).to receive(:pipe).and_wrap_original do |orig|
          pair = orig.call
          created.concat(pair)
          pair
        end

        supervisor.send(:fork_worker, worker_config, slot: 0)

        expect(supervisor.forks).not_to have_key(7001)
        expect(created).to all(be_closed)
      end

      it "closes both pipe ends when the fork raises Errno::EAGAIN" do
        allow(supervisor).to receive(:fork).and_raise(Errno::EAGAIN)
        created = []
        allow(IO).to receive(:pipe).and_wrap_original do |orig|
          pair = orig.call
          created.concat(pair)
          pair
        end

        expect { supervisor.send(:fork_worker, worker_config, slot: 0) }.not_to raise_error
        expect(created).to all(be_closed)
      end
    end

    # Consumer forks get the same OS-pipe liveness channel as workers (issue
    # #274) so the watchdog can detect a wedged consumer without the database.
    describe "fork_consumer (issue #274)" do
      let(:consumer_config) { { topics: ["orders.#"], threads: 3 } }

      it "stores an open liveness_reader IO for the forked consumer" do
        supervisor.send(:fork_consumer, consumer_config, slot: 0)

        info = supervisor.forks[7001]
        expect(info[:type]).to eq(:consumer)
        expect(info[:liveness_reader]).to be_a(IO)
        expect(info[:liveness_reader]).not_to be_closed
      end

      it "seeds last_pipe_tick_at (monotonic) and leaves pipe_seen unarmed" do
        supervisor.send(:fork_consumer, consumer_config, slot: 0)

        info = supervisor.forks[7001]
        expect(info[:last_pipe_tick_at]).to be_a(Float)
        expect(info[:pipe_seen]).to be(false)
      end

      it "closes both pipe ends and stores no reader when the fork fails (nil pid)" do
        allow(supervisor).to receive(:fork).and_return(nil)
        created = []
        allow(IO).to receive(:pipe).and_wrap_original do |orig|
          pair = orig.call
          created.concat(pair)
          pair
        end

        supervisor.send(:fork_consumer, consumer_config, slot: 0)

        expect(supervisor.forks).not_to have_key(7001)
        expect(created).to all(be_closed)
      end
    end

    describe "reap_children closes the liveness reader" do
      let(:status_double) { instance_double(Process::Status, exitstatus: 0, success?: true) }

      it "closes the reader and drops the fork on reap" do
        reader, writer = IO.pipe
        writer.close
        supervisor = described_class.new(
          forks: { 3001 => { type: :worker, config: worker_config, spawned_at: 0.0,
                             liveness_reader: reader, last_pipe_tick_at: 0.0, pipe_seen: true } },
          shutting_down: true
        )
        allow(Process).to receive(:waitpid2).with(-1, Process::WNOHANG).and_return([3001, status_double], nil)

        supervisor.send(:reap_children)

        expect(reader).to be_closed
        expect(supervisor.forks).not_to have_key(3001)
      end
    end

    describe "shutdown closes remaining liveness readers" do
      it "closes any un-reaped worker readers" do
        reader, writer = IO.pipe
        writer.close
        supervisor = described_class.new(
          forks: { 3001 => { type: :worker, config: worker_config, liveness_reader: reader } }
        )
        allow(supervisor).to receive(:reap_children)
        allow(supervisor).to receive(:interruptible_sleep)
        allow(Process).to receive(:kill)

        supervisor.send(:shutdown)

        expect(reader).to be_closed
      end
    end
  end

  describe "bootstrap_queues (private)" do
    let(:supervisor) { described_class.new }
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive(:ensure_all_queues)
    end

    it "calls ensure_all_queues on the client" do
      supervisor.send(:bootstrap_queues)

      expect(mock_client).to have_received(:ensure_all_queues).once
    end

    it "rescues errors and logs them" do
      allow(mock_client).to receive(:ensure_all_queues).and_raise(StandardError, "connection failed")
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.send(:bootstrap_queues) }.not_to raise_error
      expect(Pgbus.logger).to have_received(:error).at_least(:once)
    end

    it "swallows SchemaNotReady so the parent survives a missing schema at boot" do
      allow(mock_client).to receive(:ensure_all_queues)
        .and_raise(Pgbus::SchemaNotReady, "PGMQ schema installation failed. Ensure migrations have been run.")
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.send(:bootstrap_queues) }.not_to raise_error
    end
  end

  describe "bootstrap_queues! (private)" do
    let(:supervisor) { described_class.new }
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive(:ensure_all_queues)
    end

    it "calls ensure_all_queues on the client" do
      supervisor.send(:bootstrap_queues!)

      expect(mock_client).to have_received(:ensure_all_queues).once
    end

    it "logs one actionable error and re-raises on SchemaNotReady" do
      message = "PGMQ schema installation failed. Ensure the pgbus database exists and migrations have been run."
      allow(mock_client).to receive(:ensure_all_queues).and_raise(Pgbus::SchemaNotReady, message)
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.send(:bootstrap_queues!) }.to raise_error(Pgbus::SchemaNotReady, message)
      expect(Pgbus.logger).to have_received(:error).once
    end

    it "reports and swallows a non-schema StandardError, as the lenient variant does" do
      allow(mock_client).to receive(:ensure_all_queues).and_raise(StandardError, "connection failed")
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.send(:bootstrap_queues!) }.not_to raise_error
    end
  end

  describe "pre-fork bootstrap" do
    let(:supervisor) { described_class.new }
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus).to receive(:client).and_return(mock_client)
      # run now emits log_boot_banner, which reads the installed PGMQ version.
      allow(mock_client).to receive_messages(
        ensure_all_queues: nil, verify_connection!: true, pgmq_schema_version: "1.5.0"
      )
    end

    it "calls bootstrap_queues before boot_processes" do
      call_order = []
      allow(supervisor).to receive(:bootstrap_queues) { call_order << :bootstrap }
      allow(supervisor).to receive(:boot_processes) { call_order << :boot }
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      supervisor.run

      expect(call_order).to eq(%i[bootstrap boot])
    end

    it "verifies the database connection once before bootstrapping queues" do
      call_order = []
      allow(mock_client).to receive(:verify_connection!) { call_order << :verify }
      allow(supervisor).to receive(:bootstrap_queues) { call_order << :bootstrap }
      allow(supervisor).to receive(:boot_processes) { call_order << :boot }
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      supervisor.run

      expect(call_order).to eq(%i[verify bootstrap boot])
      expect(mock_client).to have_received(:verify_connection!).once
    end

    it "propagates a ConfigurationError from verification without booting children" do
      allow(mock_client).to receive(:verify_connection!)
        .and_raise(Pgbus::ConfigurationError, "Database connection failed via database_url: nope")
      allow(supervisor).to receive(:bootstrap_queues)
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      expect { supervisor.run }.to raise_error(Pgbus::ConfigurationError, /database_url/)

      expect(supervisor).not_to have_received(:bootstrap_queues)
      expect(supervisor).not_to have_received(:boot_processes)
    end

    it "rescues bootstrap errors in the parent without aborting" do
      allow(supervisor).to receive(:bootstrap_queues).and_call_original
      allow(mock_client).to receive(:ensure_all_queues).and_raise(StandardError, "pg down")
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)
      allow(Pgbus.logger).to receive(:error)

      expect { supervisor.run }.not_to raise_error
    end
  end

  describe "boot diagnostics banner (private)" do
    let(:supervisor) { described_class.new(config: config) }
    let(:config) { Pgbus::Configuration.new }
    let(:messages) { [] }

    before do
      allow(Pgbus.logger).to receive(:info) { |&blk| messages << blk.call if blk }
    end

    # Capture only the banner text: every banner line is prefixed "[Pgbus] boot:".
    def banner
      messages.select { |m| m.to_s.start_with?("[Pgbus] boot:") }.join("\n")
    end

    it "logs one banner block with version, pool size, pgmq mode, notify flags, and roles" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return("1.5.0")

      supervisor.send(:log_boot_banner)

      expect(banner).to include("pgbus #{Pgbus::VERSION}")
      expect(banner).to include("pool=#{config.resolved_pool_size}")
      expect(banner).to include("pgmq_schema_mode=auto")
      expect(banner).to include("pgmq_version=1.5.0")
      expect(banner).to include("listen_notify=true")
      expect(banner).to include("worker_notify_wakeup=true")
    end

    it "renders the connection target as host + dbname without the password" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("db")
      expect(banner).to include("app")
      expect(banner).not_to include("sekret")
    end

    it "redacts the password from a connection_params hash" do
      config.connection_params = { host: "pghost", dbname: "myapp", user: "u", password: "sekret" }
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("pghost")
      expect(banner).to include("myapp")
      expect(banner).not_to include("sekret")
    end

    it "degrades the pgmq version to 'unknown' when the lookup raises, without aborting" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_raise(StandardError, "pg down")

      expect { supervisor.send(:log_boot_banner) }.not_to raise_error
      expect(banner).to include("pgmq_version=unknown")
    end

    # Distinct from the raising case above: PGMQ is reachable but nothing is
    # tracked yet, so pgmq_schema_version returns nil. Locks in the `|| "unknown"`
    # branch separately from the rescue branch.
    it "renders the pgmq version as 'unknown' when the lookup returns nil (not installed)" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("pgmq_version=unknown")
    end

    it "reflects a narrowed roles list" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.roles = [:workers]
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("workers")
      expect(banner).not_to match(/roles=.*dispatcher/)
    end

    it "lists worker capsules with queues, threads, and execution mode" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.workers = [
        { name: "critical", queues: %w[critical], threads: 5 },
        { name: "default", queues: %w[default low], threads: 3 }
      ]
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("critical")
      expect(banner).to include("queues=critical")
      expect(banner).to include("threads=5")
      expect(banner).to include("queues=default,low")
      expect(banner).to include("threads=3")
    end

    it "names an unnamed capsule 'anonymous'" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.workers = [{ queues: %w[default], threads: 5 }]
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("anonymous")
    end

    it "lists event consumers with topics and threads" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.event_consumers = [{ topics: %w[orders.# payments.#], threads: 4 }]
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return(nil)

      supervisor.send(:log_boot_banner)

      expect(banner).to include("orders.#,payments.#")
      expect(banner).to include("threads=4")
    end

    it "renders under the JSON log formatter without raising" do
      config.database_url = "postgres://u:sekret@db:5432/app"
      config.log_format = :json
      allow(Pgbus.client).to receive(:pgmq_schema_version).and_return("1.5.0")

      formatted = []
      formatter = config.logger.formatter
      allow(Pgbus.logger).to receive(:info) do |&blk|
        line = blk.call
        formatted << formatter.call("INFO", Time.now, nil, line)
      end

      expect { supervisor.send(:log_boot_banner) }.not_to raise_error
      json_lines = formatted.select { |l| l.include?("boot:") }
      expect(json_lines).not_to be_empty
      json_lines.each { |l| expect { JSON.parse(l) }.not_to raise_error }
      expect(formatted.join).not_to include("sekret")
    end
  end

  describe "banner is emitted during #run" do
    let(:supervisor) { described_class.new(config: config) }
    let(:config) { Pgbus::Configuration.new }
    let(:mock_client) { build_mock_client }

    before do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive_messages(
        verify_connection!: true, ensure_all_queues: nil, pgmq_schema_version: "1.5.0"
      )
    end

    it "logs the banner after start_heartbeat and before bootstrap_queues" do
      call_order = []
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:start_heartbeat) { call_order << :heartbeat }
      allow(supervisor).to receive(:log_boot_banner) { call_order << :banner }
      allow(supervisor).to receive(:bootstrap_queues) { call_order << :bootstrap }
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:monitor_loop)
      allow(supervisor).to receive(:shutdown)

      supervisor.run

      expect(call_order).to eq(%i[heartbeat banner bootstrap])
    end
  end

  describe "standalone health server" do
    let(:supervisor) { described_class.new(config: config) }
    let(:config) { Pgbus::Configuration.new }
    let(:mock_client) { build_mock_client }
    let(:health_server) { instance_double(Pgbus::Web::HealthServer, start: nil, stop: nil) }

    before do
      config.database_url = "postgres://u:sekret@db:5432/app"
      allow(Pgbus).to receive(:client).and_return(mock_client)
      allow(mock_client).to receive_messages(
        verify_connection!: true, ensure_all_queues: nil, pgmq_schema_version: "1.5.0"
      )
      allow(Pgbus::Web::HealthServer).to receive(:new).and_return(health_server)
      allow(supervisor).to receive(:setup_signals)
      allow(supervisor).to receive(:log_boot_banner)
      allow(supervisor).to receive(:boot_processes)
      allow(supervisor).to receive(:monitor_loop)
    end

    it "does not build a health server when health_port is nil" do
      config.health_port = nil

      supervisor.run

      expect(Pgbus::Web::HealthServer).not_to have_received(:new)
    end

    it "starts a health server with the configured port and bind when health_port is set" do
      config.health_port = 9394
      config.health_bind = "0.0.0.0"

      supervisor.run

      expect(Pgbus::Web::HealthServer).to have_received(:new).with(port: 9394, bind: "0.0.0.0")
      expect(health_server).to have_received(:start)
    end

    it "starts the health server after the heartbeat" do
      config.health_port = 9394
      call_order = []
      allow(supervisor).to receive(:start_heartbeat) { call_order << :heartbeat }
      allow(health_server).to receive(:start) { call_order << :health }

      supervisor.run

      expect(call_order).to eq(%i[heartbeat health])
    end

    it "stops the health server on shutdown" do
      config.health_port = 9394

      supervisor.run

      expect(health_server).to have_received(:stop)
    end
  end

  describe "recurring_tasks_configured? (private)" do
    let(:supervisor) { described_class.new }

    it "returns true when recurring_tasks are set in config" do
      config.recurring_tasks = { "task1" => { "class" => "MyJob", "schedule" => "0 * * * *" } }
      expect(supervisor.send(:recurring_tasks_configured?)).to be true
      config.recurring_tasks = nil
    end

    it "returns false when nothing is configured" do
      config.recurring_tasks = nil
      config.recurring_tasks_file = nil
      expect(supervisor.send(:recurring_tasks_configured?)).to be false
    end
  end
end
