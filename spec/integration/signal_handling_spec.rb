# frozen_string_literal: true

require_relative "../integration_helper"

# These tests fork child processes that create new PG connections.
# pg gem 1.6.x has a known segfault when connecting after fork on macOS ARM64
# (libpq SSL context corruption). These tests run reliably on Linux (CI).
FORK_PG_SAFE = !RUBY_PLATFORM.include?("darwin")

RSpec.describe "Signal handling (integration)", :integration do
  let(:client) { Pgbus.client }

  before do
    skip "Fork + PG.connect segfaults on macOS ARM64 (pg gem bug)" unless FORK_PG_SAFE
    client.ensure_queue("default")
  end

  # Build a connection URL with the given pool size, safely merging query params.
  def connection_url(pool:)
    parsed = URI.parse(PGBUS_DATABASE_URL)
    params = URI.decode_www_form(parsed.query || "").to_h
    params["pool"] = pool.to_s
    parsed.query = URI.encode_www_form(params)
    parsed.to_s
  end

  # Disconnect ActiveRecord before forking so the child gets a clean slate.
  # After the fork, the parent re-establishes its own connection.
  def fork_with_fresh_connections(&block)
    ActiveRecord::Base.connection_handler.clear_all_connections!
    Pgbus.reset_client!
    pid = fork(&block)
    # Parent: re-establish connection after fork
    ActiveRecord::Base.establish_connection(connection_url(pool: 20))
    Pgbus.reset_client!
    pid
  end

  # Force-kill and reap a child process if it's still running.
  def force_kill(pid)
    Process.kill("KILL", pid)
    Process.waitpid(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    # already exited/reaped
  end

  describe "worker graceful shutdown via SIGTERM" do
    it "stops polling and drains in-flight jobs before exiting" do
      # Enqueue a job before starting the worker
      payload = { "job_class" => "TestJob", "job_id" => SecureRandom.uuid, "arguments" => [] }
      client.send_message("default", payload)

      # Fork a worker process
      read_pipe, write_pipe = IO.pipe
      worker_pid = fork_with_fresh_connections do
        read_pipe.close
        Pgbus.reset_client!
        ActiveRecord::Base.establish_connection(connection_url(pool: 5))

        worker = Pgbus::Process::Worker.new(
          queues: ["default"],
          threads: 1,
          config: Pgbus.configuration
        )

        # Notify parent that worker is running
        worker_started = false
        original_run = worker.method(:claim_and_execute)
        worker.define_singleton_method(:claim_and_execute) do
          unless worker_started
            worker_started = true
            write_pipe.write("R")
            write_pipe.flush
          end
          original_run.call
        end

        worker.run
        write_pipe.write("D") # Signal clean exit
        write_pipe.close
      end

      write_pipe.close

      # Wait for worker to start polling
      ready = read_pipe.wait_readable(10)
      expect(ready).to be_truthy, "Worker did not start within 10s"
      expect(read_pipe.read(1)).to eq("R")

      # Send SIGTERM for graceful shutdown
      Process.kill("TERM", worker_pid)

      # Worker should exit cleanly within the shutdown timeout
      result = nil
      Timeout.timeout(15) do
        _, status = Process.waitpid2(worker_pid)
        result = status
      end

      expect(result.exitstatus).to eq(0), "Worker did not exit cleanly after SIGTERM (status=#{result&.exitstatus})"

      # Check for clean exit marker
      remaining = read_pipe.read
      read_pipe.close
      expect(remaining).to include("D"), "Worker did not reach clean exit point"
    ensure
      force_kill(worker_pid) if worker_pid
    end
  end

  describe "worker immediate shutdown via SIGQUIT" do
    it "kills the thread pool and exits immediately" do
      read_pipe, write_pipe = IO.pipe
      worker_pid = fork_with_fresh_connections do
        read_pipe.close
        Pgbus.reset_client!
        ActiveRecord::Base.establish_connection(connection_url(pool: 5))

        worker = Pgbus::Process::Worker.new(
          queues: ["default"],
          threads: 1,
          config: Pgbus.configuration
        )

        # Notify parent AFTER claim_and_execute is first called (signals are set up)
        worker_started = false
        original_run = worker.method(:claim_and_execute)
        worker.define_singleton_method(:claim_and_execute) do
          unless worker_started
            worker_started = true
            write_pipe.write("R")
            write_pipe.flush
            write_pipe.close
          end
          original_run.call
        end

        worker.run
      end

      write_pipe.close

      # Wait for worker to be ready (signals are set up, run loop entered)
      ready = read_pipe.wait_readable(10)
      expect(ready).to be_truthy, "Worker did not start within 10s"
      read_pipe.read(1)
      read_pipe.close

      Process.kill("QUIT", worker_pid)

      result = nil
      Timeout.timeout(10) do
        _, status = Process.waitpid2(worker_pid)
        result = status
      end

      expect(result.exitstatus).to eq(0), "Worker did not exit after SIGQUIT (status=#{result&.exitstatus})"
    ensure
      force_kill(worker_pid) if worker_pid
    end
  end

  describe "supervisor signal propagation" do
    it "SIGTERM propagates to all child processes" do
      read_pipe, write_pipe = IO.pipe

      supervisor_pid = fork_with_fresh_connections do
        read_pipe.close
        Pgbus.reset_client!
        ActiveRecord::Base.establish_connection(connection_url(pool: 5))

        # Minimal config: 1 worker, no scheduler, no dispatcher
        config = Pgbus.configuration.dup
        config.workers = [{ queues: ["default"], threads: 1 }]
        config.skip_recurring = true

        supervisor = Pgbus::Process::Supervisor.new(config: config)

        # Override boot_processes to fork a simple child that sleeps,
        # register it in @forks so the supervisor manages it properly.
        supervisor.define_singleton_method(:boot_processes) do
          child_pid = fork do
            # Simple child that exits on TERM
            trap("TERM") { exit 0 }
            trap("QUIT") { exit 0 }
            sleep 120
          end
          @forks[child_pid] = { type: :worker, config: {} }
          write_pipe.write("S")
          write_pipe.flush
        end

        supervisor.run
        write_pipe.write("X") # Clean exit
        write_pipe.close
      end

      write_pipe.close

      # Wait for supervisor to start
      ready = read_pipe.wait_readable(10)
      expect(ready).to be_truthy, "Supervisor did not start within 10s"
      expect(read_pipe.read(1)).to eq("S")

      # Send SIGTERM to supervisor
      Process.kill("TERM", supervisor_pid)

      # Supervisor should exit cleanly after draining children.
      # Use a generous timeout — CI runners can be slow to schedule processes.
      result = nil
      Timeout.timeout(30) do
        _, status = Process.waitpid2(supervisor_pid)
        result = status
      end

      expect(result.exitstatus).to eq(0), "Supervisor did not exit cleanly after SIGTERM (status=#{result&.exitstatus})"

      remaining = read_pipe.read
      read_pipe.close
      expect(remaining).to include("X"), "Supervisor did not reach clean exit"
    ensure
      force_kill(supervisor_pid) if supervisor_pid
    end
  end

  describe "worker processes jobs before shutdown" do
    it "completes in-flight job when SIGTERM arrives during execution" do
      # Create a marker path — close! unlinks it so the file only exists
      # if the job actually writes it, avoiding a false positive.
      marker = Tempfile.new("pgbus_signal_test")
      marker_path = marker.path
      marker.close!

      # Enqueue a "job" with a marker path
      payload = {
        "job_class" => "TestJob",
        "job_id" => SecureRandom.uuid,
        "arguments" => [],
        "marker_path" => marker_path
      }
      client.send_message("default", payload)

      read_pipe, write_pipe = IO.pipe
      worker_pid = fork_with_fresh_connections do
        read_pipe.close
        Pgbus.reset_client!
        ActiveRecord::Base.establish_connection(connection_url(pool: 5))

        worker = Pgbus::Process::Worker.new(
          queues: ["default"],
          threads: 1,
          config: Pgbus.configuration
        )

        # Override process_message to write marker instead of running ActiveJob.
        # Must accept source_queue: keyword to match the call site in claim_and_execute.
        worker.define_singleton_method(:process_message) do |message, queue_name, **_kwargs|
          parsed = begin
            JSON.parse(message.message)
          rescue StandardError
            {}
          end
          if parsed["marker_path"]
            write_pipe.write("J") # Signal job picked up
            write_pipe.flush
            sleep 1 # Simulate work — long enough for parent to send SIGTERM
            File.write(parsed["marker_path"], "completed")
          end
          Pgbus.client.archive_message(queue_name, message.msg_id)
        rescue StandardError => e
          warn "[child] process_message error: #{e.class}: #{e.message}"
        ensure
          @in_flight&.decrement
        end

        worker.run
        write_pipe.close
      end

      write_pipe.close

      # Wait for the worker to pick up the job via pipe
      job_started = read_pipe.wait_readable(15)
      expect(job_started).to be_truthy, "Worker did not pick up the job within 15s"
      expect(read_pipe.read(1)).to eq("J")

      # Send SIGTERM while job is in-flight (worker is sleeping 1s)
      Process.kill("TERM", worker_pid)

      Timeout.timeout(15) do
        Process.waitpid2(worker_pid)
      end
      read_pipe.close

      # The marker file should exist with expected content if the job completed
      expect(File.exist?(marker_path)).to be true
      expect(File.read(marker_path)).to eq("completed")
    ensure
      force_kill(worker_pid) if worker_pid
      File.delete(marker_path) if marker_path && File.exist?(marker_path)
    end
  end
end
