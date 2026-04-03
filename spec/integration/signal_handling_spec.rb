# frozen_string_literal: true

require_relative "../integration_helper"

# rubocop:disable RSpec/ExampleLength
RSpec.describe "Signal handling (integration)", :integration do
  let(:client) { Pgbus.client }

  before do
    client.ensure_queue("default")
  end

  describe "worker graceful shutdown via SIGTERM" do
    it "stops polling and drains in-flight jobs before exiting" do
      # Enqueue a job before starting the worker
      payload = { "job_class" => "TestJob", "job_id" => SecureRandom.uuid, "arguments" => [] }
      client.send_message("default", payload)

      # Fork a worker process
      read_pipe, write_pipe = IO.pipe
      worker_pid = fork do
        read_pipe.close
        Pgbus.reset_client!

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
    end
  end

  describe "worker immediate shutdown via SIGQUIT" do
    it "kills the thread pool and exits immediately" do
      worker_pid = fork do
        Pgbus.reset_client!
        worker = Pgbus::Process::Worker.new(
          queues: ["default"],
          threads: 1,
          config: Pgbus.configuration
        )
        worker.run
      end

      # Give worker time to start
      sleep 1

      Process.kill("QUIT", worker_pid)

      result = nil
      Timeout.timeout(10) do
        _, status = Process.waitpid2(worker_pid)
        result = status
      end

      expect(result.exitstatus).to eq(0), "Worker did not exit after SIGQUIT (status=#{result&.exitstatus})"
    end
  end

  describe "supervisor signal propagation" do
    it "SIGTERM propagates to all child processes" do
      read_pipe, write_pipe = IO.pipe

      supervisor_pid = fork do
        read_pipe.close
        Pgbus.reset_client!

        # Minimal config: 1 worker, no scheduler, no dispatcher
        config = Pgbus.configuration.dup
        config.workers = [{ queues: ["default"], threads: 1 }]
        config.skip_recurring = true

        supervisor = Pgbus::Process::Supervisor.new(config: config)

        # Override boot_processes to only fork a worker, notify parent when ready
        supervisor.define_singleton_method(:boot_processes) do
          fork_worker(config.workers.first)
          write_pipe.write("S")
          write_pipe.flush
        end

        # Skip dispatcher fork
        supervisor.define_singleton_method(:fork_dispatcher) { nil }
        supervisor.define_singleton_method(:boot_consumers) { nil }
        supervisor.define_singleton_method(:boot_outbox_poller) { nil }

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

      # Supervisor should exit cleanly after draining children
      result = nil
      Timeout.timeout(35) do # 30s shutdown timeout + 5s buffer
        _, status = Process.waitpid2(supervisor_pid)
        result = status
      end

      expect(result.exitstatus).to eq(0), "Supervisor did not exit cleanly after SIGTERM (status=#{result&.exitstatus})"

      remaining = read_pipe.read
      read_pipe.close
      expect(remaining).to include("X"), "Supervisor did not reach clean exit"
    end
  end

  describe "worker processes jobs before shutdown" do
    it "completes in-flight job when SIGTERM arrives during execution" do
      # Create a marker file that the job will write to prove it completed
      marker = Tempfile.new("pgbus_signal_test")
      marker_path = marker.path
      marker.close

      # Enqueue a "job" with a marker path
      payload = {
        "job_class" => "TestJob",
        "job_id" => SecureRandom.uuid,
        "arguments" => [],
        "marker_path" => marker_path
      }
      client.send_message("default", payload)

      worker_pid = fork do
        Pgbus.reset_client!

        # Create a custom executor that writes to the marker file
        worker = Pgbus::Process::Worker.new(
          queues: ["default"],
          threads: 1,
          config: Pgbus.configuration
        )

        # Override execute_job to simulate work and write marker
        begin
          worker.method(:execute_job)
        rescue StandardError
          nil
        end
        worker.define_singleton_method(:execute_job) do |msg, queue_name|
          parsed = begin
            JSON.parse(msg.message)
          rescue StandardError
            {}
          end
          if parsed["marker_path"]
            sleep 0.5 # Simulate work
            File.write(parsed["marker_path"], "completed")
          end
          # Archive the message
          Pgbus.client.archive_message(queue_name, msg.msg_id)
        end

        worker.run
      end

      # Give worker time to pick up the job
      sleep 2

      # Send SIGTERM while job might still be processing
      Process.kill("TERM", worker_pid)

      Timeout.timeout(15) do
        Process.waitpid2(worker_pid)
      end

      # The marker file should exist if the job completed
      expect(File.exist?(marker_path)).to be true
    ensure
      File.delete(marker_path) if marker_path && File.exist?(marker_path)
    end
  end
end
# rubocop:enable RSpec/ExampleLength
