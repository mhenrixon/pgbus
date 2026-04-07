# frozen_string_literal: true

require "optparse"

module Pgbus
  module CLI
    module_function

    def start(args)
      command = args.first || "help"

      case command
      when "start"
        start_supervisor(args[1..] || [])
      when "status"
        show_status
      when "queues"
        list_queues
      when "version"
        puts "pgbus #{Pgbus::VERSION}"
      when "help", "--help", "-h"
        print_help
      else
        puts "Unknown command: #{command}"
        print_help
        exit 1
      end
    end

    def start_supervisor(args = [])
      apply_start_options(args)
      Pgbus.logger.info { "[Pgbus] Starting Pgbus #{Pgbus::VERSION}..." }

      supervisor = Process::Supervisor.new
      supervisor.run
    end

    # Parses CLI flags for `pgbus start` and applies them to the global
    # configuration before the supervisor boots. Designed to override the
    # initializer config without requiring a redeploy.
    def apply_start_options(args)
      options = parse_start_options(args)

      Pgbus.configuration.workers = options[:queues] if options[:queues]
      apply_capsule_filter(options[:capsule]) if options[:capsule]
    end

    def parse_start_options(args)
      options = {}
      OptionParser.new do |opts|
        opts.banner = "Usage: pgbus start [options]"

        opts.on("--queues STRING", "Override worker capsules (e.g. \"critical: 5; default: 10\")") do |v|
          options[:queues] = v
        end

        opts.on("--capsule NAME", "Run only the named capsule from the configured workers") do |v|
          options[:capsule] = v
        end
      end.parse!(args.dup)
      options
    end

    def apply_capsule_filter(name)
      capsule = Pgbus.configuration.capsule_named(name)
      unless capsule
        available = (Pgbus.configuration.workers || []).filter_map { |c| c[:name] || c["name"] }.compact
        raise ArgumentError,
              "no capsule named #{name.inspect} (available: #{available.join(", ")})"
      end

      # Go through the public setter so any future normalization/validation
      # in workers= is applied consistently to the CLI override path too.
      Pgbus.configuration.workers = [capsule]
    end

    def show_status
      processes = ProcessEntry.order(:kind, :created_at)
                              .select(:kind, :hostname, :pid, :metadata, :last_heartbeat_at)

      if processes.none?
        puts "No Pgbus processes running."
        return
      end

      puts "KIND         HOST                 PID      HEARTBEAT                      METADATA"
      puts "-" * 100
      processes.each do |p|
        puts format("%-12s %-20s %-8s %-30s %s",
                    p.kind, p.hostname, p.pid, p.last_heartbeat_at, p.metadata)
      end
    end

    def list_queues
      Pgbus.client.list_queues
      metrics = Pgbus.client.metrics

      puts "QUEUE                                    DEPTH      VISIBLE    OLDEST (s)      TOTAL          "
      puts "-" * 95

      Array(metrics).each do |m|
        puts format("%-40s %-10s %-10s %-15s %-15s",
                    m.queue_name, m.queue_length, m.queue_visible_length,
                    m.oldest_msg_age_sec || "-", m.total_messages)
      end
    end

    def print_help
      puts <<~HELP
        Usage: pgbus <command> [options]

        Commands:
          start    Start the Pgbus supervisor (workers + dispatcher)
          status   Show running Pgbus processes
          queues   List queues with metrics
          version  Show version
          help     Show this help

        Options for `start`:
          --queues STRING    Override worker capsules from the CLI
                             (e.g. "critical: 5; default: 10")
          --capsule NAME     Run only the named capsule from the configured
                             workers (useful for one-capsule-per-process
                             deployments)
      HELP
    end
  end
end
