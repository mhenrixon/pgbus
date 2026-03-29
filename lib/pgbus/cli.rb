# frozen_string_literal: true

module Pgbus
  module CLI
    module_function

    def start(args)
      command = args.first || "help"

      case command
      when "start"
        start_supervisor
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

    def start_supervisor
      Pgbus.logger.info { "[Pgbus] Starting Pgbus #{Pgbus::VERSION}..." }

      supervisor = Process::Supervisor.new
      supervisor.run
    end

    def show_status
      if defined?(ActiveRecord::Base)
        processes = ActiveRecord::Base.connection.execute(
          "SELECT kind, hostname, pid, metadata, last_heartbeat_at FROM pgbus_processes ORDER BY kind, created_at"
        )

        if processes.none?
          puts "No Pgbus processes running."
          return
        end

        puts "KIND         HOST                 PID      HEARTBEAT                      METADATA"
        puts "-" * 100
        processes.each do |p|
          puts format("%-12s %-20s %-8s %-30s %s",
                      p["kind"], p["hostname"], p["pid"], p["last_heartbeat_at"], p["metadata"])
        end
      else
        puts "ActiveRecord not available. Run from a Rails context."
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
        Usage: pgbus <command>

        Commands:
          start    Start the Pgbus supervisor (workers + dispatcher)
          status   Show running Pgbus processes
          queues   List queues with metrics
          version  Show version
          help     Show this help
      HELP
    end
  end
end
