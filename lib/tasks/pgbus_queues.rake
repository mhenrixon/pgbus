# frozen_string_literal: true

namespace :pgbus do
  namespace :queues do
    desc "Create FIFO indexes on all pgbus queues (required for grouped reads)"
    task fifo_indexes: :environment do
      puts "Creating FIFO indexes on all pgbus queues..."
      Pgbus.client.create_fifo_indexes_all
      puts "Done. FIFO indexes created on all queues."
    end

    desc "Create FIFO index on a specific queue (QUEUE=name)"
    task fifo_index: :environment do
      queue = ENV.fetch("QUEUE") do
        abort "Usage: rake pgbus:queues:fifo_index QUEUE=<queue_name>"
      end
      puts "Creating FIFO index on queue '#{queue}'..."
      Pgbus.client.create_fifo_index(queue)
      puts "Done."
    end
  end

  namespace :archives do
    desc "Convert a queue's archive table to pg_partman-managed partitions (QUEUE=name)"
    task partition: :environment do
      queue = ENV.fetch("QUEUE") do
        abort "Usage: rake pgbus:archives:partition QUEUE=<queue_name> " \
              "[INTERVAL=10000] [RETENTION=100000] [LEADING_PARTITION=10]"
      end
      interval = ENV.fetch("INTERVAL", "10000")
      retention = ENV.fetch("RETENTION", "100000")
      leading_raw = ENV.fetch("LEADING_PARTITION", "10")
      leading = begin
        Integer(leading_raw, 10)
      rescue ArgumentError, TypeError
        abort "LEADING_PARTITION must be a positive integer, got #{leading_raw.inspect}"
      end
      abort "LEADING_PARTITION must be a positive integer" if leading <= 0

      puts "Converting archive table for queue '#{queue}' to partitioned..."
      puts "  Partition interval: #{interval}"
      puts "  Retention interval: #{retention}"
      puts "  Leading partitions: #{leading}"

      Pgbus.client.convert_archive_partitioned(
        queue,
        partition_interval: interval,
        retention_interval: retention,
        leading_partition: leading
      )
      puts "Done. Archive table is now partitioned."
    end
  end
end
