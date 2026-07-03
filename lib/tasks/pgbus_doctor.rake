# frozen_string_literal: true

namespace :pgbus do
  desc "Run environment diagnostics (config, DB, PGMQ, queues, LISTEN/NOTIFY, process liveness)"
  task doctor: :environment do
    require "pgbus/doctor"

    doctor = Pgbus::Doctor.new
    puts doctor.report
    exit 1 unless doctor.success?
  end
end
