# frozen_string_literal: true

namespace :pgbus do
  namespace :streams do
    desc "Fail if any pgbus controller or web component includes ActionController::Live"
    task :lint_no_live do
      # ActionController::Live has well-documented interactions with Puma
      # that tie up worker threads for the lifetime of a streaming response
      # (puma/puma#1009, puma/puma#938, puma/puma#569, rails/rails#10948).
      # The pgbus streams subsystem is built on rack.hijack specifically to
      # avoid this class of bug — hijack releases the worker thread as long
      # as the Rack code returns promptly after hijacking. Any accidental
      # reintroduction of ActionController::Live into the pgbus web layer
      # would regress the architecture invisibly, so we guard against it
      # in CI.
      roots = %w[
        app/controllers/pgbus
        lib/pgbus/web
      ].map { |r| File.expand_path("../../#{r}", __dir__) }.select { |p| File.directory?(p) }

      offenders = []
      roots.each do |root|
        Dir.glob("#{root}/**/*.rb").each do |path|
          content = File.read(path)
          offenders << path if content.match?(/^\s*include\s+ActionController::Live\b/)
        end
      end

      if offenders.any?
        warn "\e[31m✗ ActionController::Live found in pgbus web code:\e[0m"
        offenders.each { |p| warn "    #{p}" }
        warn ""
        warn "  Rationale: the pgbus streams subsystem uses rack.hijack to"
        warn "  avoid Puma thread pinning (puma/puma#1009). Including"
        warn "  ActionController::Live reintroduces the bug it was designed"
        warn "  to avoid. Use Pgbus::Web::StreamApp for SSE endpoints."
        abort
      end

      puts "\e[32m✓ pgbus web code is free of ActionController::Live\e[0m"
    end
  end
end
