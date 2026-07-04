# frozen_string_literal: true

# Operating the worker fleet: the CLI, split-role deployments, recycling, the
# circuit breaker, prefetch, and the async fiber mode.
class Views::Docs::Pages::RunningWorkers < DocsUI::Page
  title "Running workers"
  eyebrow "Operations"

  def lead = "Start the supervisor, split roles across containers, and keep workers from leaking with recycling."

  def content
    cli
    roles
    recycling
    circuit_breaker
    prefetch
    async
  end

  private

  def cli
    DocsUI::Section("The CLI") do
      DocsUI::Code(<<~SHELL, lexer: :shell)
        pgbus start     # supervisor: workers + dispatcher + scheduler + consumers
        pgbus status    # show running processes
        pgbus queues    # list queues with depth/metrics
        pgbus version   # print the version
      SHELL
    end
  end

  def roles
    DocsUI::Section("Split-role deployments", description: "One role per container.") do
      md <<~'MD'
        By default `pgbus start` boots every role in one supervisor. For
        containerized deployments where each role is its own process, use the
        role flags (mutually exclusive) and, optionally, a single capsule:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        pgbus start --workers-only              # only worker processes
        pgbus start --scheduler-only            # only the recurring-task scheduler
        pgbus start --dispatcher-only           # only the maintenance dispatcher
        pgbus start --workers-only --capsule critical  # one capsule per container
      SHELL
      DocsUI::Callout(:note) do
        plain "The auto-tuned "
        code { "pool_size" }
        plain " follows the role: a "
        code { "--scheduler-only" }
        plain " process opens only the connections it actually needs, not one per configured worker thread."
      end
    end
  end

  def recycling
    DocsUI::Section("Worker recycling", description: "The fix for the memory-bloat problem.") do
      md <<~'MD'
        pgbus workers retire themselves before they leak — the main reliability
        difference from backends that leave workers alive forever. When a limit is
        hit, the worker drains its thread pool, exits, and the supervisor forks a
        fresh process:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.max_jobs_per_worker = 10_000 # restart after 10k jobs
          config.max_memory_mb       = 512    # restart above 512 MB RSS
          config.max_worker_lifetime = 1.hour # restart after an hour
        end
      RUBY
      md <<~'MD'
        RSS is sampled from `/proc/self/statm` on Linux and `ps -o rss` on macOS.
      MD
    end
  end

  def circuit_breaker
    DocsUI::Section("Circuit breaker", description: "Auto-pause a failing queue.") do
      md <<~'MD'
        A queue that fails repeatedly is auto-paused with exponential backoff, so a
        broken dependency doesn't burn the whole fleet retrying. It auto-resumes
        after the backoff and resets; continued failures double the backoff:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure { |c| c.circuit_breaker_enabled = true } # default
      RUBY
      DocsUI::Callout(:note) do
        plain "Pause state lives in "
        code { "pgbus_queue_states" }
        plain " and survives restarts; you can also pause/resume manually from the dashboard. "
        plain "Add the table with "
        code { "rails generate pgbus:add_queue_states" }
        plain "."
      end
    end
  end

  def prefetch
    DocsUI::Section("Prefetch flow control") do
      md <<~'MD'
        Cap the number of in-flight (claimed but unfinished) messages per worker to
        keep a burst from overwhelming a slow downstream:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure { |c| c.prefetch_limit = 20 } # nil = unlimited (default)
      RUBY
    end
  end

  def async
    DocsUI::Section("Async execution mode (fibers)", description: "For I/O-bound work.") do
      md <<~'MD'
        Workers can run jobs as fibers instead of threads — ideal for I/O-bound
        workloads (HTTP calls, email, LLM APIs) where jobs spend their time waiting.
        Because fibers yield during I/O, many share a handful of connections:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.execution_mode = :async # all workers, or per-capsule:
          config.workers = [
            { queues: %w[webhooks emails], threads: 100, execution_mode: :async },
            { queues: %w[default], threads: 5 } # stays thread-based
          ]
        end
      RUBY
      DocsUI::Callout(:warning) do
        plain "Async needs "
        code { 'gem "async"' }
        plain " and "
        code { "config.active_support.isolation_level = :fiber" }
        plain ". Don't use it for CPU-bound jobs — they block the reactor. Messages stay "
        plain "protected by the visibility timeout regardless of mode."
      end
    end
  end
end
