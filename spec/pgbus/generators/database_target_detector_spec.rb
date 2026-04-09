# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"
require "pgbus/generators/database_target_detector"

RSpec.describe Pgbus::Generators::DatabaseTargetDetector do
  # @tmpdir is set inside an around block so Dir.mktmpdir's block
  # semantics (auto-cleanup) work. A `let` can't bridge the block
  # boundary, so the instance variable is the idiomatic shape here.
  subject(:detector) { described_class.new(destination_root: @tmpdir) } # rubocop:disable RSpec/InstanceVariable

  around do |example|
    Dir.mktmpdir do |dir|
      @tmpdir = dir
      FileUtils.mkdir_p(File.join(dir, "config", "initializers"))
      example.run
    end
  end

  # The runtime path takes priority over file scanning. Tests that
  # exercise the static scanners need to neutralise it.
  before do
    allow(Pgbus.configuration).to receive(:connects_to).and_return(nil)
  end

  def write_file(relative, content)
    path = File.join(@tmpdir, relative) # rubocop:disable RSpec/InstanceVariable
    FileUtils.mkdir_p(File.dirname(path))
    File.write(path, content)
  end

  describe "#detect" do
    context "with runtime configuration taking priority" do
      it "returns the writing database name from Pgbus.configuration.connects_to" do
        allow(Pgbus.configuration).to receive(:connects_to)
          .and_return(database: { writing: :pgbus })

        expect(detector.detect).to eq("pgbus")
      end

      it "handles string-keyed hashes" do
        allow(Pgbus.configuration).to receive(:connects_to)
          .and_return("database" => { "writing" => "queue_db" })

        expect(detector.detect).to eq("queue_db")
      end

      it "returns nil when connects_to is not a hash" do
        allow(Pgbus.configuration).to receive(:connects_to).and_return(:unexpected)

        expect(detector.detect).to be_nil
      end

      it "returns nil when connects_to is nil (runtime falls through to file scan)" do
        allow(Pgbus.configuration).to receive(:connects_to).and_return(nil)

        expect(detector.detect).to be_nil
      end

      it "does not raise if accessing the configuration fails" do
        allow(Pgbus.configuration).to receive(:connects_to).and_raise(StandardError, "boom")

        expect(detector.detect).to be_nil
      end
    end

    context "when scanning config/initializers/pgbus.rb" do
      it "extracts the database name from a symbol-keyed connects_to" do
        write_file("config/initializers/pgbus.rb", <<~RUBY)
          Pgbus.configure do |c|
            c.connects_to = { database: { writing: :pgbus } }
          end
        RUBY

        expect(detector.detect).to eq("pgbus")
      end

      it "extracts from a parenthesized call form" do
        write_file("config/initializers/pgbus.rb", <<~RUBY)
          Pgbus.configure do |c|
            c.connects_to(database: { writing: :queue_db })
          end
        RUBY

        expect(detector.detect).to eq("queue_db")
      end

      it "returns nil when the initializer exists but has no connects_to" do
        write_file("config/initializers/pgbus.rb", <<~RUBY)
          Pgbus.configure do |c|
            c.database_url = ENV["DATABASE_URL"]
          end
        RUBY

        expect(detector.detect).to be_nil
      end

      it "returns nil when the initializer is absent" do
        expect(detector.detect).to be_nil
      end
    end

    context "when scanning config/application.rb" do
      it "extracts the database name from connects_to in application.rb" do
        write_file("config/application.rb", <<~RUBY)
          module MyApp
            class Application < Rails::Application
              config.after_initialize do
                Pgbus.configuration.connects_to = { database: { writing: :pgbus } }
              end
            end
          end
        RUBY

        expect(detector.detect).to eq("pgbus")
      end
    end

    context "with conflicting sources" do
      it "prefers the runtime configuration over the initializer scan" do
        allow(Pgbus.configuration).to receive(:connects_to)
          .and_return(database: { writing: :runtime_db })
        write_file("config/initializers/pgbus.rb", <<~RUBY)
          Pgbus.configure do |c|
            c.connects_to = { database: { writing: :file_db } }
          end
        RUBY

        expect(detector.detect).to eq("runtime_db")
      end

      it "prefers the initializer over application.rb" do
        write_file("config/initializers/pgbus.rb", <<~RUBY)
          Pgbus.configure { |c| c.connects_to = { database: { writing: :init_db } } }
        RUBY
        write_file("config/application.rb", <<~RUBY)
          Pgbus.configuration.connects_to = { database: { writing: :app_db } }
        RUBY

        expect(detector.detect).to eq("init_db")
      end
    end
  end
end
