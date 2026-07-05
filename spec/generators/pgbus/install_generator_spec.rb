# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"
require "stringio"
require "rails/generators"
require "generators/pgbus/install_generator"

RSpec.describe Pgbus::Generators::InstallGenerator do
  describe "migration template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/migration.rb.erb", __dir__)
    end

    it "exists" do
      expect(File.exist?(template_path)).to be true
    end

    it "uses enable_extension only conditionally, not as the sole install path" do
      content = File.read(template_path)
      expect(content).to include("Pgbus::PgmqSchema.install_sql")
      expect(content).to include("extension_available?")
    end

    it "references PgmqSchema for installation" do
      content = File.read(template_path)
      expect(content).to include("Pgbus::PgmqSchema")
    end

    it "includes pgmq_schema_mode detection" do
      content = File.read(template_path)
      expect(content).to include("pgmq_schema_mode")
    end
  end

  describe "initializer template" do
    let(:template_path) do
      File.expand_path("../../../lib/generators/pgbus/templates/initializer.rb.erb", __dir__)
    end

    it "exists" do
      expect(File.exist?(template_path)).to be true
    end

    it "wraps configuration in a Pgbus.configure block" do
      content = File.read(template_path)
      expect(content).to include("Pgbus.configure do |c|")
    end

    it "bakes in the #311 realtime broadcast-queue default" do
      content = File.read(template_path)
      expect(content).to include('c.streams_broadcast_queue = "realtime"')
    end

    it "backs the realtime queue with a dedicated capsule" do
      content = File.read(template_path)
      expect(content).to include("realtime")
      expect(content).to match(/c\.capsule\s+:realtime/)
    end
  end

  describe "end-to-end install" do
    let(:tmpdir) { Dir.mktmpdir }
    let(:initializer_path) { File.join(tmpdir, "config", "initializers", "pgbus.rb") }
    let(:legacy_yml_path) { File.join(tmpdir, "config", "pgbus.yml") }

    before do
      FileUtils.mkdir_p(File.join(tmpdir, "config"))
      generator = described_class.new([], {}, destination_root: tmpdir)
      generator.destination_root = tmpdir
      capture_io { generator.create_config_file }
    end

    after { FileUtils.rm_rf(tmpdir) }

    it "creates config/initializers/pgbus.rb" do
      expect(File.exist?(initializer_path)).to be true
    end

    it "does not create the legacy config/pgbus.yml" do
      expect(File.exist?(legacy_yml_path)).to be false
    end

    it "generates Ruby that loads via Pgbus.configure and passes validate!" do
      expect { eval_initializer(initializer_path) }.not_to raise_error
    end

    it "produces a realtime capsule that drains the realtime broadcast queue" do
      config = eval_initializer(initializer_path)
      broadcast_queue = config.streams_broadcast_queue
      drains = Array(config.workers).any? do |w|
        queues = w[:queues] || w["queues"] || []
        queues.include?("*") || queues.include?(broadcast_queue)
      end
      expect(broadcast_queue).to eq("realtime")
      expect(drains).to be true
    end
  end

  describe "post-install messaging" do
    let(:tmpdir) { Dir.mktmpdir }

    after { FileUtils.rm_rf(tmpdir) }

    it "points users at the initializer, not the legacy yml" do
      generator = described_class.new([], {}, destination_root: tmpdir)
      generator.destination_root = tmpdir
      out, = capture_io { generator.display_post_install }
      expect(out).to include("config/initializers/pgbus.rb")
      expect(out).not_to include("config/pgbus.yml")
    end
  end

  # Load a generated initializer through the real Pgbus.configure path — which
  # runs validate! — then return the resulting configuration and reset the
  # global so specs stay isolated. This exercises the exact boot behavior a
  # Rails app gets loading config/initializers/pgbus.rb.
  def eval_initializer(path)
    Pgbus.reset!
    eval(File.read(path), TOPLEVEL_BINDING, path) # rubocop:disable Security/Eval
    Pgbus.configuration
  ensure
    Pgbus.reset!
  end

  def capture_io
    original_stdout = $stdout
    original_stderr = $stderr
    $stdout = StringIO.new
    $stderr = StringIO.new
    yield
    [$stdout.string, $stderr.string]
  ensure
    $stdout = original_stdout
    $stderr = original_stderr
  end
end
