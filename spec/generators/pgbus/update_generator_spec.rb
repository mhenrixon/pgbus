# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"
require "rails/generators"
require "generators/pgbus/update_generator"

RSpec.describe Pgbus::Generators::UpdateGenerator do
  describe "class" do
    it "is a Rails::Generators::Base subclass" do
      expect(described_class.ancestors).to include(Rails::Generators::Base)
    end

    it "has a description" do
      expect(described_class.desc).to include("config/pgbus.yml")
      expect(described_class.desc).to include("config/initializers/pgbus.rb")
    end

    it "exposes a --source option defaulting to config/pgbus.yml" do
      option = described_class.class_options[:source]
      expect(option).not_to be_nil
      expect(option.default).to eq("config/pgbus.yml")
    end

    it "exposes a --destination option defaulting to config/initializers/pgbus.rb" do
      option = described_class.class_options[:destination]
      expect(option).not_to be_nil
      expect(option.default).to eq("config/initializers/pgbus.rb")
    end
  end

  describe "end-to-end conversion" do
    let(:tmpdir) { Dir.mktmpdir }
    let(:source_path) { File.join(tmpdir, "config/pgbus.yml") }
    let(:dest_path) { File.join(tmpdir, "config/initializers/pgbus.rb") }

    before do
      FileUtils.mkdir_p(File.dirname(source_path))
      File.write(source_path, <<~YAML)
        production:
          visibility_timeout: 600
          workers:
            - queues: ["*"]
              threads: 5
      YAML
    end

    after { FileUtils.rm_rf(tmpdir) }

    it "writes the converted Ruby source to config/initializers/pgbus.rb" do
      generator = described_class.new([], destination: dest_path, source: source_path)
      generator.destination_root = tmpdir
      capture_io { generator.invoke_all }

      expect(File.exist?(dest_path)).to be(true)
      content = File.read(dest_path)
      expect(content).to include("Pgbus.configure do |c|")
      expect(content).to include("c.visibility_timeout = 10.minutes")
      expect(content).to include('c.workers = "*: 5"')
    end
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
