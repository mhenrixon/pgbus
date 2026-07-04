# frozen_string_literal: true

require "tmpdir"
require "fileutils"
require "stringio"

# Shared helpers for exercising Pgbus migration generators end-to-end.
#
# Content-only assertions (reading the .erb) prove a template exists but never
# run the generator body — create_migration_file, migration_version, and
# display_post_install stay uncovered. These helpers invoke the generator
# against a throwaway destination so those methods actually execute and the
# generated migration can be asserted against.
module GeneratorInvocation
  # Runs the generator in a temporary destination root and yields the path of
  # the migration it wrote (matched by the destination basename, minus the
  # timestamp prefix Rails adds), its content, and the destination root.
  # Silences the generator's stdout chatter.
  def generate_migration(generator_class, basename:, options: {}, migrate_dir: "db/migrate")
    Dir.mktmpdir do |tmpdir|
      FileUtils.mkdir_p(File.join(tmpdir, migrate_dir))

      generator = generator_class.new([], options)
      generator.destination_root = tmpdir
      # Separate-database routing delegates to Rails' db_migrate_path, which
      # needs a real database.yml. Stub it so --database specs don't require one.
      allow(generator).to receive(:db_migrate_path).and_return(migrate_dir) if options[:database].present?

      silence_generator { generator.invoke_all }

      path = Dir[File.join(tmpdir, migrate_dir, "*#{basename}")].first
      yield path, path && File.read(path), tmpdir
    end
  end

  def silence_generator
    original = $stdout
    $stdout = StringIO.new
    yield
  ensure
    $stdout = original
  end
end

RSpec.configure do |config|
  config.include GeneratorInvocation, type: :generator
  # Auto-tag specs living under spec/generators/ so the helper is available
  # without every file repeating `type: :generator`.
  config.define_derived_metadata(file_path: %r{/spec/generators/}) do |metadata|
    metadata[:type] ||= :generator
  end
end
