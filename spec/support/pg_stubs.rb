# frozen_string_literal: true

# Helpers for stubbing the `pg` gem in unit specs that mock PGMQ and therefore
# never load libpq. `PG.library_version` gates pgbus's libpq-native read bounds
# (see Pgbus::Client#libpq_read_bounds_effective? / #libpq_supports_socket_bounds?),
# so specs exercising those paths need it stubbed without a real pg dependency.
module PgStubs
  # Defines PG if absent and stubs PG.library_version. Defaults to 180_000
  # (libpq 18, i.e. >= 12 so socket bounds are supported).
  def stub_pg_library_version(version = 180_000)
    stub_const("PG", Module.new) unless defined?(PG)
    allow(PG).to receive(:library_version).and_return(version)
  end
end

RSpec.configure do |config|
  config.include PgStubs
end
