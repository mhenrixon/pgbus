## [Unreleased]

### Breaking Changes

- **Queue names must be alphanumeric and underscores only.** Queue names containing dashes (e.g., `my-app-queue`) will now raise `ArgumentError`. Rename to underscored form (e.g., `my_app_queue`) before upgrading. This restriction prevents SQL injection via PGMQ queue identifiers, which are interpolated into table names and cannot be parameterized.

### Security

- Add `bundler-audit` to CI for dependency vulnerability scanning
- Add `QueueNameValidator` to enforce strict queue name validation (alphanumeric + underscores, 61 char max)
- Add `config.allowed_global_id_models` to restrict which models can be deserialized from event payloads
- Add security headers to dashboard (X-Frame-Options, X-Content-Type-Options, Referrer-Policy, Permissions-Policy)
- Warn when dashboard `web_auth` is unconfigured
- Add `globalid` as an explicit runtime dependency (was used but only transitively available via activejob)

## [0.1.0] - 2026-03-30

- Initial release
