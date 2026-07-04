# Releasing pgbus

pgbus is released **manually** by a maintainer. Publishing is driven by a
GitHub Release: creating a Release with a `vX.Y.Z` tag triggers
`.github/workflows/release.yml`, which tests, builds, and publishes the gem to
RubyGems via OIDC trusted publishing with a Sigstore attestation. Nothing is
published from a branch or a PR — a PR only *stages* the version bump and the
changelog roll.

## Pre-release checklist

Run these on the branch you intend to release, before cutting the tag:

- [ ] `bundle exec rake` is green (RuboCop + the full RSpec suite).
- [ ] `gem build pgbus.gemspec --strict` builds cleanly and the package
      contains no repo-only tooling. In particular, the custom RuboCop cop
      lives at repo-root `rubocop/` (outside every `spec.files` glob) and must
      **not** appear in the built `.gem` — verify with
      `tar tzf data.tar.gz | grep -i rubocop` returning nothing.
- [ ] `bundle exec rake pgbus:pgmq:status` — confirm the installed vs. vendored
      PGMQ schema version. If a new `lib/pgbus/pgmq_schema/pgmq_v*.sql` was
      vendored this cycle, the release notes should call it out and remind
      operators to run `rails generate pgbus:upgrade_pgmq` after upgrading.
- [ ] `docs/Gemfile.lock` pins `pgbus (X.Y.Z)` matching the new
      `lib/pgbus/version.rb`. The docs site's frozen `bundle install` in CI
      fails if the lock drifts from `version.rb`. Re-pin with
      `cd docs && bundle install` and commit the updated lock in the same PR as
      the version bump.

## Cutting a release

1. **Bump the version.** Edit `lib/pgbus/version.rb` to the new `X.Y.Z`
   (`Pgbus::VERSION`). This is the single source of truth — `release.yml`
   verifies the tag matches it exactly.

2. **Roll the changelog.** In `CHANGELOG.md`, rename the top `## [Unreleased]`
   section to `## [X.Y.Z] - YYYY-MM-DD` (today's date), and add a fresh empty
   `## [Unreleased]` section above it with the standard category stubs
   (`### Added`, `### Changed`, `### Fixed`, `### Security`). Keep the entries
   **user-facing only** — pure chore/test/docs-infra commits do not get a
   changelog bullet.

3. **Re-pin the docs lockfile.** `cd docs && bundle install` so
   `docs/Gemfile.lock` reflects the new `pgbus (X.Y.Z)`.

4. **Commit** the bump + roll + docs lock together on a branch, open a PR, and
   merge it once green. (Steps 1–4 are exactly what a "release hygiene" PR
   stages; the tag is cut afterward.)

5. **Tag and push.** On the merged commit:

   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

   The tag **must** be `vX.Y.Z` (a leading `v`); `release.yml` strips the `v`
   and compares the remainder to `Pgbus::VERSION`.

6. **Create the GitHub Release.** Publish a Release pointing at the `vX.Y.Z`
   tag (`gh release create vX.Y.Z --title vX.Y.Z --notes-file <notes>` or via
   the web UI). Use the rolled `CHANGELOG.md` section as the release notes.
   Publishing the Release is what triggers the pipeline.

## What `release.yml` does after you publish

Triggered by `release: [published]`, the workflow runs these jobs in order:

1. **`test`** — runs `bundle exec rake` on Ruby 3.3 and 3.4 (`fail-fast`), so a
   broken build blocks the publish.

2. **`build`** (needs `test`) —
   - **Tag/version consistency check:** reads `Pgbus::VERSION`, strips the `v`
     from the release tag, and fails the job if they differ.
   - Builds the gem with `gem build pgbus.gemspec --strict`.
   - **Gem-contents guard:** unpacks the gem and fails if any `.git*` file,
     `*.gemspec`, or a `spec`/`test` directory leaked into the package.
   - Generates SHA256/SHA512 checksums and uploads the gem + checksums as the
     `gem` artifact.

3. **`publish-rubygems`** (needs `build`, environment `rubygems`,
   `id-token: write`) —
   - Verifies the checksums.
   - Configures RubyGems **trusted publishing** credentials via OIDC
     (`rubygems/configure-rubygems-credentials`) — no long-lived API token.
   - Signs the gem with `sigstore-cli` and pushes with
     `gem push --attestation` (Sigstore attestation), then uploads the
     `.sigstore.json` bundle as the `sigstore` artifact.

4. **`upload-release-assets`** (needs `build` + `publish-rubygems`,
   `contents: write`) — attaches the `.gem`, both checksum files, and the
   Sigstore bundle to the GitHub Release.

If any job fails, the gem is not published; fix and re-run, or delete and
re-create the Release once fixed.

## Post-release notes

- **RubyGems trusted publishing** must be configured for `pgbus` on
  rubygems.org (the `rubygems` GitHub Environment is the trusted publisher).
  There is no `RUBYGEMS_API_KEY` secret — OIDC replaces it.
- **`SLACK_WEBHOOK_URL`** — the dependency-watch workflow (added by #287)
  posts to Slack when a watched upstream dependency (e.g. pgmq-ruby, PGMQ)
  releases a new version. Configure the `SLACK_WEBHOOK_URL` repository secret
  so those notifications reach the team; without it the workflow degrades to a
  no-op notification step.
