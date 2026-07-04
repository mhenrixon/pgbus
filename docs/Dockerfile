# syntax = docker/dockerfile:1
ARG RUBY_VERSION=3.4.2
FROM ruby:$RUBY_VERSION-slim AS base

ARG BUN_VERSION=1.3.2
ENV BUN_INSTALL="/usr/local/bun"
ENV PATH="/usr/local/bun/bin:$PATH"
WORKDIR /rails
ENV BUNDLE_WITHOUT="development:test" RAILS_ENV="production"

RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y curl libjemalloc2 && \
    rm -rf /var/lib/apt/lists /var/cache/apt/archives
RUN gem update --system --no-document && gem install -N bundler

FROM base AS build
RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y build-essential git libyaml-dev pkg-config unzip
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v${BUN_VERSION}"
COPY Gemfile Gemfile.lock ./
RUN bundle install && rm -rf ~/.bundle/ "${BUNDLE_PATH}"/ruby/*/cache
COPY . .
RUN bun install --frozen-lockfile
# assets:precompile runs bun run build:css via the css:build rake enhance.
RUN SECRET_KEY_BASE_DUMMY=1 ./bin/rails assets:precompile

FROM base
# Kamal verifies this label on --skip-push deploy; must equal `service:`.
LABEL service="pgbus"
COPY --from=build "${BUNDLE_PATH}" "${BUNDLE_PATH}"
COPY --from=build /rails /rails
RUN groupadd --system --gid 1000 rails && \
    useradd rails --uid 1000 --gid 1000 --create-home --shell /bin/bash && \
    chown -R 1000:1000 /rails/log /rails/tmp
USER 1000:1000
EXPOSE 3000
CMD ["./bin/rails", "server", "-b", "0.0.0.0"]
