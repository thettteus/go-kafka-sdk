# Contributing to go-kafka-sdk

Thanks for your interest in improving `go-kafka-sdk`! This document explains how
to set up your environment, run the checks, and submit changes.

## Prerequisites

- **Go 1.24.5+** (see `go.mod`).
- **golangci-lint v1.64.8**, matching the version pinned in CI.
- **CGO + librdkafka** — the SDK binds to `librdkafka` via `confluent-kafka-go`.
  On most platforms the bundled static build works. If your environment needs
  dynamic linking:

  ```sh
  # macOS (Homebrew)
  brew install librdkafka pkg-config

  # Linux (Debian/Ubuntu)
  sudo apt-get install -y librdkafka-dev pkg-config
  ```

- **Docker** (optional) — to run a local Kafka broker for integration tests.

## Development workflow

1. Fork and clone the repository, then create a branch off `main`.
2. Make your change. Keep commits focused and messages descriptive.
3. Run the checks below locally.
4. Open a pull request against `main` and fill in the PR template.

## Running checks locally

Use Go 1.24.5 for these checks to match CI and the pinned linter. If your
default Go installation is newer, `GOTOOLCHAIN` selects the matching toolchain.

```sh
export GOTOOLCHAIN=go1.24.5

# Format & vet
gofmt -l .
go vet ./...

# Lint (bundles staticcheck)
golangci-lint run

# Unit tests — no broker required
go test -race ./...
```

### Integration tests

Integration tests are guarded by the `integration` build tag and require a
Kafka broker on `localhost:9092`. Start one with Docker Compose:

```sh
docker compose up -d
go test -tags integration -race -v ./kafka
```

## Coding guidelines

- Keep the public API small and idiomatic; prefer composition over configuration.
- Always handle errors explicitly — no silent `_ =` unless justified with a comment.
- Add or update godoc comments for any exported symbol you touch.
- New behavior needs test coverage (unit where possible, integration when a broker is genuinely required).
- Run `gofmt`/`goimports` before committing.

## Reporting bugs and requesting features

Use the issue templates under **Issues → New issue**. For security issues, do
**not** open a public issue — see [SECURITY.md](SECURITY.md).
