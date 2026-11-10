# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- MIT `LICENSE`.
- GitHub Actions CI: build, `go vet`, race-enabled unit tests, `golangci-lint`,
  and a broker-backed integration test job.
- `golangci-lint` configuration (`.golangci.yml`).
- Community & governance docs: `CONTRIBUTING.md`, `SECURITY.md`,
  `CODE_OF_CONDUCT.md`, issue templates, and a pull request template.
- `ROADMAP.md` describing the path to a production-grade 1.0.

### Changed

- Rewrote `README.md` to match the actual public API.
- Split tests: unit tests run without a broker; the producer/consumer
  integration test now lives behind the `integration` build tag
  (`go test -tags integration ./kafka`).

### Fixed

- Restore environment variables after the configuration unit test.
- Log consumer shutdown errors instead of silently ignoring them.
- Format the Go sources to satisfy the new lint checks.
- Isolate integration runs with unique topics and groups, use IPv4 for local
  Docker brokers, and check producer flush completion with longer timeouts.

## [0.1.4] and earlier

Initial iterations of the SDK: producer, consumer, batch consumption, consumer
worker pool, and env-based configuration. See the Git history and tags for
details.

[Unreleased]: https://github.com/thettteus/go-kafka-sdk/compare/v0.1.4...HEAD
[0.1.4]: https://github.com/thettteus/go-kafka-sdk/releases/tag/v0.1.4
