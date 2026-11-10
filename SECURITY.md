# Security Policy

## Supported versions

`go-kafka-sdk` is pre-1.0. Security fixes are applied to the latest released
`v0.x` tag. Pin a specific version and upgrade promptly when fixes are released.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, use one of the following private channels:

- Open a [GitHub Security Advisory](https://github.com/thettteus/go-kafka-sdk/security/advisories/new)
  (preferred), or
- Contact the maintainer privately.

Please include:

- A description of the vulnerability and its impact.
- Steps to reproduce (a minimal proof of concept helps).
- Affected version(s) and environment details.

You can expect an initial acknowledgement within a few business days. Once the
issue is confirmed, we will work on a fix, coordinate a release, and credit you
in the advisory unless you prefer to remain anonymous.

## Scope

This project is a thin wrapper over
[`confluent-kafka-go`](https://github.com/confluentinc/confluent-kafka-go) and
`librdkafka`. Vulnerabilities in those upstream dependencies should be reported
to their respective projects; if a vulnerability is exposed or amplified by this
SDK's API, please report it here as well.
