# Changelog

All notable changes are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows the policy in [VERSIONING.md](VERSIONING.md).

---

## [Unreleased]

## [v0.3.0] — 2026-06-05

### Added

- `NewSubscriptionWithRowsForTesting` in the `corrosion` package: constructs a
  `*Subscription` that serves an initial row snapshot via `Rows()` and then delivers
  change events via `Changes()`, without requiring a real Corrosion server. This fills
  the gap left by `NewSubscriptionForTesting`, which only supports the `Changes()` path
  and returns nil from `Rows()`.

- `corrosiontest.NewSubscriptionWithRows`: public wrapper in the `corrosiontest` package.
  Prefer this over the internal `NewSubscriptionWithRowsForTesting` in test code.

- `corrosiontest/subscription_test.go`: unit tests covering the new function — initial
  row load, empty snapshot, change delivery after rows, and column-count mismatch
  detection at scan time.

### Notes

`NewSubscription` (changes-only, nil rows) is unchanged and remains the right choice
when the code under test never calls `Rows()`. Use `NewSubscriptionWithRows` when the
code loads initial state via `Rows()` before switching to `Changes()`.

---

## [v0.2.0] — 2026-04-29

### Added

- Tracing support via `go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp`.
- `Watch` loop with automatic reconnect and backoff.
- `Health` endpoint client.

---

## [v0.1.0] — 2026-04-19

Initial release. Core subscription client, query execution, and `corrosiontest` Docker
harness.
