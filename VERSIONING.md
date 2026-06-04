# Versioning Policy

This module follows [Semantic Versioning](https://semver.org/) under the pre-1.0
convention: breaking changes may land in minor releases. The API is not yet stable.

**PATCH** (`v0.x.Y`): bug fixes only. No new exports, no changed signatures.

**MINOR** (`v0.X.0`): new features and, until v1.0, potentially breaking changes.
Read the changelog before upgrading. Breaking changes are called out under a
`### Breaking` heading.

**MAJOR**: reserved for v1.0, which signals API stability.

## Path to v1.0

No firm timeline. Rough criteria: subscription and query APIs stable across two minor
releases with no breaking changes, at least two production consumers, and `corrosiontest`
running in CI without flakes.
