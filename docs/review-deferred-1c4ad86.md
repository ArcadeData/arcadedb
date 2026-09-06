# Review items skipped on 1c4ad86 (PR #7208, cycle 1)

The `claude` review on `1c4ad86adfb7d1112586f71b64e711c722cd01f9` raised three non-blocking
observations. Two were applied; this records the one that was not, and why.

## Skipped: reflection on a private method in the test

> **Reflection on a private method in the test** (`registerSnapshotMarker`) is a bit of a white-box
> smell, but justified here since it's the only way to drive the real warning-emitting path without
> duplicating Ratis's snapshot bookkeeping - reasonable tradeoff for a regression test this targeted.

Skipped deliberately, and the reviewer reaches the same conclusion in the same sentence.

The warning under test is emitted from inside `SimpleStateMachineStorage.cleanupOldSnapshots()`, which
`ArcadeStateMachine` reaches only from `registerSnapshotMarker`. The alternatives are worse:

- widening `registerSnapshotMarker` to package-private purely for the test enlarges the production API
  surface to serve a log assertion
- reaching `cleanupOldSnapshots` through the public `takeSnapshot()` drags in the applied-index,
  trusted-index-clamp and phase-2 preconditions, so the test would fail for reasons unrelated to
  logging
- calling `storage.cleanupOldSnapshots(...)` directly stops testing ArcadeDB's own path and starts
  testing Ratis

`Issue6111StaleSnapshotReadFloorTest` already drives the same private method the same way, so this is
the module's existing convention rather than a new one.

## Applied in cycle 1

- the SLF4J-to-JUL binding assumption is now stated in the filter's class javadoc, together with the
  Ratis message-text assumption, and both are noted as failing *open* (the warning returns) rather
  than silently suppressing something else
- the marker-accumulation follow-up (`cleanupOldSnapshots` never prunes without an `.md5` companion)
  is filed as #7209 instead of living only in the PR body
