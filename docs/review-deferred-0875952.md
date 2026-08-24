# Review notes - PR #6658, cycle 2 (head 0875952)

Second `claude` bot review on this PR (posted 2026-08-23T22:00:50Z, as a PR issue comment - no commit SHA,
so it wasn't picked up by the original SHA-only polling and is being processed now on schedule fix).

## Applied

- **Test coverage gap**: the review noted no test directly exercised `Describe('P')` followed by an `Execute`
  with a limit smaller than the full result - the "more consequential" of the two defects #6458 describes
  (`describeCommand()` previously discarding the client's row-limit entirely). Added
  `describePortalThenExecuteWithASmallLimitReturnsOnlyThatManyRowsThenSuspends` to
  `Issue6458PortalSuspensionIT`, mirroring the raw-wire-protocol precision of the existing
  no-Describe test but sending `Describe('P')` first. Verified: 5/5 tests pass in the class
  (`mvn -pl postgresw verify -DskipITs=false -Dit.test=Issue6458PortalSuspensionIT`).

## Skipped (with rationale)

- **Performance consideration - eager full materialization of `fullResultSet`**: the review flags that a
  client which sends a single `Execute` with a small max-row count and never sends `Describe('P')` still
  gets the entire result set materialized server-side, and asks whether that needs a follow-up "before this
  sees production traffic with big scans." This is already the known, already-tracked trade-off from cycle 1
  - documented in `PostgresPortal.fullResultSet`'s Javadoc and filed as #6659 (open). The reviewer's own
  wording ("looks like a deliberate, tracked trade-off rather than an oversight") confirms no new action is
  needed here; #6659 is where the streaming redesign belongs, not this PR.
- **Pre-existing issue made more visible - stale portal state on rebind (#6660)**: the review reconfirms the
  `bindCommand()` `KNOWN ISSUE (#6660)` comment is accurate and notes the new pagination state makes the
  failure mode worse. Already tracked as #6660 (open) from cycle 1, and the review explicitly says "just
  confirming that's understood and not conflated with this fix being 'done'" - no code change requested.
- **Minor - per-batch column-count-mismatch DEBUG logging**: the review notes that comparing `portal.columns`
  (from the full result) against `dataRowColumns` (from just the current slice) could now log spurious
  `DEBUG`-level "column count mismatch" warnings on later batches for sparse documents, where it wouldn't
  have before this fix (when `cachedResultSet` held the full result in one shot). Verified in
  `PostgresNetworkExecutor.executeCommand()` (~line 560): the log line is gated by `if (DEBUG)` and is purely
  informational - `columnsToUse` still falls back to `portal.columns` for actual serialization regardless, so
  there is no correctness or behavioral effect, only a debug-log false positive when `DEBUG` is manually
  enabled against sparse-document workloads. The reviewer's own assessment ("Cosmetic only") agrees. Left
  as-is; not worth a special-case in a debug-only diagnostic for a condition its own fallback already handles
  correctly.
