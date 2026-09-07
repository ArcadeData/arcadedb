# Review notes for PR #7254 (issue #7209), head 5a9e9a8f

Cycle 1, reviewer `claude`. No formal review and no inline comments; one PR issue comment carrying
three non-blocking nits. Nothing was left unclear enough to defer to the developer; two items were
applied and one was consciously skipped, with the reason below.

## Applied

1. **FQN instead of an import.** `Issue7209SnapshotMarkerPruningTest` used
   `final java.lang.reflect.Field f = ...` while importing `Method` and `Proxy` from the same package,
   against the project convention in `CLAUDE.md`. Now `import java.lang.reflect.Field;` and `Field f`.
2. **Unsynchronised concurrent registration.** The reviewer's read is correct: with the old
   `cleanupOldSnapshots` being a no-op the race was latent, and now that pruning really deletes, a
   low-index registration whose file lands after a concurrent high-index prune leaves one marker
   behind until a later, higher-`keepIndex` prune sweeps it. The reviewer asked for the reasoning to
   be stated near `registerSnapshotMarker` rather than for a lock, and that is what was added: the
   orphan is self-healing and costs a zero-byte file, whereas a lock would add serialisation to the
   snapshot path; what the prune must never do - delete a marker that is still the latest - is
   guaranteed by the strictly-below comparison, with no lock.

## Skipped, with the reason

3. **`docs/7209-raft-snapshot-marker-pruning.md` is a large permanent addition to `docs/`.** Skipped
   here, not because the point is wrong - the developer has objected to tracking docs landing in PRs
   before, and the reviewer is right that it duplicates much of the PR body - but because this file is
   a required output of the `resolve-issue-with-review` workflow: Phase 6 of that skill stages, commits
   and pushes exactly this path, and the completeness evidence it carries (the greps, the coverage
   table, the residual-risk argument) is what the workflow's gate is checked against. Deleting it from
   inside the workflow that mandates it is not a call to make unilaterally.

   **Flagged for the developer:** if tracking docs are no longer wanted in this repo, drop
   `docs/7209-raft-snapshot-marker-pruning.md` (and this notes file) in one commit before merging -
   nothing in the fix or the tests references either.

4. **"Not verified in this environment."** No action needed: the reviewer could not run Maven in its
   sandbox. The lane was run here - `mvn -o test -pl ha-raft -DexcludedGroups=benchmark,vector,slow`
   -> `Tests run: 1213, Failures: 0, Errors: 0, Skipped: 0`, `BUILD SUCCESS` - and CI runs it again.
