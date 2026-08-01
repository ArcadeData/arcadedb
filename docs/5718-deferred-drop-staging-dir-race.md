# #5718 - Flaky `ArcadeStateMachineDeferredDropTest.theDatabaseNameIsReusableAsSoonAsApplyReturns`

## Problem

The test failed intermittently on the `unit-tests` CI job, on PRs entirely unrelated to `ha-raft`
(seen on 2 of 3 runs of PR #5709, as the only failure out of 772 tests in the module):

```
java.lang.AssertionError:
[exactly one staging directory expected]
Expected size: 1 but was: 0 in:
[]
    at ArcadeStateMachineDeferredDropTest.findStagingDirectory(...:109)
    at ArcadeStateMachineDeferredDropTest.theDatabaseNameIsReusableAsSoonAsApplyReturns(...:158)
```

## Root cause

An ordering race in the test itself, not a defect in `ArcadeStateMachine` or `DeferredDatabaseDeleter`.

The test released the deferred deleter before it looked for the directory that deleter is about to
remove:

```java
gate.countDown();                     // unblocks the deferred deleter
awaitGone(findStagingDirectory());    // ...then lists the staging dirs
```

`gate.countDown()` lets the blocked executor task proceed, so the deleter can finish its recursive
delete before `findStagingDirectory()` runs `Files.list`. When the deleter wins, zero staging
directories remain and the `hasSize(1)` assertion inside `findStagingDirectory()` fails.

Both sibling tests, `applyDeregistersTheDatabaseWithoutDeletingItsFilesInline` and
`replayingTheEntryAfterTheRenameIsANoOp`, already resolve the staging path *before* `countDown()`
and are therefore unaffected.

## Fix

Resolve the staging directory while the deleter is still gated, matching the sibling tests:

```java
final Path staged = findStagingDirectory();

gate.countDown();
awaitGone(staged);
```

With the deleter blocked at the moment of the listing, exactly one staging directory is guaranteed
to exist, so the assertion no longer depends on scheduling.

Production code is unchanged: the diff is 3 insertions / 1 deletion, confined to
`ha-raft/src/test/java/com/arcadedb/server/ha/raft/ArcadeStateMachineDeferredDropTest.java`.

## Verification

The test passes locally as-is even when broken, so a plain green run proves nothing. The race was
made deterministic first, and the fix was then proven against that same deterministic schedule.

1. **Reproduce.** Inserting `Thread.sleep(500)` between `countDown()` and `findStagingDirectory()`
   fails on the pre-fix code with the same assertion CI reports:

   ```
   [ERROR] ArcadeStateMachineDeferredDropTest.theDatabaseNameIsReusableAsSoonAsApplyReturns:159
           ->findStagingDirectory:109
   [exactly one staging directory expected]
   Expected size: 1 but was: 0
   ```

   The caller line reads `:159` here against `:158` in the CI trace above because the injected
   `Thread.sleep(500)` shifts the call site down by one; `findStagingDirectory:109` is unchanged, as
   is the assertion. It is the same failure, not a different one.

2. **Prove the fix closes the race.** Applying the reordering while *keeping* the 500 ms delay in
   place makes the test pass. The scheduling that previously failed deterministically now succeeds,
   which is what shows the fix is the reordering and not luck.

3. **Confirm the shipped form.** With the artificial delay removed, the class was run 8 consecutive
   times: `PASS=8 FAIL=0` (4 tests per run).

4. **Sweep for the same anti-pattern.** No other test under `ha-raft/src/test` lists or resolves a
   deleted path after a `countDown()`; the two sibling tests already order it correctly.

5. **Regression check.** Full `ha-raft` unit suite (`mvn -o -pl ha-raft test`): 772 tests,
   `ArcadeStateMachineDeferredDropTest` green (4/4). 12 failures remain, all in `LeaveClusterTest`
   (2) and `WaitForApplyTest` (4), which are unrelated multi-node cluster tests.

   Those failures are **pre-existing on this machine, not caused by this change**. Verified with a
   control worktree checked out at the same base commit `61571d454` *without* the fix: it produces
   the identical 6 failures at the identical line numbers. They also fail when run in isolation, so
   it is not suite-level port contention introduced by this branch. The symptom
   (`lastAppliedIndex` stuck at `-1`) is a local cluster-formation problem, consistent with the
   known local ha-raft environment flakiness, and is out of scope for #5718.

## Impact

Test-only. No production behaviour changes. Removes one recurring source of red `unit-tests` runs
on unrelated PRs.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5741

### Review cycles

| Cycle | Head SHA | Change under review | Outcome |
|---|---|---|---|
| 1 | `73df98f7c` | The fix itself, plus this tracking doc. | LGTM, nothing blocking. One cosmetic nit: the doc quotes `:158` in one trace and `:159` in another, read as an off-by-one. |
| 2 | `63b24a9d7` | Explain the `:158` vs `:159` caller-line shift. | LGTM, nothing blocking. Remaining nit ("the doc is heavy for a 3-line fix") was self-resolved by the reviewer as house convention, so no action. |

On cycle 1 the reviewer's premise was checked rather than accepted: the two line numbers are both
correct and describe different file states. `:158` is the original CI trace against the unmodified
file; `:159` is the local repro, where the injected `Thread.sleep(500)` shifts the call site down by
one. Nothing was renumbered. What was wrong was the surrounding prose, which claimed the repro
reproduced "the exact CI assertion and line number" - so the wording was corrected and the shift
explained inline.

No deferred items.

**Final state:** clean-approval.

## Follow-ups

None for this issue. Note for triage: a red board on an unrelated PR is not automatically caused by
that PR. `ha-integration-tests` flakiness is tracked separately under #5702, and the
`HttpRedMetricsIT` counter assertion under #5630.
