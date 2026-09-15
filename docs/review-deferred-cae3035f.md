# Review notes - PR #7636, head `cae3035f`

The `claude` review of `cae3035f` found no functional bug and nothing blocking. What follows is every item
it raised that was NOT applied, with the reason, plus the two it asked a human to sign off on.

## Applied in the follow-up commit (recorded here for completeness)

- **"Symlinked configuration files are now shipped instead of silently dropped ... with no dedicated regression
  test."** Applied: `theWindowPathShipsASymlinkedSchemaThatTheFallbackStillRefuses` now drives both branches
  through a real symlink. It pays for itself immediately - the first version of that test failed, because a
  symlink target is resolved relative to the LINK's directory and the fixture's relative target made a broken
  link, which the window correctly reported as an absent file.
- **`import java.util.function.BiConsumer` out of alphabetical order.** Applied. The pre-existing slip the
  reviewer noticed in the same block (`java.util.Locale` before `java.nio.file.Files`) was left alone as
  unrelated churn.

## Skipped, with rationale

- **"The `captureConfigurationFiles` t0-barrier gap is argued, not covered by a test that exercises the gap
  itself."** Agreed as a description, skipped as a task. The gap is the interval between the file-set monitor's
  release and `LocalSchema`'s save inside one DDL; it is microseconds wide and has no synchronisation point a
  test could hold it open at without instrumenting the engine's DDL path. A test that tried would be a race
  that passes for the wrong reason - `Issue6114LockFreeBackupIT` says exactly this about the same gap in its
  own javadoc, and that is the precedent being followed. It is written down in the residual-risk section
  instead, which is where an untestable exposure belongs.
- **"A closing database now logs a spurious WARNING ... worth confirming no log-scraping alerting depends on
  that message's absence."** Skipped: there is nothing in this repository to change. The message already
  exists and is already emitted on this path by the full backup; what changed is that a closing database can
  now reach it. Whether an operator's alerting scrapes it is a deployment question - flagged below rather than
  answered by a code change.
- **"`BiConsumer<PageSnapshot, TimeSeriesCompactionPause>` ... took a re-read to confirm `pause` isn't
  silently dropped."** The reviewer's own text says "no action needed". The javadoc on
  `streamThroughPointInTimeImage` already states that the fallback arm is handed `null` for the pause and why.
  No change.

## Deferred to the developer - a decision only a maintainer can make

The reviewer asked for an explicit "yes, acceptable" on two deliberate behaviour changes. They are argued in
the code and in the tracking doc, and both match what `FullBackupFormat` has done since #6114, but the sign-off
is not something this loop can give itself:

1. A symlinked `configuration.json` / `schema.json` is now shipped (window path) rather than dropped. Pinned by
   a test as of this commit, so the decision is at least visible and guarded either way.
2. Removing the read lock makes the t0-barrier gap reachable during a ship, where before it was reachable only
   by a DDL already past the schema write lock.
