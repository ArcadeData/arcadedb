# Performance tracking phase 3: regression alerting - design

Issue: #12434. Parent: #12427. Inputs: `docs/superpowers/specs/2026-11-21-perf-tracking-phase2-3-inputs.md`. Phase 1:
`docs/superpowers/specs/2026-11-17-continuous-performance-tracking-design.md`.

## Intent

The nightly and release runs record every measurement, but a regression is noticed only if someone happens to open the
dashboard. Phase 3 judges every new run automatically and tells maintainers through the one channel that is not lost: an
issue in ArcadeData/arcadedb. As with the Bolt compatibility and client contract checks, the sooner the better: a
regression is reported on the first bad night.

## Goals

- Every nightly and release run published to ArcadeData/arcadedb-perf is judged, within 6 hours of its publication.
- A regression opens one issue per **regression event**: all lines that got worse in the same run, titled with the commit
  range that introduced them, so there is one place to bisect.
- Later runs comment on that issue (still worse, recovered), and the issue closes itself when every line has recovered.
- A maintainer can accept a regression (an intended trade-off, a bench change) by closing the issue; it then stops
  alerting.
- The rule is public, deterministic and explained in the issue: anyone can check a verdict from the numbers it shows.

## Non-goals

- Bisecting automatically to the guilty commit.
- Alerting on manual (dry) runs: only the triggers the dashboard charts, `nightly` and `release`, are judged.
- A control case or a host-noise check during the run (inputs doc, "Starting point" 5): later, once false alarms show
  whether they are needed.
- Notifications other than the issue (mail, chat).

## Anti-goals (a failure even if it works)

- **Noise in a public repository**: issues opened by host jitter train people to ignore them. The band and the absolute
  floor exist for this; a false alarm is a bug in the rule, fixed in the rule.
- **Duplicates**: the same regression opening a second issue, or the same run commenting twice.
- **A regression that silently stops alerting** without a maintainer closing it.
- **New secrets or a person as author**: the issues come from `github-actions[bot]`.

## Constraints

- Runs on GitHub-hosted runners in ArcadeData/arcadedb, with the workflow's own `GITHUB_TOKEN` (`issues: write`,
  `contents: read`). It does not touch perf-01 or the results store.
- Reads only the public ArcadeData/arcadedb-perf repository: the raw run files `data/YYYY/MM/*.json` (they carry every
  repetition's value, so the spread is known) and nothing private.
- Stateless and idempotent: running it twice, or after a missed schedule, gives the same issues and comments. Whatever
  must be remembered lives in the issues themselves.
- The comparison logic is Java in the `bench` module, unit-tested, next to the exporter; the workflow only moves data and
  calls the GitHub API.

## Approaches

| | Where | Trade-off |
|---|---|---|
| A | at the end of the perf-infra job on perf-01, reading the results store | alerts at once, but needs a personal token with issue access on arcadedb (issues authored by a person), a new secret, and private logic |
| **B (chosen)** | a scheduled workflow in ArcadeData/arcadedb, reading the public raw data | up to 6 h of delay; no new secret, `github-actions[bot]` as author like the Bolt nightly, public reviewable rule |
| C | a workflow in arcadedb-perf on each publish commit | no delay, but the same personal-token problem as A |

The maintainer chose B, scheduled **every 6 hours** (4 runs a day), plus `workflow_dispatch`.

## Design

### Lines and points

A **line** is one (machine, variant, scale, workload, case, metric), as on the dashboard; `_status` is the line of a case
without metrics. A **point** is one run's value on a line: the median, every repetition's value, the case status, the
measured commit and its commit date, the run id, the bench commit and the tag for releases.

The judge reads all raw files, keeps triggers `nightly` and `release`, and builds the lines with their points ordered as
the dashboard orders them (commit date, then run time). A file of the older shape (no `schemaVersion`, `commitDate`, `tag`
or `config`, like the 2026-11-17 sample) is ordered by its run time, as the exporter does.

A **run** for this purpose is one trigger at one measured version: a nightly is identified by its measured commit, a
release by its tag. The three files of one nightly (`-jvm`, `-native`, `-jvm-ldbc`, published one to three hours apart)
are one run; so are the M and L files of one release. A point is identified the same way (trigger, commit or tag, plus the
line), never by `runId`: a release re-run with `replace` deletes the earlier raw file and gets a new `runId`, but it is the
same run.

### What is judged

The judge is stateless: on every invocation it judges, **separately for each trigger**, the newest point of that trigger on
every line against its baseline, and the issue markers decide what has already been reported (below). Judging per trigger
matters on M lines, which hold both nightly and release points: a release measured today at an older commit is still
judged, and it never hides a nightly's verdict.

- A **nightly** point's baseline is the previous **valid** nightly point of the same line.
- A **release** point's baseline is the newest valid point of the **previous different tag** on the same line (a re-run of
  the same tag is never its own baseline). The issue then says "release 26.11.1 vs 26.10.2".
- A point is **valid** when its case status is `ok`. A failed or timed-out run is never a baseline.
- Nightly and release points never judge or recover each other.

### When a line is worse

Status first: a **case** whose baseline is `ok` and whose newest point is `failed` or `timeout` is **worse**, whatever the
values. It counts as **one** worse line, the case's `_status`, not one per metric: the metrics of a failed case (some
carry them, e.g. a recall under its gate) are not judged on that run. A case that was already failing at its baseline is
not reported again.

Otherwise, with `better` = lower or higher from the metric, and `change` the relative change of the medians signed so
that positive means worse:

- `spread(point)` = (max - min) / median of the point's repetition values; 0 with fewer than 2 values.
- `band` = max(spread(baseline), spread(newest), **10%**).
- the line is **worse** when `change > band` **and** the absolute difference of the medians is at least the unit's
  absolute floor: 1 ms for `ms`, 0.5 s for `s`, 1 MiB for `bytes`, 0 for rates and ratios. The floor keeps a 0.4 ms to
  0.6 ms latency (+50%) from alerting.
- a base of 0 is judged by the absolute floor alone.
- **better** is the mirror case; improvements are reported in comments, never as an issue.

The 10% floor and the per-unit floors are constants in one place, with a test each, so tuning them after a false alarm is
a one-line change with a reason.

### Regression events and issues

All lines that are worse in the same run (one trigger at one measured commit or tag) form one event, whatever file they
came from: the three files of a nightly published in different invocations land in the same issue. The event's **commit
range** is the previous nightly run's commit to this run's commit; for a release, the previous tag to this tag. A line
whose own baseline is older (its previous nightly had failed) shows its own baseline commit in its table row.

An issue carries:

- the label `performance-regression` (created by the workflow if missing)
- the title `Performance regression: N lines worse between <base7>..<new7>` (nightly) or
  `Performance regression: N lines worse in release <tag> vs <tag>`
- a body with the compare link, a table (workload, case, scale, variant, metric, before, after, change, band) with links
  to the dashboard workload page and to both raw files, a note when the **bench sources** changed between the two runs
  ("the bench changed between these runs: check it first"), and how to accept (close the issue). `config.benchCommit` is
  the arcadedb `main` HEAD the jar was built from and differs almost every night, so comparing it means nothing: the
  workflow runs `git diff --quiet <base benchCommit> <new benchCommit> -- bench/` in its own arcadedb checkout (full
  history) and passes the answer to the gate for every pair of bench commits it needs
- a hidden marker holding the event's state as JSON: the trigger and the run (commit or tag), the lines with their
  baseline values, and the runs already reported (by commit or tag), `<!-- perf-regression {...} -->`. The marker is what
  makes the workflow stateless.

### Each invocation

1. Read every open issue with the label, and the closed ones of the last 30 days, with their markers.
2. Judge the newest point of every line.
3. For each worse line:
   - already in an **open** issue (same line) and the newest run already reported there: nothing.
   - already in an open issue, newest run not yet reported: it goes in that issue's next comment as **still worse**.
   - in an issue a maintainer **closed** (not the bot) whose recorded baseline run (commit or tag) is this line's
     baseline: accepted, nothing.
   - otherwise: part of this run's event. If an issue for the same run (trigger and commit or tag) is already open, the
     line is added to it (comment, marker updated); otherwise the event opens a new issue.
4. For each line of an open issue that is no longer worse (its newest point of the issue's trigger within the band of the
   issue's recorded baseline, or its status `ok` again): **recovered** in the next comment. A **nightly** issue closes
   itself, as completed with a last comment, when every line has been recovered on **2 consecutive nightly runs**. A
   **release** issue needs **1** later release where every line has recovered (releases are a month apart); until then
   it stays open, and a maintainer can close it.
5. Better lines, in the open issue whose lines they share (if any), are listed as improvements in that issue's next
   comment.
6. At most one comment per issue per run, marked with the run (commit or tag), so a rerun of the workflow, or a release
   re-measured with `replace`, never comments twice.

A closed issue a maintainer reopens is tracked again from its marker.

### Components

- **`bench gate`** (Java, bench module): reads the raw files of an arcadedb-perf checkout and the current issues (their
  number, state, who closed them, marker JSON) from a file the workflow writes, and prints a plan as JSON: issues to open
  (title, body, label), comments to add, markers to update, issues to close. No network access; every rule above is a unit
  test over synthetic points and issues.
- **`.github/workflows/perf-regression.yml`** (arcadedb): cron every 6 hours and `workflow_dispatch`; concurrency group so
  two invocations never race; checks out arcadedb-perf (public, read-only), builds the bench module, lists the labelled
  issues with `actions/github-script` into a file, runs `bench gate`, and applies the plan with `actions/github-script`
  (create, comment, update body, close), as `bolt-nightly.yml` does. Pinned action SHAs as in the other workflows.

### Testing

- Unit tests for the judge: status flip (one line per case), band from the spread, the 10% floor, each absolute floor,
  zero base, higher and lower is better, per-trigger judging on a line with both, release baseline = previous different
  tag, a same-tag re-run, an invalid (failed) baseline skipped, the older file shape.
- Unit tests for the plan: new event, same event on a rerun (no change), the three files of one nightly arriving in
  different invocations (one issue), still worse, recovered once and twice (close), a release issue closed by one
  recovered release, manual close = accepted, reopened issue, one comment per run, a release re-measured with `replace`
  (no second comment), bench-change note on and off.
- A fixture test over a copy of real arcadedb-perf files (the 2026-11-20 and 2026-11-21 nightlies, 26.10.2, 26.11.1):
  the plan it produces is checked by hand once and pinned.
- The workflow is run once with `workflow_dispatch` and a dry-run input that prints the plan without touching issues,
  before the schedule is enabled.

## Left to the builder

- The exact JSON shapes of the plan and the issue list file.
- Whether the gate reads the raw files or the exported series (the raw files are needed for the repetition values).
- Table formatting and the dashboard deep links.
- The cron minute (4 runs a day, every 6 hours).
