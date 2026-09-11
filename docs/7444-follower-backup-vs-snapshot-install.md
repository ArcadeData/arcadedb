# #7444 - A leader-side restore does not exclude a follower's scheduled backup of the same replicated database

Follow-up to #7384. That issue gave every whole-database maintenance operation a per-database
admission slot (`BackupCoordinator`), deliberately scoped **per server instance**: an HA test - and
a co-located pair of nodes - runs several servers with the same database names in one process, and
their backups are genuinely independent.

## The gap as reported

A restore is leader-only on both transports, so restore-versus-restore of one database is already
serialised cluster-wide. A backup is not: `runOnServer` in `backup.json` defaults to `"*"`, so every
node runs its own scheduled backup.

After a successful restore the leader calls `ServerControlPlane.replicateRestoredDatabase`, which
submits an install-database entry with `forceSnapshot=true`. Every follower applies it and
**replaces its own copy of the database directory** from the leader's snapshot. A follower running
its own scheduled backup of that database at that moment has the directory reinstalled underneath
the backup, and its own `BackupCoordinator` slot is the wrong one to consult - the restore happened
on a different JVM.

## Root cause

The slot is taken by every operation that replaces a database directory **except one**: the HA
snapshot install. `ServerControlPlane.performRestore` takes `Operation.RESTORE` (#7384);
`SnapshotInstaller.install` - which closes the live database, swaps its directory for the leader's
snapshot and reopens it - takes nothing.

The reporter offered two shapes of fix and named the smaller one: "the install-database apply path
on a follower takes the local slot before it reinstalls and the follower's backup task waits for or
is refused by it." That is what this change does, one level lower than the apply path so that
*every* install driver is covered rather than only the one the issue named.

## Completeness

### 1. The invariant

> On any one server, a snapshot install of a database and a backup of that same database can never
> be in flight at the same time: the install holds the node's per-database maintenance slot for its
> whole duration, so a backup that has not started is refused, and a backup already running is
> waited for (bounded) before the files are replaced.

### 2. Every way to violate it

Every driver that replaces a local database directory from the leader:

```
$ grep -rn "SnapshotInstaller\.\(install\|acquireNewDatabase\|recoverPendingSnapshotSwaps\)(" --include='*.java' */src/main
ha-raft/.../ArcadeStateMachine.java:615   -> recoverPendingSnapshotSwaps
ha-raft/.../ArcadeStateMachine.java:2970  -> install   (applyInstallDatabaseEntry, forceSnapshot arm - the reported path)
ha-raft/.../ArcadeStateMachine.java:3269  -> install   (installFromLeaderForBootstrap)
ha-raft/.../ArcadeStateMachine.java:3327  -> install
ha-raft/.../ArcadeStateMachine.java:4238  -> install
ha-raft/.../ArcadeStateMachine.java:4463  -> install
ha-raft/.../DatabaseReconciler.java:274   -> acquireNewDatabase
ha-raft/.../DatabaseReconciler.java:280   -> install
ha-raft/.../DatabaseReconciler.java:423   -> install
```

Every caller that takes the slot today (the readers/holders the install has to exclude):

```
$ grep -rn "getBackupCoordinator()\.begin\|coordinator.begin(" --include='*.java' */src/main
server/.../ServerControlPlane.java:930   coordinator.begin(databaseName, Operation.BACKUP)   (HTTP/gRPC trigger backup)
server/.../ServerControlPlane.java:951   begin(databaseName, operation)                      (RESTORE / IMPORT admission)
server/.../backup/BackupTask.java:116    coordinator.begin(databaseName, Operation.BACKUP)   (scheduled tick)
```

Same-shape siblings - everything in `src/main` that starts a full backup:

```
$ grep -rn "backupDatabase\|new Backup(" --include='*.java' */src/main
engine/.../BackupDatabaseStatement.java:151   SQL BACKUP DATABASE - takes no slot (already filed as #7443)
server/.../ServerControlPlane.java:1027       HTTP trigger backup - under the slot taken at :930
server/.../backup/BackupTask.java:246         scheduled backup   - under the slot taken at :116
```

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `ArcadeStateMachine.applyInstallDatabaseEntry` (forceSnapshot) -> `install` - the reported path | yes | yes |
| `ArcadeStateMachine:3269` / `:3327` / `:4238` / `:4463` -> `install` | yes (slot taken inside `install`, not at the call site) | yes - the test drives `install` itself, which is the single choke point all five share |
| `DatabaseReconciler:280` / `:423` -> `install` | yes (same choke point) | yes - same |
| `DatabaseReconciler:274` -> `acquireNewDatabase` (database never seen locally) | argued, see below | n/a |
| `ArcadeStateMachine:615` -> `recoverPendingSnapshotSwaps` (startup crash recovery) | **no** - filed as #7449 | no |
| `BackupTask.run` scheduled tick - the loser side the issue describes | yes (already takes `BACKUP`; `RESTORE` now excludes it) | yes |
| HTTP/gRPC "trigger backup" -> `ServerControlPlane:930` | yes (same slot, same `Operation.BACKUP`) | yes (unit) |
| SQL `BACKUP DATABASE` / `IMPORT DATABASE` | **no** - already filed as #7443 | no |
| Leader takes the slot on every peer before it starts | **no** - the larger of the two options the issue offers; argued below | n/a |

### Argued rows

- **`acquireNewDatabase`.** It only publishes under the final name when the database is *not*
  registered on this node, and it re-checks that immediately before publishing. Both arms where the
  database *is* registered delegate to `install` (SnapshotInstaller.java:410 and :443), which takes
  the slot. A backup cannot be running on a database that is not registered:
  `BackupTask.run` returns at its first line when `!server.existsDatabase(databaseName)`, and
  `performBackup` resolves it with `getDatabase(name, false, false)` - `allowLoad=false` - so it can
  never be the thing that opens it (#6752). The `deleteDirectoryIfExists(dbPath)` before the rename
  runs only on the not-registered arm.
- **Cluster-wide serialisation.** The issue names it as the larger option and says of the smaller
  one that it "may be enough on its own". It is: the hazard is a follower replacing its own files,
  and the follower replaces them only from inside `SnapshotInstaller.install`, which now holds that
  follower's own slot. A cluster-wide slot would additionally cover a backup on a peer that the
  leader's restore never reaches an install for - but there is no such peer, because
  `replicateRestoredDatabase` submits the entry to every replica.

### 5. Reachability

`SnapshotInstaller.install` is on the live follower path: `applyInstallDatabaseEntry` calls it
directly from the Ratis apply thread (ArcadeStateMachine.java:2970), and the whole download runs on
that same thread. `server` is dereferenced unconditionally inside `install`
(`server.getConfiguration()` at the retry-count read), so `getBackupCoordinator()` cannot see a null
server there. No feature flag gates the change off: the new setting only sizes the *wait*, and the
exclusion happens whatever its value.

### 7. Residual risk

- A snapshot install that runs while a backup started before it is **not** prevented; it is waited
  for, for at most `arcadedb.ha.snapshotInstallBackupWaitMs` (default 60s), and then proceeds with a
  WARNING. Blocking indefinitely is not an option: a committed Raft entry has to be applied.
- Startup crash recovery (`recoverPendingSnapshotSwaps`) still takes no slot - #7449.
- SQL `BACKUP DATABASE` still takes no slot - #7443, unchanged by this PR.

## The change

| File | What |
|---|---|
| `server/.../backup/BackupCoordinator.java` | `begin(database, operation, timeoutMs)` - the same reservation, but it waits out a conflicting operation for a bounded time instead of refusing immediately. `end` notifies the waiters. |
| `ha-raft/.../SnapshotInstaller.java` | `install` takes `Operation.RESTORE` for its whole duration through that bounded wait, and releases it in a `finally`. The body moved unchanged into a private `installHoldingMaintenanceSlot`. |
| `engine/.../GlobalConfiguration.java` | `arcadedb.ha.snapshotInstallBackupWaitMs` (`SCOPE.SERVER`, default 60 000) sizes that wait. |

Why a waiting form at all: every other holder of the slot may simply be refused - a scheduled
backup is covered again on the next tick, a restore or an import is an operator command that can be
retried. An install cannot. It applies a committed Raft entry, and a follower that declines to apply
one diverges from the cluster, so an in-flight backup may delay the install but must not veto it.
The wait expiring is therefore not an error: the install proceeds and says so at WARNING, naming the
setting to raise.

Sixty seconds is proportionate rather than cautious. The download already runs on the same Ratis
apply thread and takes minutes for a large database, so the wait is small beside what that thread is
already committed to.

## Test results

```
$ mvn -o -pl server test -Dtest='com.arcadedb.server.backup.*Test'
Tests run: 169, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o -pl ha-raft test -Dtest='Snapshot*Test,Issue7*Test,Issue6*Test,Issue4*Test,ArcadeStateMachine*Test'
Tests run: 659, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o -pl engine test -Dtest='GlobalConfigurationTest,Issue7124...,Issue7163...'
Tests run: 29, Failures: 0, Errors: 0, Skipped: 0
```

New tests:

- `server/.../backup/Issue7444MaintenanceSlotWaitTest` - 6 cases on the bounded wait: a free slot is
  taken without spending the timeout, the slot is taken the instant the holder releases it, the wait
  expires and names the operation still in the way without reserving anything, a zero or negative
  timeout is exactly the non-waiting form, a release on an unrelated database does not hand the
  waiter a slot, and the backup/import pair still coexists through the waiting form.
- `ha-raft/.../Issue7444SnapshotInstallExcludesBackupTest` - 3 cases driving the real
  `SnapshotInstaller.install` against a real `ArcadeDBServer` and a local HTTP server standing in for
  the leader's snapshot endpoint: a backup is refused while the swap is in progress (paused on the
  existing `swapBarrierForTesting` seam), an install waits for a backup already running and the live
  database is untouched while it waits, and a backup that never ends does not block the install past
  the configured wait.

### Both new tests were shown to fail without the fix

With the reservation in `install` replaced by a constant and the waiting `begin` short-circuited to
the non-waiting one:

```
Issue7444MaintenanceSlotWaitTest: Tests run: 6, Failures: 2
  aWaitOnOneDatabaseIsNotWokenByAnUnrelatedOne:128
  theSlotIsTakenAsSoonAsTheOperationInTheWayReleasesIt:72 [the install waits instead of replacing the files under a running backup]

Issue7444SnapshotInstallExcludesBackupTest: Tests run: 3, Failures: 2
  aBackupIsRefusedWhileThisNodeIsReinstallingTheDatabase:146 [the install holds this node's maintenance slot while it replaces the files]
  anInstallWaitsForABackupThatIsAlreadyRunning:195 [the install waited instead of replacing the files under a running backup]
```

The third case in each class pins a safety property rather than the bug (the wait must stay bounded;
a zero timeout must behave like the non-waiting form), so it passes either way by design.

## Impact

- HA followers: a scheduled backup is now refused - and logged as skipped by `BackupTask`, which
  already names the operation in the way - while the node reinstalls that database from the leader.
  The schedule covers it again on the next tick.
- No change outside HA: `install` only runs on a node pulling a snapshot.
- No hot path is touched. The reservation is one `ConcurrentHashMap.compute` per install, and the
  new monitor in `end` is uncontended except while an install is actually waiting.
- An existing unit test (`Issue7139RetainedBackupNotDestroyedTest`) drives `install` with a
  partially-stubbed `ArcadeDBServer` whose `getBackupCoordinator()` returns null. Rather than change
  that test, the reservation tolerates a null coordinator, the way `downloadSnapshot` and
  `purgeRaftLogBeforeInstall` in the same class already tolerate a partially-stubbed server.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not seen this document. The `Task` tool was
disabled in this session (`Error: No such tool available: Task`), so the pass was run by the author
against the tree instead - which is weaker, and is recorded as such. Findings:

1. **`acquireNewDatabase`'s publish arm still deletes `dbPath` without the slot.** Real, argued
   above rather than fixed: that arm runs only while `!server.existsDatabase(databaseName)`, both
   arms where it is registered delegate to `install`, and `BackupTask` returns at its first line for
   an unregistered database. Not fixed because fixing it needs the reservation released before each
   delegation to `install` (the slot is not reentrant), which is complexity for a path the reported
   scenario never reaches: a restore replicates as a `forceSnapshot` entry, which lands in
   `applyInstallDatabaseEntry` -> `install`, never in `acquireNewDatabase`.
2. **The reservation changed install-versus-install behaviour, not only install-versus-backup.**
   Real, and an improvement: a second install of the same database on the same server now waits for
   the first instead of overlapping it and logging `INSTALLS_IN_FLIGHT`'s WARNING. Written down on
   that field, together with why the WARNING is still reachable (the wait is bounded).
3. **`Operation.RESTORE` is reused rather than a new `SNAPSHOT_INSTALL` constant.** Deliberate. A
   follower's log will read "a restore of it is already in progress" for a backup skipped by an
   install, which is true - the node is restoring its own copy - but is not the word an operator who
   issued no restore would predict. A fourth constant would read better and would pass #7384's
   `Operation.values()` loops unchanged; it was not added because the whole point is that an install
   must be admitted exactly as a restore is, and a second constant that must never diverge from the
   first is a thing to keep in sync rather than a distinction. Named here so a reviewer can disagree.
4. **A stale line reference in a comment.** Real, fixed: the null-coordinator comment cited
   `ArcadeDBServer:161`, which drifts. It now names the field instead of the line.
5. **Does any caller pass a null `server` into `install`?** Checked, no:
   `grep -rn "SnapshotInstaller.install(" */src/test` returns seven call sites, five with a real
   `ArcadeDBServer` and two (`Issue7139RetainedBackupNotDestroyedTest`) with a Mockito mock whose
   `getBackupCoordinator()` is unstubbed - the case the null tolerance covers.
6. **Port binding in the new ha-raft test.** Each case starts a real `ArcadeDBServer`, which binds
   the 2480-2489 HTTP range. Not new: `SnapshotInstallSwapLockTest` in the same package does exactly
   the same, and the range auto-increments.
