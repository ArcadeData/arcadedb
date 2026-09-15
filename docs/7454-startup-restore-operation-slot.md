# 7454 - the startup `restore:` command takes the #7384 per-database operation slot

Issue: https://github.com/ArcadeData/arcadedb/issues/7454

## Root cause

#7384 gave every backup, restore and import that goes through `ServerControlPlane` a per-database
exclusive slot handed out by `BackupCoordinator`. The `restore:` startup command of
`arcadedb.server.defaultDatabases` does not go through the control plane: `loadDefaultDatabases()`
called `restoreDatabaseFromStartupCommand` directly, and that method touched `BackupCoordinator`
nowhere.

The window is reachable rather than theoretical, and the ordering in `startInternal()` is the proof (every
line number in this document is against the base commit `dd8087d45`, before this branch's edits):

| line | call |
|---|---|
| `ArcadeDBServer.java:448` | `httpServer.startService()` |
| `ArcadeDBServer.java:452` | `pluginManager.startPlugins(AFTER_HTTP_ON)` |
| `ArcadeDBServer.java:476` | `loadDefaultDatabases()` |

No HTTP command handler gates on server status, so a client that authenticates in that window can
have `restore database mydb`, `trigger backup mydb` or `import database mydb` admitted against the
database the startup command is extracting - `beginExclusive` finds the slot free because nothing
took it. Two writers then share one database directory.

`AutoBackupSchedulerPlugin.getInstallationPriority()` returns `AFTER_DATABASES_OPEN`, which runs
*after* `loadDefaultDatabases()`, so the scheduler is not one of the racers. The racers are the
client transports and an HA snapshot install.

## The half the issue's suggested fix did not reach

The issue suggests taking the slot "around the restore in `restoreDatabaseFromStartupCommand`".
That leaves the most destructive step outside it. The `restore:` case in `loadDefaultDatabases()`
**dropped the database it was about to replace** before calling the method:

```java
case "restore":
  // DROP THE DATABASE BECAUSE THE RESTORE OPERATION WILL TAKE CARE OF CREATING A NEW DATABASE
  if (database != null) {
    ((DatabaseInternal) database).getEmbedded().drop();
    removeDatabase(dbName);
  }
  restoreDatabaseFromStartupCommand(dbName, commandParams, ...);
```

Deleting the directory a concurrent backup is reading is exactly the #7384 race. The drop has
therefore moved *inside* `restoreDatabaseFromStartupCommand`, under the same reservation as the
extraction. It could not be reserved twice - the reservations are documented as not reentrant - so
one method owning the whole command is the only shape that covers both steps.

## The decision the issue asked for: what a refusal at boot means

**Refused, not waited out, and fatal to startup.**

- The refusal is the wording every other entry point uses (`MaintenanceCoordinator.refusal`) and the
  type every other server entry point raises (`ServerControlPlane.OperationInProgressException`,
  which extends the engine's `DatabaseOperationInProgressException`).
- `startInternal()` already wraps `loadDefaultDatabases()` in `try { … } catch (Exception e) { stop();
  throw e; }`, so the server stops loudly. That is the answer the `restore:` and `import:` startup
  commands already give every other failure - issue #7484 settled that "a startup misconfiguration
  that silently leaves a default database empty is worse than one that refuses to start".
- Waiting was considered and rejected. `SnapshotInstaller` waits (`begin(name, op, timeoutMs)`,
  #7444) because it applies a committed Raft entry and may not decline. An operator's startup
  command may decline, and the only thing that can hold the slot when it runs is a client operation
  admitted in the boot window above: waiting it out and *then* dropping and restoring over its
  result destroys a completed operation instead of a half-finished one. Refusing leaves both states
  on disk and names what was holding the slot.

## Completeness

### 1. The invariant

> Every whole-database restore this server performs - the `restore:` startup command included, and
> including the drop of the database it replaces - holds **both** protections a control-plane restore
> holds for its whole duration: the `BackupCoordinator` per-database slot, and the #7441 claim on the
> database name.

The second half was added in review cycle 1. The two are not interchangeable: `create database` is
not a participant in the maintenance slot at all (`ArcadeDBServer.createDatabase` consults only
`checkDatabaseNameIsNotBeingRestored` and the existence checks), so the name claim is the only thing
that refuses a client creating this name while the archive is being extracted into its directory.

### 2. Every way to violate it

Every reservation site in main source:

```
$ grep -rn "Operation\.\(BACKUP\|RESTORE\|IMPORT\)" --include="*.java" engine/src/main server/src/main ha-raft/src/main
engine/src/main/java/com/arcadedb/query/sql/parser/BackupDatabaseStatement.java:102:  reserve(context.getDatabase(), Operation.BACKUP)
engine/src/main/java/com/arcadedb/query/sql/parser/ImportDatabaseStatement.java:82:   reserve(context.getDatabase(), Operation.IMPORT)
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1017:                begin(databaseName, Operation.BACKUP)
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1393:                beginExclusive(databaseName, Operation.RESTORE)
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1443:                beginExclusive(targetDatabase, Operation.RESTORE)
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1529:                beginExclusive(databaseName, Operation.IMPORT)
server/src/main/java/com/arcadedb/server/backup/BackupTask.java:116:                  begin(databaseName, BackupCoordinator.Operation.BACKUP)
ha-raft/src/main/java/com/arcadedb/server/ha/raft/SnapshotInstaller.java:306,1015:    begin(databaseName, BackupCoordinator.Operation.RESTORE, timeout)
```

`ArcadeDBServer.java` appears nowhere in that list - it was the only main-source file performing a
whole-database restore with no reservation at all.

Every whole-database delete in server main source (the sibling shape - a drop is what makes a
restore destructive):

```
$ grep -rn "\.drop()" server/src/main/java
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:423:   dropDatabaseClusterWide  -> reached from dropDatabase (NO slot) and from a restore's replace (inside RESTORE)
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1863:  dropQuietly              -> compensating drop, inside the caller's RESTORE/IMPORT reservation
server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:1648:      the startup restore's replace-drop (NO slot) -- fixed here
```

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `restore:` startup command - extraction | **yes** | yes - `aStartupRestoreHoldsTheSlotWhileItRuns`, `aStartupRestoreIsRefusedWhileAConflictingOperationHoldsTheSlot` (x3) |
| `restore:` startup command - the drop of the replaced database | **yes** | yes - `aRefusedStartupRestoreDoesNotDropTheDatabaseItWouldHaveReplaced`, `aStartupRestoreOverAnExistingDatabaseStillReplacesIt` |
| `restore:` startup command - the database NAME, against a concurrent `create database` | **yes** (added in review cycle 1) | yes - `aStartupRestoreClaimsTheDatabaseNameAgainstAConcurrentCreate` |
| `restore:` startup command - slot and claim released on success / on failure | **yes** | yes - `aSuccessfulStartupRestoreReleasesTheSlot`, `aFailedStartupRestoreReleasesTheSlot` |
| `restore:` startup command, driven through the real `loadDefaultDatabases()` loop with a following `import:` | **yes** | yes - `Issue7454StartupRestoreThenImportTest` |
| `import:` startup command (`database.command("sql", "import database …")`) | already covered | argued - see below |
| `import:` startup command's `createDatabase` before the SQL statement | already safe | argued - see below |
| HTTP/gRPC `restore database`, `restore backup`, `import database`, `trigger backup` | already covered (#7384) | existing `Issue7384ConcurrentRestoreIT` |
| SQL `BACKUP DATABASE`, `IMPORT DATABASE` | already covered (#7443) | existing `Issue7443SqlMaintenanceSlotIT` |
| `AutoBackupSchedulerPlugin` / `BackupTask` | already covered | existing |
| HA snapshot install | already covered (#7444) | existing |
| **`ServerControlPlane.dropDatabase`** | **no** | **filed as #7641** |

#### Argued rows, with evidence

- **`import:` startup command.** It runs `database.command("sql", "import database …")` on the
  handle returned by `getDatabase`/`createDatabase`, both declared `public ServerDatabase`
  (`ArcadeDBServer.java:1040`, `:1143`). `ServerDatabase` binds the coordinator as a wrapper
  (`ServerDatabase.java:98`: `wrapped.setWrapper(MaintenanceCoordinator.WRAPPER_NAME,
  server.getBackupCoordinator())`), and `ImportDatabaseStatement.java:82` reserves
  `Operation.IMPORT` through `MaintenanceCoordinator.reserve`. The startup `import:` command
  therefore already inherits the #7443 reservation. Nothing to fix.
- **`createDatabase` before that import.** It runs inside `synchronized (databasesLock)`, calls
  `checkDatabaseNameIsNotBeingRestored(databaseName)` (the #7441 name claim) and throws
  `IllegalArgumentException` if the database already exists, both from the registry and from
  `factory.exists()` (`ArcadeDBServer.java:1148-1172`). A concurrent restore of the same name is
  refused by the name claim, and a concurrent create is refused by the existence checks. The failure
  mode is a refusal, not a shared directory.

### 4. Reachability

`restoreDatabaseFromStartupCommand` is called from `loadDefaultDatabases()`
(`ArcadeDBServer.java:1654`), which `startInternal()` calls on every server boot
(`ArcadeDBServer.java:476`). `backupCoordinator` is a `final` field initialised inline at
`ArcadeDBServer.java:185`, so it exists long before `loadDefaultDatabases()` runs. No flag gates any
of it. `ServerRestoreDatabaseIT` drives the real configuration end to end and stays green.

### 5. Residual risk

- **The startup restore's drop is node-local on an HA node** - filed as
  [#7643](https://github.com/ArcadeData/arcadedb/issues/7643). Unchanged in substance by this branch,
  which moved the drop verbatim.
- **`drop database` is still unslotted** - filed as
  [#7641](https://github.com/ArcadeData/arcadedb/issues/7641). A `drop database mydb` concurrent
  with a backup, restore or import of `mydb` still deletes the directory that operation is using.
  Distinct from [#7469](https://github.com/ArcadeData/arcadedb/issues/7469), which is the same
  omission for `create database`.
- **The slot is per server instance, not per JVM and not cluster-wide.** That scoping is
  `BackupCoordinator`'s documented design (#7384) and is unchanged here.
- **The boot window itself still exists.** Nothing here makes the HTTP listener refuse commands
  while the server is `STARTING`; the fix makes the conflict *detected and refused* rather than
  silent, it does not close the window. Gating handlers on server status is a larger change than
  this issue asks for and is not attempted.

## Changes

- `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`
  - `restoreDatabaseFromStartupCommand` takes `Operation.RESTORE` before it touches anything and
    releases it in a `finally` that wraps the existing progress `finally`.
  - The replace-drop moved into that method from `loadDefaultDatabases()`, so it happens under the
    reservation.
  - `loadDefaultDatabases()` re-resolves its `database` handle after a `restore:` command, because
    the instance it held has been dropped and rebuilt. Previously the stale handle survived into a
    following command in the same `{…}` list.
  - Javadoc records the invariant, the entry points, and why a conflict is refused rather than
    waited out.
- `server/src/test/java/com/arcadedb/server/Issue7454StartupRestoreSlotIT.java` - new, 8 tests.

## Test results

Without the fix (main source restored from `HEAD`, tests unchanged): `Tests run: 8, Failures: 5,
Errors: 1`. The two that pass without it are `aSuccessfulStartupRestoreReleasesTheSlot` and
`aFailedStartupRestoreReleasesTheSlot`: a reservation that is never taken is trivially not leaked,
so those two are leak guards on the new code rather than reproductions of the bug.

With the fix:

```
Issue7454StartupRestoreSlotIT      Tests run: 8,  Failures: 0, Errors: 0
Issue7440StartupRestoreProgressIT  Tests run: 3,  Failures: 0, Errors: 0
Issue7384ConcurrentRestoreIT       Tests run: 8,  Failures: 0, Errors: 0
Issue7443SqlMaintenanceSlotIT      Tests run: 7,  Failures: 0, Errors: 0
Issue7444MaintenanceSlotWaitTest   Tests run: 6,  Failures: 0, Errors: 0
BackupCoordinatorTest              Tests run: 8,  Failures: 0, Errors: 0
ServerRestoreDatabaseIT            Tests run: 1,  Failures: 0, Errors: 0
ServerImportDatabaseIT             Tests run: 1,  Failures: 0, Errors: 0
ServerDefaultDatabasesIT           Tests run: 1,  Failures: 0, Errors: 0
ServerReadOnlyDatabasesIT          Tests run: 1,  Failures: 0, Errors: 0
Issue7484StartupImportFailureTest  Tests run: 2,  Failures: 0, Errors: 0
Issue7385RestoreProgressIT         Tests run: 4,  Failures: 0, Errors: 0
```


## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not read the author's reasoning. **No `Task`
tool was available in this environment**, so the pass was run by the author against the finished
diff instead, with the findings below. That is a weaker instrument and is recorded as such.

| # | Finding | Disposition |
|---|---|---|
| 1 | The issue's own suggested fix - "take the slot around the restore in `restoreDatabaseFromStartupCommand`" - leaves the replace-drop outside the reservation, and the drop is the destructive half. | **Fixed here.** The drop moved inside the method. `aRefusedStartupRestoreDoesNotDropTheDatabaseItWouldHaveReplaced` is the test that would have caught the narrower fix. |
| 2 | Moving the drop leaves `loadDefaultDatabases()`'s local `database` handle pointing at an instance that no longer exists, for a following command in the same `{…}` list. | **Fixed here.** The handle is re-resolved after the restore. This was already true before the change - the old code dropped the instance and kept the variable - so it is an incidental correctness improvement, not a regression this branch introduced, and there is no test for a `{restore:…,import:…}` pair. |
| 3 | A refusal at boot is now fatal to startup, where before the command silently corrupted. Is refusing right? | **Argued.** See "The decision the issue asked for" above. Recorded as a deliberate decision rather than an oversight; a reviewer who disagrees should say so, since the alternative (`begin(name, op, timeoutMs)`, as `SnapshotInstaller` uses) is one line away. |
| 4 | `ServerControlPlane.dropDatabase` has the same omission. | **Filed as [#7641](https://github.com/ArcadeData/arcadedb/issues/7641).** Out of scope. |
| 5 | The drop this branch relocated uses `getEmbedded().drop()`, which unwraps past the Raft wrapper and deletes files locally. `ServerControlPlane.dropDatabaseClusterWide` documents that shape as the #7389 defect ("takes the database out from under the cluster and leaves it on every follower"). | **Filed as [#7643](https://github.com/ArcadeData/arcadedb/issues/7643)** in review cycle 2, after CodeRabbit raised it independently. Two reviewers finding the same thing is what moved it out of this document and into the tracker. The issue lays out the three candidate answers rather than picking one: which behaviour the command means is genuinely open. |
| 6 | Two of the eight new tests pass without the fix. | **Accepted.** `aSuccessfulStartupRestoreReleasesTheSlot` and `aFailedStartupRestoreReleasesTheSlot` guard against a leaked reservation, which is a hazard the fix itself creates; they cannot fail against code that takes no reservation. Named as leak guards in the test results section rather than counted as bug reproductions. |


## Review cycles

### Cycle 1 - `6003e0e`

`claude-review` returned one substantive finding and no blockers. Codacy reported 0 new issues; lint
and every CodeQL analyzer passed.

**Finding: the startup restore took the maintenance slot but not the #7441 name claim.**
Verified against the tree before acting on it, rather than accepted on the reviewer's word:

- `ServerControlPlane.restoreDatabase` (`:1393-1404`) and `restoreBackup` (`:1443-1455`) take two
  protections, `beginExclusive(..., Operation.RESTORE)` **and**
  `reserveRestoreTarget` / `releaseDatabaseNameReservedForRestore`.
- `ArcadeDBServer.createDatabase` (`:1145-1172`) consults `checkDatabaseNameIsNotBeingRestored` and
  the existence checks, and nothing from `BackupCoordinator`. So a `create database` of the name a
  startup restore is extracting into was admitted: the slot cannot refuse it, and no claim existed.
- The window is the one this branch already reasons about, made wider by the branch's own drop: it
  opens the moment the directory being replaced goes away.

**Fixed in cycle 2**, not deferred. `restoreDatabaseFromStartupCommand` now calls
`reserveDatabaseNameForRestore` before the drop and releases it in the same `finally` as the slot.
`aStartupRestoreClaimsTheDatabaseNameAgainstAConcurrentCreate` pins it, and fails
("the startup restore never claimed the database name") when the two calls are removed.

Nothing else in the review was actionable: the remaining sections confirmed the slot lifecycle, the
locking, the test coverage and the style, and explicitly endorsed the two decisions this document
argues for (moving the drop, and refusing rather than waiting).

No deferred items, and no review comment was skipped.

### Cycle 2 - additions

- `reserveDatabaseNameForRestore` / `releaseDatabaseNameReservedForRestore` around the whole command.
- `Issue7454StartupRestoreSlotIT#aStartupRestoreClaimsTheDatabaseNameAgainstAConcurrentCreate`, and a
  name-claim release assertion added to the shared `assertReservable` helper so every other test in
  the class now checks the claim did not leak either.
- `Issue7454StartupRestoreThenImportTest` - written during cycle 1 and pushed here. It is the only
  test that drives the real `loadDefaultDatabases()` loop, and the only coverage of the re-resolved
  `database` handle: it fails with a closed-database error when that one line is removed.

Regression sweep after the cycle-2 changes: `Tests run: 188, Failures: 0, Errors: 0` across the whole
`com.arcadedb.server.backup` package plus every startup-command suite listed above.


### Cycle 2 - CodeRabbit on `351d911`

CodeRabbit posted two inline findings. Both were verified against the tree before being acted on.

1. **"Reserve the target name for the startup restore."** The same finding `claude-review` made on
   `6003e0e`, and already fixed in `351d911` - the commit CodeRabbit reviewed. Replied on the thread
   naming the commit, the ordering (claim before the drop, released with the slot) and the test.
   CodeRabbit had already marked it "Addressed in commit 351d911" itself.
2. **"Use the HA-aware drop contract for startup restore."** Real, verified, and **out of scope**:
   `getEmbedded().drop()` does unwrap past the Raft wrapper, `loadDefaultDatabases()` puts no HA
   restriction on `restore:`, and the HA plugin is started at `ArcadeDBServer.java:452`, before
   `loadDefaultDatabases()` at `:476`, so the dropped handle can be replicated. But the code is moved
   verbatim by this branch rather than changed by it, and which behaviour is correct is open - see the
   adversarial pass, row 5. **Filed as [#7643](https://github.com/ArcadeData/arcadedb/issues/7643)**
   and answered on the thread with the reasoning and the three candidate answers.

No comment was skipped, and nothing was deferred to a notes file.
