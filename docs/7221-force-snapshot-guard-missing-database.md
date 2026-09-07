# #7221 - the forceSnapshot replay guard skips reinstalling a database that is no longer on disk

Issue: https://github.com/ArcadeData/arcadedb/issues/7221
Branch: `fix/7221-force-snapshot-guard-missing-db`

## Finding ledger

- [x] 1. `applyInstallDatabaseEntry`'s `forceSnapshot` replay guard (#7143) returns early on a persisted
      applied index alone, so a database whose directory was deleted is never reinstalled.

## Root cause

`ArcadeStateMachine.applyInstallDatabaseEntry` (force branch) reads
`readPersistedAppliedIndex(databaseName)` and returns when it is `>= entryIndex`. The index lives in
`<SERVER_DATABASE_DIRECTORY>/.raft/applied-index` - a *sibling* of the per-database directories, not a
file inside them:

```java
private Path getRaftDir() {
  ...
  return Path.of(dbDir, ".raft");
}
private Path getAppliedIndexFile() {
  final Path raftDir = getRaftDir();
  return raftDir != null ? raftDir.resolve("applied-index") : null;
}
```

Deleting one database's directory therefore leaves that database's entry in the map intact, and the
guard's stated premise ("a previous session ran this install to completion and Raft has replicated
this database forward since") is a claim about a past session, never about the database being present
now. The normal-create arm 40 lines below asks the question the force arm skips:

```java
if (server.existsDatabase(databaseName)) { ... return; }
```

## Completeness

### 1. Invariant

**A `forceSnapshot` `INSTALL_DATABASE_ENTRY` is never skipped as an already-applied replay while the
database it names is absent from this node.**

### 2. Enumeration

```
$ grep -rn --include='*.java' "applyInstallDatabaseEntry" .
./ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue7143ForceSnapshotReplayGuardTest.java:86,99,110
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:348   (javadoc)
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:948   (apply dispatch)
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:2736  (declaration)
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:3067  (javadoc)
```

One production caller: the `INSTALL_DATABASE_ENTRY` case of the apply dispatch.

Producers of `forceSnapshot=true`:

```
$ grep -rn --include='*.java' "createInReplicas(" .
server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java:247   createInReplicas()
server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java:611   createInReplicas()
server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java:1362  createInReplicas(true)  <- the only true
ha-raft/.../RaftReplicatedDatabase.java:3266,3279  server/.../HAReplicatedDatabase.java:30,44,47
```

Same-shape siblings - every `persistedApplied`-based early return in the module:

```
$ grep -rn --include='*.java' "persistedApplied >=\|persistedApplied >\|persistedApplied<" ha-raft/src/main/java
ArcadeStateMachine.java:688   staleSnapshot = persistedApplied >= 0 && snapshotIndex > persistedApplied + tolerance
ArcadeStateMachine.java:2756  if (persistedApplied >= entryIndex)      <- the bug
ArcadeStateMachine.java:2869  if (persistedApplied >= 0 && persistedApplied < index)
ArcadeStateMachine.java:2904  if (persistedApplied >= index)
ArcadeStateMachine.java:3637  (comment)
```

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `INSTALL_DATABASE_ENTRY` apply -> `applyInstallDatabaseEntry` force arm, database absent | **yes** | yes - `aReplayedForceSnapshotEntryStillReinstallsWhenTheDatabaseIsGone` |
| `INSTALL_DATABASE_ENTRY` apply -> force arm, database present (the #7143 case) | unchanged | yes - `theGuardStillSkipsTheReDownloadWhenTheDatabaseIsStillThere` + `Issue7143ForceSnapshotReplayGuardTest` |
| `INSTALL_DATABASE_ENTRY` apply -> normal-create arm | already checked existence before this PR | pre-existing |
| `applyBootstrapFingerprintEntry` line 2904 `persistedApplied >= index` | **argued** | - |
| `applyBootstrapFingerprintEntry` line 2869 superseded guard | **argued** | - |
| `reinitialize()` line 688 stale-snapshot check | **argued** | - |

Arguments (section 3 outcome 3, with the evidence read in the tree):

- **line 2904** - skipping bootstrap *verification* for an absent database reaches the same outcome the
  unskipped path does: `recordBootstrapBaseline` already ran at line 2880, above the guard, and the very
  next statement after the guard is `if (!server.existsDatabase(dbName)) { log; return; }`. Both arms
  return without installing anything, so presence cannot change the result.
- **line 2869** - the "superseded" branch deliberately does not record the baseline ("it describes a
  state this cluster never adopted"). That decision is about Raft history, not about files on disk; an
  absent database still gets its copy from the follow-on `INSTALL_DATABASE_ENTRY`.
- **line 688** - compares the *global* applied index against the Ratis snapshot index inside
  `reinitialize()`. It is a Raft-log position check that publishes a read floor; it never skips an
  install, so the invariant does not apply to it.

### 4. Which check

`server.existsDatabase(name)` - the registry check the create arm 40 lines below already uses, and the
one the reporter proposed. `loadDatabases()` runs at `ArcadeDBServer.java:359`, well before the HA
plugin starts (`startPlugins(AFTER_HTTP_ON)`, line 376), so by the time Ratis replays the log every
database present on disk is registered and the check is meaningful.

### 5. Reachability

`applyInstallDatabaseEntry` is reached from the `INSTALL_DATABASE_ENTRY` case of `applyTransaction`'s
dispatch (line 948) on every peer that applies the entry, and no configuration flag gates it. The force
arm is entered whenever `RaftLogEntryCodec` decodes `forceSnapshot=true`, which the restore-database
HTTP flow commits (`PostServerCommandHandler.replicateRestoredDatabase` -> `createInReplicas(true)`).

## Residual risk

- The check is on the server's **registry**, not on the filesystem. With
  `arcadedb.server.databaseLoadAtStartup=false` (default `true`) nothing is registered at startup, so a
  replayed force entry re-downloads the snapshot even though the files are on disk. That is the
  pre-#7143 behaviour - wasteful, never wrong - and it is the same assumption the normal-create arm has
  made all along.
- Deleting a database directory *underneath a running node* still hits the skip: the database stays
  registered, so the guard sees it as present. Only the restart-based recovery the issue describes is
  covered. Removing a live database's files out from under an open server is not a supported operation
  in the first place - the open file handles keep the inodes alive - so this is a limit of the check,
  not a hole in the recovery path.
- Not a risk, checked rather than assumed: a database **dropped through Raft** after the force entry is
  not resurrected by this change. `applyTransaction` routes a `DROP_DATABASE_ENTRY` through
  `writePersistedAppliedIndexDroppingDatabase` (ArcadeStateMachine.java:963-966), which evicts the
  per-database entry, so `readPersistedAppliedIndex(D)` already returns `-1` for a dropped database and
  the skip was unreachable for one before this change too.

## The fix

`ArcadeStateMachine.applyInstallDatabaseEntry`, force branch: the skip now also requires the database to
be present, and the log line no longer asserts a completed reinstall the filesystem contradicts.

```java
if (persistedApplied >= entryIndex) {
  if (server != null && server.existsDatabase(databaseName)) {
    ... "already reinstalled ... and is present on this node; skipping the snapshot re-download"
    return;
  }
  ... "was reinstalled ... but is not present on this node now; reinstalling it from the leader"
}
```

`server != null` keeps the guard itself from being the thing that throws when no server is wired; a null
server means no positive evidence the database is here, so the reinstall proceeds - which is the same
stance every other "absent evidence" branch in this class takes.

## Tests

`ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue7221ForceSnapshotGuardMissingDatabaseTest.java`

- `aReplayedForceSnapshotEntryStillReinstallsWhenTheDatabaseIsGone` - writes the applied index, performs
  the operator's wipe (close, deregister, delete the directory), builds a *fresh* state machine so the
  index is read back from the surviving sibling file, asserts the index survived, then asserts the entry
  reaches the download path. Verified to FAIL before the fix, with the misleading log line in the output:
  `Database 'db-wiped' already reinstalled by this entry in a previous session (persistedAppliedIndex=50
  >= entryIndex=42); skipping the snapshot re-download`.
- `theGuardStillSkipsTheReDownloadWhenTheDatabaseIsStillThere` - pins the #7143 half: a present database
  is still skipped.

Results:

- `Issue7221ForceSnapshotGuardMissingDatabaseTest` + `Issue7143ForceSnapshotReplayGuardTest`: 5/5 green.
- Full `ha-raft` unit lane (`-DexcludedGroups=benchmark,slow,vector`): **389 run, 0 failures, 0 errors**.
  The run then ends on a forked-VM crash attributed to `LeaveClusterTest`. It is environmental, not this
  change: `lsof -nP -iTCP:2480 -sTCP:LISTEN` shows two other JVMs already holding port 2480 (concurrent
  agents on this machine), which is the condition `CLAUDE.md` documents, and `LeaveClusterTest` passes on
  its own on this branch (2/2 green, 32.55 s).

## Ledger

- [x] 1. `applyInstallDatabaseEntry`'s `forceSnapshot` replay guard - **fixed**, with a regression test
      proven to fail before the change.

## Adversarial pass (Phase 1.5)

No `Task` tool is exposed in this environment, so the pass could not be run by a subagent that had not
already been persuaded; it was run by the author instead, and that weaker provenance is stated here
rather than hidden. Findings:

1. **The log line over-claimed.** "is not present on this node" describes the filesystem, but the check
   that runs is `existsDatabase`, a *registry* lookup - and with a null server nothing was checked at
   all. Claiming more than was verified is the exact defect the issue reports. **Fixed here:** both
   messages now say "registered", and the anomalous branch logs at `WARNING`, since a bookkeeping record
   that disagrees with the node's own registry is what an operator hunting a missing database needs to
   see.
2. **"A Raft-dropped database is now resurrected on replay."** **Not real, and the first draft of this
   document asserted it as a residual risk.** `DROP_DATABASE_ENTRY` evicts the per-database applied index
   (ArcadeStateMachine.java:963-966), so the guard already read `-1` for a dropped database and never
   skipped. Corrected in Residual risk above and recorded in a comment at the guard.
3. **"The reinstall can now throw and quarantine the database where it used to return quietly."** Real
   but not new: the same throw is what `Issue7143ForceSnapshotReplayGuardTest`'s two
   not-yet-applied cases already exercise on `HEAD`. This change only widens *which* replays take that
   arm, to exactly the set the issue says must take it.
4. **"`assertThatThrownBy` with no type assertion passes on any throwable."** True, and deliberate:
   the discriminator is throw-vs-return - the guard firing returns normally, which is what the test
   observed before the fix. Pinning the exception type would couple the test to the accident that this
   unit test has no Raft server wired. Same form as the neighbouring #7143 tests.

## Review cycles

### Cycle 1 - `cf7ea5ab2b`

`claude` reviewed the diff and independently re-verified three of the PR's claims (that `existsDatabase`
is a pure registry lookup, that the `DROP_DATABASE_ENTRY` eviction makes resurrection unreachable, and
that the bootstrap-guard row is correctly argued rather than blanket-fixed). One non-blocking
observation, and it was a real defect:

> Now the WARNING branch falls through into [the restore block] for a database that's merely
> un-registered. That block calls `raftHAServer.getLeaderId()` with no null-guard on `raftHAServer`
> itself [...] it is now reachable from more states than before.

**Applied.** Checked before agreeing, and the finding is sharper than the sandbox could see:
`resolveSnapshotSource` is *already* carefully null-safe - it reads the volatile once into a local and
returns `PeerDialAddress.refuse("the HA server is not available on this node")` - but the argument
expression `raftHAServer.getLeaderId()` is evaluated **before** the call, dereferencing the very field
that guard exists to tolerate. `getClusterToken()` below it had the same shape. The force arm now reads
the volatile once into a local and passes `raftHA != null ? raftHA.getLeaderId() : null`;
`PeerDialAddress.resolve` refuses a null peer id as "the leader is unknown"
(`PeerDialAddress.java:91-92`), so the refusal path is reached instead of an NPE.

On the reviewer's "worth a quick sanity check that `raftHAServer` truly can't be null in production" -
checked rather than assumed, and the neighbouring comment's "a teardown can null it" was *not* adopted
as fact: `grep -rn --include='*.java' "setRaftHAServer" .` shows no production caller passing null
(`RaftHAServer.java:1419` is the only production call at all). The reachable window is the other one -
the field starts null, and a state machine that has not been rewired yet still carries null, which is
precisely the regression `Issue4839RecoveryRewiresStateMachineIT` exists to catch. The comment at the
call site says that, not the teardown story.

Knock-on improvement to this PR's own test: with the refusal reached instead of an NPE, the
discriminator in `aReplayedForceSnapshotEntryStillReinstallsWhenTheDatabaseIsGone` stopped being
accidental, so it now asserts `hasMessageContaining("Cannot reinstall database 'db-wiped' from the
leader")` rather than merely "something was thrown" - which also answers the reviewer's parenthetical
about the test passing on an incidental NPE. The three existing `Issue7143ForceSnapshotReplayGuardTest`
cases were left untouched and still pass on the new exception.

Everything else in the review was confirmation or praise; nothing was deferred, and no notes file was
produced.

Tests after the cycle-1 change: `Issue7221` + `Issue7143` + `Issue6202SnapshotInstallGuardTest` +
`Issue6111StaleSnapshotReadFloorTest` + `Issue6760PartialSnapshotInstallTest` = 44 green; full `ha-raft`
unit lane again 389 run / 0 failures / 0 errors, with the same environmental `LeaveClusterTest` fork
crash from the externally-held port 2480.

### Cycle 2 - `514e90ff26`

`claude` re-verified the sibling-directory claim, the `existsDatabase` mirror of the create arm, both
"argued, not fixed" rows, and the cycle-1 null fix ("a genuine, separate bug fix, not defensive
padding"). Two non-blocking notes, both about the *other* volatile collaborator:

> `server != null && server.existsDatabase(databaseName)` [...] reads the volatile `server` field twice
> [...] the same pattern the second commit just applied to `raftHAServer` two lines below.

> if `server` were ever null when this branch's `persistedApplied >= entryIndex` is true, execution now
> falls through to the restore flow and eventually calls
> `SnapshotInstaller.resolveDatabasePath(server, databaseName)`, which dereferences `server` unguarded.

**Both applied, as one change**, because they are one inconsistency: the force branch now reads
`this.server` once into `localServer` and uses it for the guard, for `resolveDatabasePath` and for the
`install` call. The reviewer's "server and raftHAServer are wired together in the same factory method"
was checked rather than accepted: `grep -rn --include='*.java' "\.setServer(" ha-raft/src/main/java
server/src/main/java` gives exactly one production call, `RaftHAServer:1418`, on the line before
`sm.setRaftHAServer(this)` in `createStateMachine()`, whose own javadoc says both collaborators must be
set. So the two nulls are the same not-yet-wired state, and the comment at the call site says that with
the line reference.

The normal-create arm below was deliberately left reading the field: it is a different branch, it is
not what this issue is about, and widening the diff into working code to make an unrelated arm
symmetrical is not a trade this PR should make. The comment says so, so the next reader does not read
the asymmetry as an oversight.

Tests after the cycle-2 change: the six state-machine/snapshot classes = 48 green; full `ha-raft` unit
lane again 389 run / 0 failures / 0 errors, with the same environmental `LeaveClusterTest` fork crash
from the externally-held port 2480.

### Cycle 3 - `bc228a43ce`

`claude` re-verified `existsDatabase`, the sibling-file claim, the cycle-1 null fix, the consecutive-line
wiring, and the fall-through logic. One item:

> `SnapshotInstaller.resolveDatabasePath` [...] dereferences `server.existsDatabase(...)` with no null
> guard. The new code passes `localServer` there, which *can* be null [...] It's only safe today because
> `resolveSnapshotSource(null)` short-circuits with a refusal [...] but that safety is incidental to the
> ordering of two independently-nulled volatile fields.

**Applied, with a correction to the reason.** The reachability analysis is right, but the safety is a
guarantee rather than an accident, and the difference matters for whether a null check is warranted:
both fields are declared `volatile` (`ArcadeStateMachine.java:197-198`) and the single writer sets
`server` **before** `raftHAServer` (`RaftHAServer.java:1418-1419`), so a thread that observed a non-null
`raftHAServer` - which it must have, or `resolveSnapshotSource` refuses and the arm throws before
`resolveDatabasePath` is reached - is guaranteed by the JMM to observe the `server` write too.

The reviewer offered "a comment at `resolveDatabasePath` (or a defensive null check there)"; the comment
is the right half of that choice, and the null check is declined with a reason rather than skipped.
`grep -rn --include='*.java' "resolveDatabasePath(" .` shows **seven** call sites (five more in
`ArcadeStateMachine`, two in `DatabaseReconciler`), all passing the field directly. A null check added
for this PR would change the behaviour every one of them sees, in service of an issue about a replay
guard - and both of the method's branches dereference `server` anyway (`existsDatabase`, then
`getConfiguration`), so no useful behaviour exists behind such a check: a null server means the caller is
unwired, and an unwired caller has nowhere to install a snapshot to. The loud dereference is the correct
outcome; only the precondition was missing, so the precondition is what was added, on
`resolveDatabasePath`'s own javadoc where all seven callers can see it.

Tests after the cycle-3 change: the seven snapshot/state-machine classes = 57 green; full `ha-raft` unit
lane again 389 run / 0 failures / 0 errors, with the same environmental `LeaveClusterTest` fork crash
(port 2480 still held by three foreign listeners at the time of the run).
