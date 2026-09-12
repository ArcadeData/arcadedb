# Issue #7510 - a replicated group change reaches a peer's cached permissions only on the security reload tick

- Issue: https://github.com/ArcadeData/arcadedb/issues/7510
- Type: bug (labels already on the issue: `bug`, `server`, `ha`, `concurrency`, `security`, `in progress`; assignee `robfrank`)
- Branch: `fix/7510-ha-replicated-group-permission-refresh`
- Refs: #7373 (the PR that made group changes replicate at all)

## Root cause

`server-groups.json` is the durable state, but the *enforcement* state is derived: every
`ServerSecurityUser` keeps a `ConcurrentHashMap<String, ServerSecurityDatabaseUser> databaseCache`
(`ServerSecurityUser:38`) built the first time that principal touches a database, and only
`ServerSecurity.updateSchema(DatabaseInternal)` re-derives it.

- On the node that served the request, `ServerControlPlane.saveGroup`/`deleteGroup` call
  `refreshPermissionsOf(database)` (`ServerControlPlane:644`, `:666`, `:673`) right after the
  cluster-wide mutation, so the caches follow the document immediately.
- On a **peer**, the entry arrives as `SECURITY_GROUPS_ENTRY` and lands in
  `ServerSecurity.applyReplicatedGroups` (`ArcadeStateMachine:3692`). That method installs the
  document and deliberately does nothing else: it runs on the Raft state-machine apply thread,
  which must never block, and `updateSchema` walks every open database.
- The peer therefore converged only through `SecurityGroupFileRepository`'s own file watcher
  (`SecurityGroupFileRepository:174-195`), which polls `file.lastModified()` every
  `arcadedb.server.reloadEvery` ms (`GlobalConfiguration:1629`, default **5000**) and then fires
  `reloadCallback` -> `ServerSecurity`'s constructor lambda (`ServerSecurity:110-115`) ->
  `updateSchema` per database.

So for up to one reload interval a peer keeps granting the **old, wider** access to any principal
that was already connected, while `GET /server/groups` on that same peer already returns the new
definition and the operator's request has already returned 200. Nothing an operator can query
shows the gap.

### The lag is real, not a never

`persist()` writes the file without touching `fileLastUpdated` (which only `load()` sets), so the
watcher does eventually see the replicated write and fire. The defect is latency, not a permanent
miss - which matches the issue report.

## Invariant the fix establishes

> Once a replicated group document has been installed on a node, that node re-derives the cached
> per-database permissions of every database it has open **without waiting for the
> `arcadedb.server.reloadEvery` file-watcher tick**, and without doing that work on the Raft
> state-machine apply thread.

## Completeness

### Sweep commands and output

```
$ grep -rn "applyReplicatedGroups" --include='*.java' server/src/main ha-raft/src/main grpcw/src/main engine/src/main
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:1067:  public void applyReplicatedGroups(final String groupsJson) {
ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:3692:      server.getSecurity().applyReplicatedGroups(payload);
(the remaining 5 hits are javadoc references)
```

```
$ grep -rn "\.updateSchema(" --include='*.java' . | grep -v '/src/test/'
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:677:        security.updateSchema((DatabaseInternal) server.getDatabase(databaseName));
engine/src/main/java/com/arcadedb/database/LocalDatabase.java:2966:          security.updateSchema(this);
engine/src/main/java/com/arcadedb/schema/LocalSchema.java:3146:      security.updateSchema(database);
```

```
$ grep -rn "groupRepository\.\(save\|applyReplicated\)\|replicateSecurityGroups(" --include='*.java' . | grep -v '/src/test/'
ha-raft/.../RaftTransactionBroker.java:449:  public void replicateSecurityGroups(final String groupsJson) {
ha-raft/.../RaftHAPlugin.java:219, :224
server/.../HAServerPlugin.java:323:  default void replicateSecurityGroups(final String groupsJson) {
server/.../ServerSecurity.java:146  (saveInError)
server/.../ServerSecurity.java:917  (saveGroups() - no production caller)
server/.../ServerSecurity.java:992  (persistGroups, from local saveGroup/deleteGroup)
server/.../ServerSecurity.java:1024 (saveGroupClusterWide)
server/.../ServerSecurity.java:1043 (deleteGroupClusterWide)
server/.../ServerSecurity.java:1087 (applyReplicatedGroups)
server/.../ServerSecurity.java:1158 (seedGroupsClusterWide)
```

```
$ grep -rn "controlPlane\.\(saveGroup\|deleteGroup\)" --include='*.java' . | grep -v '/src/test/'
server/.../http/handler/DeleteGroupHandler.java:50
server/.../http/handler/PostGroupHandler.java:53
grpcw/.../ArcadeDbGrpcAdminService.java:441, :453
```

```
$ grep -rn "onReload\|reloadCallback" --include='*.java' . | grep -v '/src/test/'
server/.../SecurityGroupFileRepository.java:51, :188, :189, :300, :301
server/.../ServerSecurity.java:110
```

### Entry-point coverage table

| # | Entry point | Covered by fix? | Covered by a test? |
|---|---|---|---|
| 1 | Peer: `ArcadeStateMachine.applySecurityGroupsEntry` -> `ServerSecurity.applyReplicatedGroups` (grant narrowed) | **yes** - schedules the refresh on a dedicated single worker | yes - `aReplicatedRevocationReachesTheCachedPrincipalWithoutTheReloadTick` |
| 2 | Same, grant **widened** (so the test cannot pass by denying everything) | **yes** - same call | yes - `aReplicatedGrantReachesTheCachedPrincipalWithoutTheReloadTick` |
| 3 | Same, but the local file write FAILED: the document is in force in memory and `ReplicatedSecurityConfigPersistenceException` is thrown | **yes** - the refresh is scheduled *before* the throw | yes - `aRefreshIsStillScheduledWhenTheReplicatedWriteFails` |
| 4 | Joining peer seed: `PostAddPeerHandler` -> `seedGroupsClusterWide` -> `replicateSecurityGroups` -> apply | **yes** | argued: byte-for-byte the same `applyReplicatedGroups` call as rows 1-3; no separate test |
| 5 | Serving node: `ServerControlPlane.saveGroup`/`deleteGroup` (HTTP `PostGroupHandler`/`DeleteGroupHandler`, gRPC `ArcadeDbGrpcAdminService`) | pre-existing `refreshPermissionsOf`, unchanged | existing `Issue7373*` / control-plane tests |
| 6 | Non-HA single node: `saveGroupClusterWide`/`deleteGroupClusterWide` fall back to local `saveGroup`/`deleteGroup`, refreshed by row 5's caller | pre-existing, unchanged | existing tests |
| 7 | Hand-edited `server-groups.json` -> watcher -> `reloadCallback` | pre-existing; now calls the same extracted `refreshAllDatabasePermissions()` | yes - `theReloadWatcherBodyRefreshesEveryOpenDatabase` |
| 8 | Peer: `applyReplicatedUsers` | argued: it builds **new** `ServerSecurityUser` instances and swaps the map, and `databaseCache` is a `private final` per-instance field (`ServerSecurityUser:38`), so no derived state survives the swap |
| 9 | Peer: `applyReplicatedApiTokens` | argued: a token principal carries a `syntheticGroupConfig`, and `ServerSecurityUser:156` / `:213` take that branch *instead of* the file group configuration, so a group document cannot change its answer |
| 10 | `ServerSecurity.saveGroups()` (public, writes the whole document with no refresh) | argued: sweep C shows the definition and **no** production call site |
| 11 | The refresh sweep itself, when one database in it cannot be handed over (dropped under the iteration, or refused with `DatabaseNotAvailableException` because its directory still carries the interrupted-snapshot marker) | **yes** - guarded per database rather than once around the loop | yes - `oneUnavailableDatabaseDoesNotStopTheOthersFromBeingRefreshed` (added by the adversarial pass) |

### Residual risk

1. **Coalescing drops a refresh by design.** The worker has a one-deep queue: while one refresh is
   queued, a second submission is dropped, because the queued one has not read the group document
   yet and will therefore see the newest one. A drop while a refresh is *running* cannot happen -
   that slot is free. This is correct but it is a queue, not a proof, and it is the thing to look
   at first if convergence ever looks slow under a burst of group edits.
2. **The refresh is still asynchronous.** The window shrinks from up to `reloadEvery` (5 s by
   default) to the scheduling latency of a single idle worker - microseconds to milliseconds - but
   it is not zero, and it cannot be: the apply thread may not block. A caller that needs a
   synchronous guarantee has to use the control plane on the node it is talking to, which already
   refreshes inline.
3. **The file watcher remains the safety net** and is deliberately not removed: if the executor is
   saturated, shut down, or the task throws, the peer still converges on the reload tick exactly as
   it did before this change.
4. **The sweep can reopen a closed database**, because it resolves names with `allowLoad = true`. Pre-existing
   on the watcher path this method was extracted from; see finding 2 of the adversarial pass below.
5. **Not covered, and pre-existing:** `SecurityGroupFileRepository`'s watcher timer is only started
   inside `load()`. `applyReplicated()` publishes the document without going through `load()`, so
   on a node whose *first ever* group access is a replicated apply, `getGroups()` never falls into
   the lazy-init branch and the watcher is never scheduled at all. Filed as a follow-up (see
   below) - it is a different defect (the safety net not existing) from the one this PR fixes
   (the safety net being the only mechanism). Filed as **#7545**.

## Change

`server/src/main/java/com/arcadedb/server/security/ServerSecurity.java`

1. **`refreshAllDatabasePermissions()`** - the loop that was inlined in the group file's `onReload` lambda,
   extracted as a public method so the replicated path runs the same body rather than a second hand-written
   copy of it. It reads the *current* document rather than a snapshot handed to it, which is what lets a
   queued refresh stand in for the ones coalesced behind it.
2. **`permissionsRefreshExecutor`** - one daemon worker, `core 0 / max 1 / 30 s keep-alive`, one-deep
   `ArrayBlockingQueue`, shaped after `ArcadeStateMachine`'s `snapshotInstallExecutor` (and, per the
   `engine-concurrency` rule, explicitly not the JDK common `ForkJoinPool`). Its rejection handler **drops**
   the task and logs at `FINE`, which is coalescing rather than loss: a drop can only happen while another
   refresh is queued and has not started, and the new document is published before the submit, so the queued
   task reads a document at least as new as the dropped one's.
3. **`applyReplicatedGroups`** now calls `scheduleDatabasePermissionsRefresh()` immediately after
   `groupRepository.applyReplicated(root)` and **before** the persistence failure is reported. That ordering is
   the point of row 3: `applyReplicated` publishes in memory first, so the node authorizes against the new
   document whether or not the write succeeded, and the case that needs the refresh most - a narrowed
   permission on a node whose config volume is full - must not be the one that skips it.
4. `stopService()` shuts the worker down.

`server/src/test/java/com/arcadedb/server/security/Issue7373ReplicatedGroupAuthorizationTest.java` - **comments
only**, no assertion or logic changed. Its javadoc described the reload-tick lag as the current behaviour and
named #7510 as the issue that would close it; that text is now false. The two assertions it makes still hold and
still mean something: the apply itself stays non-blocking, and the mocked server there reports no open database,
so the new asynchronous refresh has nothing to walk and cannot race them.

## Finding ledger

- [x] 1. A replicated group change reaches a peer's cached permissions only on the security reload tick -
      **fixed**, off-thread refresh scheduled from `applyReplicatedGroups` (coverage rows 1-4).

## Test results

New: `server/src/test/java/com/arcadedb/server/security/Issue7510ReplicatedGroupRefreshTest.java`, **5 tests** -
four written against the coverage table before the fix, plus the per-database-guard test added afterwards by the
adversarial pass (finding 1 below).

Before the fix (only the extraction in place, no scheduling):

```
Tests run: 4, Failures: 3, Errors: 0, Skipped: 0
  aRefreshIsStillScheduledWhenTheReplicatedWriteFails:169 [the revocation is enforced on the peer even though persisting it failed]
  aReplicatedGrantReachesTheCachedPrincipalWithoutTheReloadTick:137 [the widened grant reaches the cached principal without waiting for the reload tick]
  aReplicatedRevocationReachesTheCachedPrincipalWithoutTheReloadTick:121 [the principal already connected to the peer is denied without waiting for the reload tick]
```

Each of the three failed by exhausting the full 10 s outcome wait (30.99 s for the class), i.e. by the peer not
converging - which is the defect, reproduced through the entry point the Raft state machine actually uses.

After the fix: `Tests run: 4, Failures: 0, Errors: 0` in 0.958 s - the class is now faster than one reload
interval, which is the change stated as a number. With the fifth test added it is `Tests run: 5, Failures: 0` in
0.577 s, and that fifth test was separately proved able to fail by reverting its guard
(`Tests run: 5, Failures: 1`).

Regression runs (all with `-Dmaven.repo.local=$WORKTREE/.m2repo`, isolated from the parallel agents):

| Command | Result |
|---|---|
| `mvn -o -pl server -am install -DskipTests` | BUILD SUCCESS |
| `mvn -o -pl ha-raft -am install -DskipTests` | BUILD SUCCESS |
| `mvn -o -pl server test -Dtest='Issue7373*,ServerSecurity*,SecurityGroup*,ServerControlPlane*,*Group*Test'` | 66 tests, 0 failures |
| `mvn -o -pl ha-raft test -Dtest='Issue7137…,Issue7227…,Issue7252…,Issue7373SecurityConfig*'` | 15 tests, 0 failures |
| `mvn -o -pl server test -DexcludedGroups=benchmark,vector,slow` | 1099 tests, 13 failures + 6 errors - **all environmental, see below** |

### The 19 failures in the full server run are a port collision, not this change

`lsof -nP -iTCP:2480 -sTCP:LISTEN` reports two foreign `java` processes holding 2480 (other agents' servers
running concurrently on this machine). The failing classes are `ClusterInternalAuthTest`,
`PostClusterAuthSessionHandlerTest`, `AutoCommitParameterTest`, `PostCommandHandlerDecodeTest`,
`HttpBodySizeLimitTest`, `Issue6220TruncateHttpDefaultTest`, `Issue5675CreateIndexIfNotExistsHttpTest` - every one
of them an HTTP test against `127.0.0.1:2480`, failing with `401` where it expected `200`/`204`/`400`/`404`, or
with `Schema Type with name 'TestDoc' was not found`. That is the signature `CLAUDE.md` documents for this exact
situation. None of them replicate a group, reload the group file, or construct a Raft entry, and the whole
`com.arcadedb.server.security` package - including the four new tests and every pre-existing group test - is green
in that same run:

```
Issue7510ReplicatedGroupRefreshTest        Tests run: 4,  Failures: 0, Errors: 0
Issue7373ReplicatedGroupAuthorizationTest  Tests run: 2,  Failures: 0, Errors: 0
Issue7373ClusterWideGroupsAndTokensTest    Tests run: 24, Failures: 0, Errors: 0
Issue6806GroupPermissionRefreshTest        Tests run: 7,  Failures: 0, Errors: 0
SecurityGroupFileRepositoryTest            Tests run: 3,  Failures: 0, Errors: 0
ServerSecurityIT                           Tests run: 6,  Failures: 0, Errors: 0
```

## Reachability

- `applyReplicatedGroups` is called on a live path: `ArcadeStateMachine.applySecurityGroupsEntry:3692`, the
  `SECURITY_GROUPS_ENTRY` arm of the Raft apply switch (`:933`).
- `ServerSecurity` is constructed by the server itself - `ArcadeDBServer.java:399`,
  `security = new ServerSecurity(this, configuration, serverRootPath + "/config")` - the only production
  construction site, so the executor field exists on every running node.
- No configuration flag gates the refresh: it is unconditional on the apply path. `arcadedb.server.reloadEvery`
  now only sizes the safety net behind it.
- Cost when nothing replicates: zero threads. The pool has `corePoolSize` 0, so a server that never applies a
  group entry never starts the worker.

## Impact

- A peer now enforces a replicated group change within the scheduling latency of an idle worker instead of up to
  `arcadedb.server.reloadEvery` ms (default 5000).
- No change to the node that served the request (`ServerControlPlane.refreshPermissionsOf`, still inline), to the
  non-HA path, or to the hand-edited-file path beyond both now calling the same extracted method.
- The Raft apply thread does strictly less work than a blocking refresh would have cost it, and exactly one queue
  offer more than before.

## Recommendations

- Watch for the `FINE` "coalesced into it" line if convergence ever looks slow under a burst of group edits; a
  sustained stream of them is the only way to keep the one-deep queue occupied.
- #7545 is the remaining gap in the same area: on a node whose first group access is a replicated apply, the file
  watcher is never scheduled at all, so the safety net this change deliberately keeps does not exist there.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not seen the author's reasoning. **The `Task` tool is
disabled in this session**, so no such subagent could be spawned; the pass was run by the author against the diff
instead, which is weaker - it is the same head that wrote the patch - and is recorded as such. Three findings,
each verified against the tree:

1. **Real, in scope - fixed here.** `refreshAllDatabasePermissions()` had no per-database guard, so one database
   that `ArcadeDBServer.getDatabase()` refuses aborted the sweep and left every database after it in the
   iteration waiting for the reload tick - this issue, reintroduced for those databases by the very method that
   removes it. The refusal is not hypothetical:
   `ArcadeDBServer.java:1422-1426` throws `DatabaseNotAvailableException` for a directory still carrying the
   interrupted-snapshot marker, and a peer mid-snapshot-install is exactly a node that also receives replicated
   group entries. Fixed by moving the guard inside the loop, and pinned by
   `oneUnavailableDatabaseDoesNotStopTheOthersFromBeingRefreshed`, which iterates the broken name FIRST and fails
   (verified by reverting the guard: `Tests run: 5, Failures: 1`) without it.

2. **Real, pre-existing, not worsened in kind - argued, not filed.** The sweep resolves each name with
   `server.getDatabase(name)`, i.e. `allowLoad = true`, so an entry that is present in the map but closed is
   **reopened** as a side effect of a permission refresh (`ArcadeDBServer.java:1411-1475`). That is undesirable,
   but it is not introduced here: `applyReplicated()` writes the group file, the watcher's `lastModified` check
   therefore fires, and the watcher has always run this same loop. The patch makes it happen sooner, not for the
   first time. Changing `allowLoad` would alter the hand-edited-file path too and is a different change;
   recorded in residual risk instead of silently altered.

3. **Not real.** Suspected deadlock: the refresh worker taking the `ServerSecurity` monitor while
   `saveGroupClusterWide` holds it across the Raft round trip. It cannot. The worker's path is
   `refreshAllDatabasePermissions` -> `updateSchema` -> `getDatabaseGroupsConfiguration`, and
   `ServerSecurity.java:1549` declares that method `protected JSONObject getDatabaseGroupsConfiguration(...)` -
   not `synchronized` - while `updateSchema` (`:587`) is not synchronized either. The worker never contends for
   that monitor, and the apply thread never waits for the worker.
