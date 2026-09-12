# #7510 - HA: a replicated group change reaches a peer's cached permissions only on the security reload tick

Branch: `fix/7510-replicated-group-permission-refresh`

## Problem

#7373 made a group change replicate, so `server-groups.json` converges on every node immediately. The state
*derived* from that document does not: each open `DatabaseInternal` has a per-user
`ServerSecurityDatabaseUser` cache, and only `ServerSecurity.updateSchema` re-derives it.

- On the node that served the request, `ServerControlPlane.saveGroup` / `deleteGroup` call
  `refreshPermissionsOf(database)` synchronously, so the 200 means "enforced here".
- On a peer, `ServerSecurity.applyReplicatedGroups` deliberately does not: it runs on the Raft
  state-machine apply thread, which must never block. The peer picks the change up through
  `SecurityGroupFileRepository`'s file watcher, up to `arcadedb.server.security.reloadEvery` ms later.

For a **narrowed** permission that is a security lag: a database already open on the peer keeps granting the
old, wider access for the length of the reload interval, while `GET /server/groups` on that same peer already
reports the new definition. Invisible from every surface an operator can check.

## Root cause

`applyReplicatedGroups` installs the document (`SecurityGroupFileRepository.applyReplicated`) and returns.
Nothing on that path re-derives the cached permissions, and the apply thread may not do the work itself.

## Invariant the fix establishes

> Once a `SECURITY_GROUPS_ENTRY` has been applied on a node, the cached per-database permissions of the
> principals already connected to that node are re-derived from the newly installed document without waiting
> for the `arcadedb.server.security.reloadEvery` tick - and the re-derivation never runs on the Raft
> state-machine apply thread.

## Completeness

### Commands run

```
$ grep -rn --include='*.java' 'applyReplicatedGroups(' --exclude-dir=test . | grep '/src/main/'
ha-raft/src/main/java/com/arcadedb/server/ha/raft/ArcadeStateMachine.java:3692:      server.getSecurity().applyReplicatedGroups(payload);
server/src/main/java/com/arcadedb/server/security/ServerSecurity.java:1067:  public void applyReplicatedGroups(final String groupsJson) {
```

One production caller: `ArcadeStateMachine.applySecurityGroupsEntry`. Every replicated group mutation -
`saveGroupClusterWide`, `deleteGroupClusterWide` and the joining-peer `seedGroupsClusterWide` - funnels
through `HAServerPlugin.replicateSecurityGroups` into that single entry type, so one fix covers all three
producers.

```
$ grep -rn --include='*.java' -e 'getSecurity()\.saveGroup' -e 'getSecurity()\.deleteGroup' \
      -e 'security\.saveGroup' -e 'security\.deleteGroup' . | grep '/src/main/'
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:643:    server.getSecurity().saveGroupClusterWide(database, name, normalized);
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:663:    if (!server.getSecurity().deleteGroupClusterWide(database, name))
```

Both transports (HTTP `PostGroupHandler`/`DeleteGroupHandler`, gRPC `ArcadeDbGrpcAdminService`) reach the
group store only through `ServerControlPlane`. No `src/main` caller of the node-local `saveGroup`/
`deleteGroup` exists outside `ServerSecurity` itself.

```
$ grep -rn --include='*.java' '\.updateSchema(' */src/main/java
engine/src/main/java/com/arcadedb/database/LocalDatabase.java:2966:          security.updateSchema(this);
engine/src/main/java/com/arcadedb/schema/LocalSchema.java:3146:      security.updateSchema(database);
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:677:        security.updateSchema((DatabaseInternal) server.getDatabase(databaseName));
```

Plus the `onReload` lambda in the `ServerSecurity` constructor (line 112), which the grep above misses because
the call is unqualified. Those are every refresher of the cache.

```
$ grep -rn --include='*.java' 'applyReplicated[A-Za-z]*(' ha-raft/src/main/java server/src/main/java
... ArcadeStateMachine.java:3648:      server.getSecurity().applyReplicatedUsers(payload);
... ArcadeStateMachine.java:3692:      server.getSecurity().applyReplicatedGroups(payload);
... ArcadeStateMachine.java:3723:      server.getSecurity().applyReplicatedApiTokens(payload);
```

The two siblings of the same shape are argued below rather than changed.

### Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `applySecurityGroupsEntry` -> `applyReplicatedGroups`, replicated **save** (narrowing) | yes | yes - `aReplicatedRevocationReachesTheCachedPrincipalWithoutTheReloadTick` |
| `applySecurityGroupsEntry` -> `applyReplicatedGroups`, replicated **save** (widening) | yes | yes - `aReplicatedGrantReachesTheCachedPrincipalWithoutTheReloadTick` |
| `applySecurityGroupsEntry` -> `applyReplicatedGroups`, replicated **delete** (`deleteGroupClusterWide`) | yes | yes - `aReplicatedGroupDeletionReachesTheCachedPrincipalWithoutTheReloadTick` |
| `applySecurityGroupsEntry` -> `applyReplicatedGroups`, joining-peer **seed** (`seedGroupsClusterWide`) | yes | yes - `aSeededGroupDocumentRefreshesTheJoiningPeer` |
| `applyReplicatedGroups` on the **persist-failure** path (document in force, method throws) | yes | yes - `aRefreshIsScheduledEvenWhenTheReplicatedDocumentCouldNotBePersisted` |
| the refresh must NOT run on the Raft apply thread | yes | yes - `theRefreshRunsOffTheCallingApplyThread` |
| a burst of entries must not lose the last document | yes | yes - `aBurstOfRepliesConvergesOnTheLastDocument` |
| `ServerControlPlane.saveGroup` / `deleteGroup` on the serving node | pre-existing (`refreshPermissionsOf`), unchanged | pre-existing `Issue6806GroupPermissionRefreshIT` |
| `ServerSecurity.saveGroup` / `deleteGroup` called directly (non-HA fallback of `saveGroupClusterWide`) | argued | - |
| `applyReplicatedUsers` | argued | - |
| `applyReplicatedApiTokens` | argued | - |
| `SecurityGroupFileRepository` watcher tick (hand-edited file) | argued - unchanged, now the fallback | pre-existing |

### Arguments (with evidence)

- **`ServerSecurity.saveGroup`/`deleteGroup` direct calls.** The grep above shows no `src/main` caller outside
  `ServerSecurity`. The only reachable one is `saveGroupClusterWide`/`deleteGroupClusterWide`'s non-HA
  fallback, and both are entered from `ServerControlPlane`, which calls `refreshPermissionsOf` on the next
  line. So the single-node path is already refreshed; there is nothing async about it.
- **`applyReplicatedUsers`.** It rebuilds the whole `users` map from the payload -
  `newUsers.put(name, new ServerSecurityUser(server, userJson))` for every entry - and publishes it in one
  reference swap. Every `ServerSecurityUser` in the new map is a fresh object whose `databaseCache` is empty,
  so its permissions are derived from the *current* group document the first time it is used. There is no
  stale derived state for a refresh to fix.
- **`applyReplicatedApiTokens`.** An API-token principal is not in the `users` map at all:
  `authenticateByApiToken` builds a `ServerSecurityUser` per request and gives it a synthetic group config
  taken from the token document (`withSyntheticGroupConfig`), which `refreshDatabaseConfiguration` explicitly
  never widens with the file groups. So a replicated token document is read fresh on every request, and
  `updateSchema` could not reach such a principal even if it ran.
- **The watcher tick.** Untouched. `SecurityGroupFileRepository.applyReplicated` still writes the file without
  updating `fileLastUpdated`, so the watcher still fires and still refreshes. It is now a redundant fallback
  behind the immediate refresh rather than the only mechanism, and it remains the path a hand-edited file
  takes.

### Reachability

`applyReplicatedGroups` is called from `ArcadeStateMachine.applySecurityGroupsEntry`, which the state machine
dispatches for `RaftLogEntryType.SECURITY_GROUPS_ENTRY` (`ArcadeStateMachine:933`). No flag gates it. The new
executor is a field of `ServerSecurity`, which the server constructs unconditionally, and the refresh is
submitted from the apply itself - no configuration has to be turned on.

## Fix

`ServerSecurity` gains a dedicated single-worker executor (`arcadedb-security-permission-refresh`) and a
coalescing `scheduleReplicatedPermissionRefresh()`, which `applyReplicatedGroups` calls right after the
document is published - including on the path that then throws the persistence failure, because the document
is in force from that moment either way.

- **Off the apply thread.** The submit is non-blocking, so the invariant at the head of
  `applyReplicatedGroups` ("never blocks, never takes the monitor") still holds. No caller-runs policy: running
  the sweep on the caller is exactly what the hand-off exists to prevent, so the executor aborts and logs, and
  the watcher tick remains the fallback.
- **Coalescing.** An `AtomicBoolean` keeps at most one refresh queued. The flag is cleared at the *start* of the
  sweep, not at the end, so a document applied while a sweep is running always schedules another one. Worst
  case is one redundant, idempotent sweep; the case that must not happen - the last document never being swept -
  cannot.
- **Not a new JVM-wide pool.** One worker, `corePoolSize` 0 (no thread until the first group change), 30 s
  keep-alive, a 4-deep queue that coalescing keeps at a depth of at most 1. It follows
  `ArcadeStateMachine.snapshotInstallExecutor`, the existing precedent for "get off the Ratis thread", rather
  than `DedicatedThreadPool`, which is for JVM-wide shared pools with `PoolMetrics` bindings - this one is per
  server instance (HA tests run several servers in one JVM) and idle in every steady state.
- The sweep itself is the body the `onReload` watcher callback already ran, extracted to
  `refreshOpenDatabasePermissions()` and now shared by both, with a per-database `try/catch` so a database
  dropped mid-sweep cannot kill the worker or skip the rest.

## Residual risk

- The refresh is asynchronous **by design**: between the Raft apply and the sweep there is a window of
  milliseconds in which a peer still answers from the old document. It cannot be closed without blocking the
  apply thread. It replaces a window of `arcadedb.server.security.reloadEvery` (default measured in seconds).
- If the executor rejects (only reachable after `stopService`), the refresh falls back to the watcher tick and
  says so at WARNING.
- `ServerControlPlane.refreshPermissionsOf` on the serving node is left synchronous, so the node that answered
  the request still enforces before it replies. That means one redundant sweep per change on that node.

## Changes

- `server/src/main/java/com/arcadedb/server/security/ServerSecurity.java`
  - new `permissionRefreshExecutor` (single worker, `corePoolSize` 0, 30 s keep-alive, 4-deep queue,
    `AbortPolicy`, daemon thread `arcadedb-security-permission-refresh`) and `permissionRefreshPending` flag;
  - new `refreshOpenDatabasePermissions()` - the sweep the `onReload` watcher callback used to inline, now
    shared, with a per-database `try/catch`;
  - new `scheduleReplicatedPermissionRefresh()` - the lossless coalescing hand-off;
  - `applyReplicatedGroups` calls it right after the document is published, before the persistence failure is
    reported;
  - `stopService()` shuts the executor down;
  - the javadoc that said the caches are "NOT refreshed from here" now says what actually happens.
- `server/src/test/java/com/arcadedb/server/security/Issue7510ReplicatedGroupPermissionRefreshTest.java` - new.

## Test results

TDD: with `scheduleReplicatedPermissionRefresh()` removed from `applyReplicatedGroups`, all 7 new tests fail
(each on its own `await` assertion, `ConditionTimeout`). With the fix:

```
$ mvn -o -pl server -Dtest=Issue7510ReplicatedGroupPermissionRefreshTest test
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o -pl server -Dtest='Issue7510*,Issue7373*,Issue7137*,Issue6806*Test,SecurityGroup*Test,
      ServerSecurity*Test,ApiTokenConfigurationTest,DatabaseUserContextTest,RootPasswordFromFileTest,
      ConcurrentSaltCacheTest,SecurityUserFileRepositoryTest' test
Tests run: 89, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o -pl ha-raft -Dtest='*Security*Test' test
Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
```

`Issue7373ReplicatedGroupAuthorizationTest` is the one to watch: it deliberately pins that the apply does not
refresh synchronously, and it still passes - the hand-off is asynchronous, so the apply still returns before
the sweep, which is what that test asserts.

**Not run locally:** the server integration tests (`Issue6806GroupPermissionRefreshIT`,
`Issue5269FileAccessRefreshIT`, `ServerSecurityIT`). Port 2480 was already held by another process on this
machine (`lsof -nP -iTCP:2480 -sTCP:LISTEN` returned two live listeners), and server ITs bind fixed ports, so a
local run would report authentication errors rather than anything about this change. They run in CI.

## Adversarial pass

The orchestrator's Phase 1.5 asks for an independent subagent, deliberately kept ignorant of the author's
reasoning. **The `Task` tool is disabled in this session**, so that independence could not be had; the pass below
was run by the same agent that wrote the patch, against the issue body and the diff, and is weaker for it. Each
finding was checked against the tree.

1. **Does the executor survive a server restart?** If `ServerSecurity` outlived `stop()`/`start()`, the
   `shutdownNow()` in `stopService()` would leave a permanently dead executor and the refresh would silently
   never happen again - a worse bug than the one being fixed. **Not real:**
   `ArcadeDBServer.startInternal()` does `security = new ServerSecurity(this, configuration, ...)`
   (`ArcadeDBServer.java:399`), so every start builds a fresh instance and a fresh executor.

2. **Is iterating `server.getDatabaseNames()` from a background thread safe?** **Not a new hazard:** the backing
   field is `private final ConcurrentMap<String, ServerDatabase> databases = new ConcurrentHashMap<>()`
   (`ArcadeDBServer.java:186`), so the returned key set iterates weakly-consistently. The watcher callback has
   iterated it from a `Timer` thread all along.

3. **A database CLOSED on the peer when the entry is applied is skipped by the sweep, and
   `ServerSecurityUser.databaseCache` is keyed by name and survives a close** - so a reopen could serve the
   permissions derived from the previous document, forever. **Not real:** `LocalDatabase.open()` calls
   `security.updateSchema(this)` on every open (`LocalDatabase.java:2965-2966`), so a reopened database
   re-derives from the document in force at that moment. Worth stating, because the row is not covered by the
   sweep and the reason it is safe is somewhere else entirely.

4. **`Issue7373ReplicatedGroupAuthorizationTest` still passes, but its comments now describe the world before
   this patch** - "on a peer the group file's watcher does, on the reloadEvery tick", "the lag issue #7510
   tracks". Left alone, that is a comment asserting an invariant the code no longer holds, which the
   verified-claims rule says is worse than no comment. **Real, fixed here, comments only.** The assertions and
   every line of test logic are untouched - `git diff` on that file shows no changed line that is not a comment
   or javadoc - and the prose now says what actually pins the test: the apply still does not walk the databases
   on the calling thread, and this fixture's mocked server reports no open databases, so the scheduled sweep has
   nothing to walk and cannot interfere.

5. **Nothing reports that the refresh happened.** The success path is silent, the two failure paths log at
   WARNING, and the executor is not in `PoolMetrics`. An operator narrowing a permission still cannot see when
   the other nodes started enforcing it, which was half of what #7510 complained about. **Real, out of scope,
   filed as #7529** - this PR closes the lag; being able to observe it is a separate change.

6. **Is the 20 s `await` in the new tests a wall-clock latency assertion?** It is a tripwire between "refreshed
   by the apply" (milliseconds) and "waiting for the hourly `reloadEvery`", with three orders of magnitude of
   headroom, and widening it can only make a run greener. That reasoning is now in the test's class javadoc so
   the next person does not read it as a latency budget.

### Behaviour change worth flagging to a reviewer

Extracting the watcher's inline sweep into `refreshOpenDatabasePermissions()` added a per-database `try/catch`.
On the watcher path a failing database used to abandon the rest of the sweep and surface as a SEVERE
"Error on reloading file ... after was changed" from `SecurityGroupFileRepository.load()`'s timer; it is now a
WARNING naming the database, and the remaining databases are still refreshed. That is deliberate - on the
refresh worker an escaping exception would also take out the sweep the next group change needs - but it is a
change to an existing path, not only to the new one.

## Known gaps

- **#7529** - no metric, gauge or success-path signal for the refresh; an operator cannot observe convergence.
