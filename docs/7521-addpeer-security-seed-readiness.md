# Issue #7521 - HA addPeer: a peer whose security seed failed is admitted and serves traffic with its own stale credentials

Branch: `fix/7521-addpeer-security-seed-readiness`

## The report

`PostAddPeerHandler` admits the peer first and seeds the three security documents afterwards, best-effort. A
failed seed was logged and reported in a `warning` field of an HTTP **200**, and nothing stopped the new peer
from serving requests. It falls back to whatever its own `config/` directory holds - for a node re-added after
time out of the cluster, a user dropped since, a group narrowed since, or **a token revoked since** still
authenticates and authorizes on that one node until the next cluster-wide change of that kind happens to occur.
Behind a load balancer that reads as an intermittent success against a credential the operator believes is dead.

The seeds are submitted as Raft entries, so the usual failure is "no quorum right now" - the same condition that
makes `addPeer` interesting in the first place.

## Root cause

`ServerSecurity.seedSecurityStateClusterWide()` made exactly **one** attempt per document.
`RaftTransactionBroker.replicateSecurity*` is `groupCommitter.submitAndWait(...)`, so the call blocks until the
entry commits and its dominant failure mode is a momentarily absent quorum. A single attempt against a transient
failure is the wrong shape: retrying costs an admin call a few seconds, not retrying leaves a committed cluster
member running on its own credentials indefinitely.

The second half is reporting. `PostAddPeerHandler` returned 200 with a `warning` field. An operator's join
automation reads the status code.

## Completeness

### The invariant

> Every path on which one node admits another to the cluster seeds **all three** security documents - users,
> groups, API tokens - each read under the security monitor and retried within a bounded budget; and when a
> document still has not committed, the caller learns it through the operation's own failure channel rather than
> through a flat success.

### Enumerating the ways to violate it

Every production caller that admits a peer:

```
$ grep -rn "\.addPeer(\|\.connectCluster(" --include="*.java" */src/main | grep -v RaftClusterManager.java
grpc-client/.../RemoteGrpcServer.java:797:    call("connect cluster", stub -> stub.connectCluster(
grpcw/.../ArcadeDbGrpcAdminService.java:935:      controlPlane.connectCluster(req.getServerAddress());
ha-raft/.../RaftHAPlugin.java:483:    raft.addPeer(target.peer(), target.name());
ha-raft/.../RaftHAPlugin.java:501:    raftHAServer.addPeer(peerId, address, name);
ha-raft/.../RaftHAServer.java:2917:    clusterManager.addPeer(peerId, address, name);
ha-raft/.../RaftHAServer.java:2922:    clusterManager.addPeer(newPeer, name);
ha-raft/.../PostAddPeerHandler.java:62:    raftHAServer.addPeer(peerId, address, name.isEmpty() ? null : name);
server/.../ServerControlPlane.java:208:      ha.connectCluster(serverAddress);
server/.../http/handler/PostServerCommandHandler.java:206:    controlPlane.connectCluster(serverAddress);
```

Every production caller of the seed:

```
$ grep -rn "replicateSecurityUsers(\|replicateSecurityGroups(\|replicateSecurityApiTokens(\|seedSecurityStateClusterWide(" --include="*.java" */src/main
...
ha-raft/.../PostAddPeerHandler.java:74:    final List<String> failedSeeds = ...getSecurity().seedSecurityStateClusterWide();
server/.../ServerControlPlane.java:226:      ha.replicateSecurityUsers(server.getSecurity().getUsersJsonPayload());
server/.../ServerSecurity.java:422,448,475,1024,1043,1187,1208   (the mutation paths, not admissions)
```

Two things fell out of this that the report did not name:

1. **`ServerControlPlane.connectCluster` seeded the users document only** - it never grew the groups and
   API-token half #7373 gave the add-peer route - **and it read the payload outside the security monitor**,
   calling `getUsersJsonPayload()` directly. That method's javadoc has always said the caller must hold the
   monitor; this is the exact window #7373 closed on the other route, still open on this one.

2. **The Kubernetes auto-join seeds nothing at all**, because it is a self-join with no admitting node:

```
$ grep -n "replicateSecurity\|seedSecurity\|addPeer" ha-raft/src/main/java/com/arcadedb/server/ha/raft/KubernetesAutoJoin.java
(no matches)
```

### Entry-point coverage table

| Entry point | Covered by the fix? | Covered by a test? |
|---|---|---|
| `POST /api/v1/cluster/peer` -> `PostAddPeerHandler` | yes - bounded retry, and a residual failure is 503 with `failedSeeds`, not a 200 with a `warning` | yes - `Issue7521AddPeerSeedFailureIsReportedTest` (5 methods) |
| `connect cluster` (HTTP `POST /api/v1/server`, gRPC `ConnectCluster`) -> `ServerControlPlane.connectCluster` | yes - now seeds all three documents through `seedSecurityStateClusterWide`, under the monitor, with the same retry | yes - `Issue7521SecuritySeedRetryTest`, the four `connectCluster*` methods |
| `ServerSecurity.seedSecurityStateClusterWide` itself (the shared mechanism both routes call) | yes - `seedSecurityStateClusterWide(long)` retries only the documents that failed, re-reading each under the monitor, with capped exponential backoff and interrupt-aware sleeps | yes - `Issue7521SecuritySeedRetryTest`, the seven mechanism methods |
| Kubernetes StatefulSet scale-up -> `KubernetesAutoJoin` self-join | **no - filed as #7531** | no |
| Readiness gate on security convergence (the window between a peer catching up and the seed being submitted; and `connect cluster` having no in-band failure channel) | **no - filed as #7532** | no |
| `RemoteGrpcServer.connectCluster`, `ArcadeDbGrpcAdminService`, `PostServerCommandHandler` | argued - these are transports that call `ServerControlPlane.connectCluster`; they add no seeding of their own, so the row above covers them. Evidence: the grep in "Enumerating" shows `ha.replicateSecurity*` appears in neither | covered transitively |

### Why the readiness gate (the issue's second option) was not taken here

The issue offered two shapes and noted the second composes better with #7511. It was not taken because:

- it is a readiness-contract change, and it needs a deadlock answer - a node that joins and is never seeded
  (nobody mutates security; or #7531's self-join path, where no seed is ever issued) must not report NOT_READY
  forever, or a rolling restart stalls;
- the existing test `Issue7401ServerControlPlaneConnectClusterTest.aFailingUsersSeedDoesNotFailTheJoin` pins
  that a failed seed must not fail the join on that verb, and this workflow does not modify existing tests.

Both are written up in #7532 with the evidence, rather than left implicit.

### Residual risk

1. The seed is still submitted **after** the membership change, so a peer can catch up and report READY before
   the first seed attempt is made. The retry shortens the failure case; it cannot close that window. #7532.
2. `connect cluster` still cannot report a residual seed failure in-band - it returns `void` and an existing
   contract forbids failing the join - so on that verb the failure reaches the operator only as a SEVERE log
   line. #7532.
3. A pod that joins through the Kubernetes auto-join is never seeded by anyone. #7531.
4. The retry budget is wall-clock. An admission issued while the cluster has genuinely lost quorum for longer
   than `arcadedb.ha.securitySeedRetryTimeout` (default 3 s) still reports the residual failure - by design:
   holding an HTTP worker thread indefinitely is worse than answering 503 and letting the operator re-POST.

## The change

| File | What |
|---|---|
| `engine/.../GlobalConfiguration.java` | new `HA_SECURITY_SEED_RETRY_TIMEOUT` (`arcadedb.ha.securitySeedRetryTimeout`, `Long`, default `3000`). `0` restores the single best-effort attempt. |
| `server/.../security/ServerSecurity.java` | new `seedSecurityStateClusterWide(long retryBudgetMs)`. Retries only the documents that failed, re-reading each under the monitor, backoff 250 ms doubling to a 1000 ms cap and never past the deadline, interrupt restores the flag and returns. The no-argument form delegates with `0`, so its existing callers and their tests keep the single-attempt contract. |
| `ha-raft/.../PostAddPeerHandler.java` | uses the retrying form with the configured budget; logs SEVERE on a residual failure; response building extracted to `addPeerResponse`, which answers **503** with `error` + a machine-readable `failedSeeds` array, still reporting `result` because the peer *is* a member. |
| `server/.../ServerControlPlane.java` | `connectCluster` now seeds through `seedSecurityStateClusterWide(budget)` instead of a bare `ha.replicateSecurityUsers(getUsersJsonPayload())` - all three documents, under the monitor, with the retry. Still does not fail the join; logs SEVERE. |
| `server/.../openapi/PluginApiSpec.java` | `POST /api/v1/cluster/peer` documents the 503 and what it means. |
| `studio/.../js/studio-cluster.js` | the add-peer `fail` handler refreshes the cluster view on a 503, because the peer *is* a member. |

### Reachability

- `PostAddPeerHandler` is constructed at `RaftHAPlugin:264` (`routes.addExactPath("/api/v1/cluster/peer", new PostAddPeerHandler(httpServer, this))`), so the changed code is on a live route.
- `ServerControlPlane.connectCluster` is reached from `PostServerCommandHandler:206` (HTTP) and `ArcadeDbGrpcAdminService:935` (gRPC).
- No flag gates either off: the new setting changes how long the retry runs, not whether the seed happens.

## Tests

New:

- `server/src/test/java/com/arcadedb/server/security/Issue7521SecuritySeedRetryTest.java` - 11 methods.
  Retry succeeds within the budget; a permanent failure is reported once the budget is spent and *was* retried;
  only the failing documents are retried; a retry re-reads rather than resubmitting the first payload (driven by
  having the failing submit revoke a token before it throws, so a stale resubmit would resurrect it); the
  no-argument form and a zero budget both stay single-attempt; an interrupt ends the retry with the flag
  restored; the three `connectCluster` parity/monitor/no-fail-the-join properties; and that a seed which cannot
  run at all still leaves the join standing.
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue7521AddPeerSeedFailureIsReportedTest.java` - 5 methods
  on `PostAddPeerHandler.addPeerResponse`: 200 when clean, 503 when not, the failing documents named in prose
  and as a list, the `error`/`detail` split, and the membership half still reported.

### Proof the tests can fail

Production behaviour reverted in place, tests re-run, then restored:

- `seedSecurityStateClusterWide(long)` deadline forced to `now + 0` and `connectCluster` put back to the
  users-only, outside-the-monitor seed: **8 of 10 failed**. The 2 that stayed green are
  `theNoArgumentFormStillMakesExactlyOneAttemptPerDocument` and `aZeroBudgetDisablesTheRetryRatherThanLooping
  Forever`, which pin a contract that holds before *and* after - correct.
- `addPeerResponse` forced back to 200: `anAdmissionWhoseSeedDidNotCommitIsNotReportedAsSuccess` failed, the
  other 3 stayed green (they assert body content, which the pre-fix shape also carried).

### Regression runs

All with `-Dmaven.repo.local=<worktree>/.m2repo`.

| Selection | Result |
|---|---|
| `server`: `com.arcadedb.server.security.*Test`, `...http.handler.openapi.*Test`, `com.arcadedb.server.*Test` | 333 run, 0 failures |
| `server`: `Issue7373...Test`, `Issue7401ServerControlPlaneConnectClusterTest`, `GetReadyHandlerHATest`, `Issue7521SecuritySeedRetryTest` | 49 run, 0 failures |
| `server`: `PluginApiSpecTest`, `RoutePathNormalizerTest` | 28 run, 0 failures |
| `ha-raft`: `HAConfigDefaultsTest`, `Issue7521AddPeerSeedFailureIsReportedTest`, `Issue7401JoinPriorityTest`, `ConfigValidationTest` | 21 run, 0 failures |
| `engine`: `Issue7124BooleanSettingStrictCoercionTest`, `Issue7163DatabaseScopeCallbackTest`, `GlobalConfigurationTest` | 29 run, 0 failures |

Not run, environmental: `DynamicMembershipTest` and the `*IT` classes in both modules could not bind their
ports - another agent on this machine holds 2434 and 2480 (`java.net.BindException: Address already in use` for
`0.0.0.0:2434`). Neither touches the seeding code.

One existing test got slower rather than red: `Issue7401ServerControlPlaneConnectClusterTest
.aFailingUsersSeedDoesNotFailTheJoin` now spends the default 3 s budget retrying a seed that is rigged never to
commit. That is the new behaviour being exercised, not a regression.

## Impact

- An operator's join automation now sees a 503 where it used to see a 200 with a field it did not read. That is
  the intended behaviour change and it is documented in the OpenAPI spec; re-POSTing the same peer is idempotent
  on the membership change and reissues the seed.
- A peer joined through `connect cluster` now receives the group document and the API-token store, which it
  never did before.
- `arcadedb.ha.securitySeedRetryTimeout=0` restores the previous single-attempt behaviour exactly.


## Adversarial pass

The skill asks for one `general-purpose` subagent, deliberately kept ignorant of the author's reasoning, to
write the follow-up issue it would file against this patch. **No `Task` tool exists in this environment**
(`ToolSearch` for it returns nothing), so the pass was run by the author against the diff instead. That is
weaker - the author has already been convinced - and it is recorded as such rather than claimed as the
isolated pass. It still produced three findings, all real, all fixed here:

1. **`connectCluster` could now fail the join it is forbidden to fail.** The old code had
   `server.getSecurity().getUsersJsonPayload()` *inside* a `try/catch (Exception)`; the rewrite put the
   `getSecurity()` dereference outside every catch. `seedSecurityStateClusterWide` collects a per-document
   failure rather than throwing, so the per-document case was covered - but anything around it (a server with
   no security store) would have escaped as a failed join, and by that point the peer is a committed member.
   Fixed: the seed call is wrapped, with a SEVERE log. Pinned by
   `connectClusterSurvivesASeedThatCannotRunAtAll`.

2. **The 503 body would have rendered as an unreadable Studio notification.**
   `studio-utils.js:globalNotifyError` uses `json.error` as the notification **title** and `json.detail` as its
   body. The first draft put the whole paragraph in `error`, so Studio would have shown a paragraph-length
   title over the placeholder "Error on execution of the command". Fixed: split into a one-line `error` and a
   `detail` carrying the remediation, which is the split `AbstractServerHttpHandler.error2json` uses
   everywhere else. Pinned by `theSummaryAndTheRemediationAreSeparateFields`.

3. **Studio would show the operator an error and a cluster view without the peer.**
   `studio-cluster.js`'s add-peer handler calls `updateCluster()` only in `.done()`. A 503 means the membership
   change *succeeded*, so the list is stale exactly when the operator most needs to see the new member. Fixed:
   the `fail` handler refreshes on a 503.

Checked and found **not** to be problems, with the evidence:

- **Enum ordinal.** `HA_SECURITY_SEED_RETRY_TIMEOUT` is inserted mid-enum.
  `grep -rn "GlobalConfiguration.*\.ordinal()\|values()\[" --include="*.java" */src/main` returns nothing,
  so no ordinal is persisted or indexed.
- **`ClusterManagementAuthorizationIT`.** Its two add-peer assertions are 403 (non-root) and 400 (root, empty
  body). Both short-circuit before `raftHAServer.addPeer`, so neither reaches the seed or the new status code.
- **`HAConfigDefaultsTest.allHAEntriesHaveNonNullDefaults`.** The new setting has a non-null default (`3000L`);
  the test runs green.
- **Interrupt landing in a seed call rather than in the backoff.** The seed's own exception is collected, and
  the very next `Thread.sleep` throws `InterruptedException` immediately because the flag is set - so the loop
  still ends at once. Pinned by `anInterruptEndsTheRetryAndLeavesTheFlagSet`, which interrupts a thread that is
  already retrying.
- **The deadline is computed before the first attempt**, so a first attempt slower than the whole budget
  yields exactly one attempt. That is the intended reading of a wall-clock budget, and it is what keeps an
  HTTP worker thread from being held indefinitely by the retry. Listed under residual risk instead: the budget
  bounds the *retrying*, not the blocking `submitAndWait` calls themselves.
