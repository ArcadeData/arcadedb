# #7516 - a Basic/API-token write forwarded between two followers with disagreeing leader views can ping-pong unbounded

## Problem

`LeaderCommandForwarder.forwardIfReplica` relays an administrative write that landed on a follower to the
leader. The one-hop rule that stops such a forward from cycling travels as the
`X-ArcadeDB-Forwarded-To-Leader` request header, and the receiving node honours it only inside the
`X-ArcadeDB-Cluster-Token` branch of `AbstractServerHttpHandler`. The forwarder sets the marker only on the
branch that authenticates with the cluster token (`Bearer AU-` session tokens). A request authenticated with
**Basic auth or an API token** is relayed with the client's own `Authorization` header and no marker at all,
so neither end of the one-hop rule engages: two followers whose resolved leader addresses name each other
forward the same request back and forth, one held Undertow worker thread per hop, bounded by nothing.

## Root cause

The marker is gated on *cluster-token authentication* rather than on *cluster-token proof*. The two are
conflated in one branch of `AbstractServerHttpHandler.execute`: the presence of `X-ArcadeDB-Cluster-Token`
both proves the request came from a peer **and** switches identity resolution to
`X-ArcadeDB-Forwarded-User`. A forward that must keep relaying the client's own stateless credentials - an
API token carries scopes that resolving the user by name on the leader would discard - therefore cannot
carry the token, and so cannot carry a trustworthy marker either.

## Invariant the fix establishes

> A request a cluster peer already forwarded to the leader is never forwarded again by the node that
> receives it, whichever credentials the client authenticated with - and a marker that does not travel
> beside a valid cluster token is still ignored.

## Completeness

### Every path that forwards a request toward the leader

```
$ grep -rn "FORWARDED_TO_LEADER_HEADER" --include='*.java' .
./ha-raft/src/test/java/.../Issue7380RestUserRoutesLeaderGateIT.java:237   (test)
./ha-raft/src/test/java/.../Issue6191FollowerForwardLoopIT.java:189,229    (test)
./ha-raft/src/test/java/.../Issue6221VerifyFanOutGuardIT.java:138          (test)
./ha-raft/src/main/java/.../RaftReplicatedDatabase.java:3533               (set)
./ha-raft/src/main/java/.../PostVerifyDatabaseHandler.java:357             (set)
./server/src/main/java/.../PostBatchHandler.java:1683                      (set)
./server/src/main/java/.../AbstractServerHttpHandler.java:373              (read)
./server/src/main/java/.../LeaderCommandForwarder.java:183                 (set)
./server/src/main/java/com/arcadedb/server/LeaderForwardContext.java:64    (const)

$ grep -rn "isAlreadyForwarded" --include='*.java' .
./ha-raft/src/main/java/.../RaftReplicatedDatabase.java:3454
./ha-raft/src/main/java/.../PostVerifyDatabaseHandler.java:182
./server/src/main/java/.../PostBatchHandler.java:1543
./server/src/main/java/.../LeaderCommandForwarder.java:107
(+ server/src/test/.../LeaderForwardContextTest.java)

$ grep -rn '"X-ArcadeDB-Cluster-Token"' --include='*.java' server/src/main/java
./server/src/main/java/.../LeaderProxy.java:85    (inbound loop check)
./server/src/main/java/.../LeaderProxy.java:133   (outbound)
./server/src/main/java/.../PostBatchHandler.java:1679
./server/src/main/java/.../AbstractServerHttpHandler.java:358
./server/src/main/java/.../LeaderCommandForwarder.java:177

$ grep -rn "forwardIfReplica" --include='*.java' .
./server/src/main/java/.../PostServerCommandHandler.java:643
./server/src/main/java/.../PutUserHandler.java:54
./server/src/main/java/.../DeleteUserHandler.java:50
./server/src/main/java/.../PostUserHandler.java:53

$ grep -rn "new LeaderProxy" --include='*.java' .
(no hits in src/main - see the argued row below)
```

### Coverage table

| Entry point | Marker set outbound | Marker honoured inbound | Outcome |
|---|---|---|---|
| `RaftReplicatedDatabase.command` (engine write forward, `/api/v1/command`) | yes, always with cluster token + forwarded user | yes | argued - already covered by #6191, unchanged |
| `PostBatchHandler.buildForwardRequest` (`/api/v1/batch`) | yes, always | yes | argued - already covered by #6191, unchanged |
| `PostVerifyDatabaseHandler` leader-to-peer fan-out | yes, always | yes | argued - already covered by #6221, unchanged |
| `LeaderCommandForwarder`, `Bearer AU-` branch (4 call sites: `POST /server`, `POST/PUT/DELETE /server/users`) | only when the **raw** `arcadedb.ha.clusterToken` setting is non-empty - which it is not on any cluster that did not declare one explicitly | yes | **fixed here** - reads the HA plugin's effective token |
| `LeaderCommandForwarder`, Basic/API-token branch (same 4 call sites) | **no** - the reported defect | no | **fixed here** |
| `LeaderCommandForwarder`, no `Authorization` header at all | no token, no marker | n/a | argued - a request with no credentials is refused by the leader before any forward decision; `isRequireAuthentication()` is true for all four call sites (they run `checkRootUser` first) |
| `LeaderProxy.tryProxy` | n/a | n/a | **filed as #7551** - dead code: `grep -rn "new LeaderProxy"` has no hit in `src/main`, nothing constructs it, so it cannot reach the bug. It is also the last raw `HA_CLUSTER_TOKEN` reader left in `src/main` after this change, and guards on cluster-token presence rather than on the marker |
| A cluster whose effective token is blank (HA plugin not started / non-Raft `HAServerPlugin`) | no token, no marker | n/a | argued - `HAServerPlugin.getClusterToken()` returns null only when HA is not active, and `forwardIfReplica` returns early (`ha == null || ha.isLeader()`) before it is read. `RaftHAServer` always derives a token at startup or throws `ConfigurationException` (`ClusterTokenProvider.initClusterToken`) |

### Sub-defect found by the sweep and fixed here

`LeaderCommandForwarder` read `getConfiguration().getValueAsString(HA_CLUSTER_TOKEN)` - the **raw setting**.
`ClusterTokenProvider.initClusterToken()` derives the token (PBKDF2 over cluster name + root password) when
the setting is empty and stores it on the provider **without writing it back into the configuration**
(only the test-only `initClusterTokenForTest` calls `config.setValue`). The receiving node validates against
the effective token (`AbstractServerHttpHandler.validateClusterForwardedAuth` prefers `ha.getClusterToken()`).
So on every cluster that did not declare `arcadedb.ha.clusterToken` explicitly - which includes
`BaseRaftHATest` - the session-token branch sent neither the cluster token nor the marker, and since that
branch also does not relay the client's `Bearer AU-` header, the forwarded request reached the leader with
no credentials at all and was answered 401.

## Residual risk

- The forward still has **no request timeout** (#7507): with the fix a cycle is two hops instead of
  unbounded, but each hop still holds a worker thread for as long as the leader takes to answer.
- The marker only stops a cycle; it does not make the misconfiguration go away. An operator still has to
  declare every node's HTTP port with the `host:raftPort:httpPort` syntax in `arcadedb.ha.serverList`.
- `LeaderProxy` remains unreachable dead code (#7551). Nothing in this change depends on it.

## The fix

Two changes, one on each end of the same hop.

**`LeaderCommandForwarder.forwardIfReplica`** now sends `X-ArcadeDB-Cluster-Token` and the
`X-ArcadeDB-Forwarded-To-Leader` marker together, once, before the auth branches - so the marker can never
travel without the token that makes it believable, on any branch. The Basic/API-token branch still relays the
caller's `Authorization` header unchanged and still sends **no** `X-ArcadeDB-Forwarded-User`: resolving the
user by name on the leader would discard the scopes an API token carries. The token is read through
`HAServerPlugin.getClusterToken()` (effective) with the raw setting as fallback, mirroring the receiving side.

**`AbstractServerHttpHandler.execute`** splits what the cluster token proves from what it substitutes.
`isValidClusterToken` answers "did a cluster peer send this" and gates the marker; `resolveForwardedUser`
runs only when `X-ArcadeDB-Forwarded-User` travels with it. A peer request with the token and no forwarded
user falls through to the ordinary `Authorization` check - and is still refused with the same
`Missing forwarded user` 401 when it carries no credentials of its own either, so no request shape that
exists today is answered differently.

## Verification

| Test | What it pins |
|---|---|
| `Issue7516BasicAuthForwardLoopIT.aBasicAuthWriteBetweenTwoFollowersThatNameEachOtherIsRefusedInOneHop` | the reported loop, on both call-site families (`POST /server/users` and `POST /server`), with the two followers' resolved leader addresses pointing at each other |
| `Issue7516BasicAuthForwardLoopIT.aMarkerWithoutTheClusterTokenDoesNotSuppressForwarding` | the trust gate did not widen: a client's own marker header is still ignored |
| `Issue7516BasicAuthForwardLoopIT.aSessionTokenWriteOnAFollowerIsForwardedAndExecutedOnTheLeader` | the effective-token fix, and the control that a forward still reaches the leader |
| `Issue7516ClusterTokenHopProofTest` (4 cases) | the receiver split: token + relayed credentials authenticates with those credentials; an invalid token is still refused; a token alone still authenticates nobody; a forwarded user still names the principal |

`Issue7516ClusterTokenHopProofTest.aClusterTokenBesideRelayedCredentialsAuthenticatesWithThoseCredentials`
was run against the un-patched tree and fails there (401), which is what makes the other three meaningful.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a `general-purpose` subagent that has not seen the author's reasoning.
**No `Task` tool is exposed in this session** (this run is itself a subagent, so it cannot spawn in-process
subagents), so the pass was run by the author against the diff instead. That is weaker by exactly the amount
the isolation was worth - it is recorded here rather than quietly skipped.

| Finding | Disposition |
|---|---|
| The loop test drove only `POST /server/users`. A fix that covered the REST routes and missed the `POST /api/v1/server` command path - the older of the two call-site families through the same forwarder - would have passed it | **Fixed here**: the same poisoned-address window now drives `POST /api/v1/server` too |
| The marker was emitted whenever an effective cluster token existed, including on the branch where no `Authorization` header travels at all, i.e. potentially without the token header itself | **Fixed here**: token and marker are now set together, once, before the auth branches, so the marker cannot travel without the proof that makes it believable |
| `exchange.getRequestHeaders().get("Authorization") == null` misses the non-null-but-empty `HeaderValues` that the ordinary auth check below already handles as absent | **Fixed here**: same `null || isEmpty()` test as the standard check |
| `LeaderForwardContext`'s Javadoc asserts "`LeaderProxy` enforces the same one-hop rule for the requests it relays". `LeaderProxy` is never constructed (`grep -rn "new LeaderProxy"` has no hit in `src/main`), and if revived would read the raw `HA_CLUSTER_TOKEN` setting - the same defect fixed here - and guards on cluster-token presence rather than on the marker | **Real, out of scope** - filed as **#7551**. Reviving or deleting it is a decision about a component this issue does not touch |
| Each hop still holds an Undertow worker thread with no request timeout on the forward | **Real, already filed** as #7507. The fix bounds the cycle at two hops; it does not bound a hop |
| A leadership change in flight now turns a Basic/API-token forward into a 400 rather than a second hop that might have landed on the new leader | **Argued**: identical to the trade-off #6191 already made for the cluster-token branch, and the refusal message says "retry". A second hop cannot prove the first did not execute, which is why `RaftReplicatedDatabase.forwardCommandToLeaderViaRaft` takes the same position in its own comment |
| Does the marker now being set on requests that reach the true leader break anything? | **Argued**: both other readers of the thread-local (`RaftReplicatedDatabase.forwardCommandToLeaderViaRaft`, `PostBatchHandler.forwardBatchToLeader`) are only reached on a node that is not the leader; verified by reading both call sites |

## Test results

```
mvn -o -pl server -Dtest=Issue7516ClusterTokenHopProofTest test
  Tests run: 4, Failures: 0, Errors: 0

mvn -o -pl ha-raft -DskipITs=false -Dit.test=Issue7516BasicAuthForwardLoopIT,Issue6191FollowerForwardLoopIT,\
  Issue7380RestUserRoutesLeaderGateIT,Issue6221VerifyFanOutGuardIT test-compile failsafe:integration-test failsafe:verify
  Issue7516BasicAuthForwardLoopIT        Tests run: 3, Failures: 0, Errors: 0
  Issue6191FollowerForwardLoopIT         Tests run: 5, Failures: 0, Errors: 0
  Issue7380RestUserRoutesLeaderGateIT    Tests run: 2, Failures: 0, Errors: 0
  Issue6221VerifyFanOutGuardIT           Tests run: 4, Failures: 0, Errors: 0
  Tests run: 14, Failures: 0, Errors: 0
```

The three existing ITs are the regression set: #6191 owns the marker mechanism this change moves,
#7380 owns the four call sites that share the forwarder, #6221 owns the leader-to-peer fan-out that
reads the same thread-local.

`ClusterInternalAuthTest` and `PostClusterAuthSessionHandlerTest` fail on this machine both with and
without the patch: they dial a hard-coded `localhost:2480` while the server may bind anywhere in the
configured `2480-2489` range, so with another test JVM already listening there the requests reach that
server instead. Verified by running both at `HEAD` with the patch reverted - identical failures. The new
`Issue7516ClusterTokenHopProofTest` reads `getServer(0).getHttpServer().getPort()` for that reason.

## Ledger

- [x] 1. A Basic/API-token forward carries no one-hop marker and can ping-pong between two followers -
      **fixed**, on both call-site families, with an IT that drives the cycle
- [x] 2. `LeaderCommandForwarder` reads the raw `arcadedb.ha.clusterToken` setting rather than the
      effective token (found by the sweep, same defect class) - **fixed**, with an IT that logs in over
      HTTP and forwards a session-token write
- [x] 3. `LeaderProxy` is unreachable and `LeaderForwardContext`'s Javadoc claims it enforces the rule -
      **filed as #7551**
- [x] 4. No request timeout on a forwarded hop - **already filed as #7507**

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7558

## Review cycles

| Cycle | Head | Review outcome | Change made |
|---|---|---|---|
| 1 | `11ab0207` | One actionable item: `LeaderForwardContext`'s Javadoc still listed `LeaderProxy` among the places the one-hop rule is enforced, which this PR's own analysis had shown to be a claim about code nothing constructs. Also noted the test-plan boxes were unticked | Both mentions of `LeaderProxy` now say it is never constructed and point at #7551. PR body's test-plan boxes ticked for the three runs actually performed; the manual three-node check left unticked and labelled as not run |
| 2 | `9dde04ba` | One maintainability item, flagged as optional: `LeaderCommandForwarder.effectiveClusterToken` and `AbstractServerHttpHandler.isValidClusterToken` each carried their own copy of the "plugin's token first, raw setting as fallback" order - the same duplication whose drift is the defect being fixed | Extracted `HAServerPlugin.effectiveClusterToken(ArcadeDBServer)`; both sides call it. No behaviour change. Re-ran the 4 unit cases and 10 IT cases, green |
| 3 | `780b46f3` | "Nothing blocking". Two minor notes: the `Authorization` header was read twice per request; and `effectiveClusterToken` being a static on an otherwise instance-method interface is "a one-line note either way, not a request for change" | Collapsed the header read to one lookup shared by both readers. The static-on-interface shape was left as is - the reviewer explicitly did not ask for a change, and putting a resolver used by two collaborators on the interface that owns the concept is what keeps them from drifting apart again. Re-ran 8 unit and 5 IT cases, green |
| 4 | `137bc4a0` | No actionable items. Traced every branch combination against the four unit cases, confirmed `constantTimeEquals` is unchanged, confirmed no unused imports and no leftover debug output, and independently re-ran the `new LeaderProxy` grep | None |

**Deferred items:** none. No `review-deferred-*.md` notes file was produced in any cycle.

**CodeRabbit** was rate-limited for the whole run (its check reports "pass / Review rate limited") and posted no findings, so no review threads are open.

**Final state:** clean-approval on cycle 4. GitHub Actions were still finishing when the loop ended
(`build-and-package`, `lint` and several CodeQL analyses pending); the merge, and confirming those, stay
with the developer.
