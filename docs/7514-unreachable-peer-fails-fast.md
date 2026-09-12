# #7514 - Adding an unreachable peer hangs ~60s and reports a raw Ratis error

Issue: https://github.com/ArcadeData/arcadedb/issues/7514
Type: bug (labels already on the issue: `bug`, `server`, `ha`, `in progress`; assignee `robfrank`)
Branch: `fix/7514-unreachable-peer-hangs-raw-ratis-error`

## Finding ledger

The issue reports one defect with a three-item suggested scope:

- [x] 1. Probe the target address before issuing the configuration change; refuse with 400 /
      `INVALID_ARGUMENT` naming the address - **fixed**, on all four entry points.
- [x] 2. Failing that, translate the Ratis reply into a sentence: which peer, which address, that it did
      not catch up within the budget - **fixed**, and done as well as item 1 rather than instead of it,
      because the probe is deliberately one-sided and a reachable-but-not-catching-up peer still reaches
      the give-up branch.
- [x] 3. Consider whether 60 s is the right budget for a synchronous HTTP request at all, or whether the
      add should report progress - **deferred, filed as #7561**. Considered and left out on purpose: the
      probe removes the case this budget was actually being paid for, and changing the retry policy of
      the shared `RaftClient` reaches every Raft operation, not only membership.

## Root cause

`RaftClusterManager.addPeer` issues a `SetConfigurationRequest` with `Mode.ADD` through
`setConfigurationWithRetry`. Two nested budgets stack up:

- the shared `RaftClient` carries `RetryLimited(maxAttempts=60, sleepTime=1s)`, so ONE
  `admin().setConfiguration(...)` call blocks for ~60 s before throwing `RaftRetryFailureException`
  (an `IOException`);
- `setConfigurationWithRetry` has its own 90 s deadline and checks it only AFTER the inner call
  returns, so a second ~60 s attempt is started at t≈60 s and the request is only abandoned at t≈120 s.

Ratis does not commit a `Mode.ADD` until the new peer has caught up, so naming a peer that is not
running cannot succeed - yet nothing on the path asks whether the address is reachable before paying
that budget. The failure is then reported as `"Failed to " + operationDesc` with the
`RaftRetryFailureException` as the cause, whose message is the serialized `SetConfigurationRequest`.

## Completeness

### Invariant

> An `addPeer` naming a Raft address that accepts no TCP connection is refused before any Raft
> configuration change is issued, with a client error (HTTP 400 / gRPC `INVALID_ARGUMENT`) naming the
> peer, the address and the reason - and no membership change failure reports a serialized Ratis
> request object as its only explanation.

### Enumeration (commands and output)

```
$ grep -rn "raftHAServer\.addPeer\|raft\.addPeer\|clusterManager\.addPeer\|new RaftClusterManager(.*)\.addPeer" --include="*.java" .
ha-raft/src/test/java/.../Issue7401JoinPriorityTest.java:94:    new RaftClusterManager(server).addPeer(target.peer(), target.name());
ha-raft/src/test/java/.../RaftAtomicMembershipTest.java:75:    new RaftClusterManager(server).addPeer("D", "localhost:2447");
ha-raft/src/main/java/.../RaftHAServer.java:2917:    clusterManager.addPeer(peerId, address, name);
ha-raft/src/main/java/.../RaftHAServer.java:2922:    clusterManager.addPeer(newPeer, name);
ha-raft/src/main/java/.../RaftHAPlugin.java:483:    raft.addPeer(target.peer(), target.name());
ha-raft/src/main/java/.../RaftHAPlugin.java:501:    raftHAServer.addPeer(peerId, address, name);
ha-raft/src/main/java/.../PostAddPeerHandler.java:62:    raftHAServer.addPeer(peerId, address, name.isEmpty() ? null : name);
```

Every production caller reaches `RaftClusterManager` through `RaftHAServer.addPeer`. The only direct
`RaftClusterManager.addPeer` callers are the two existing unit tests.

```
$ grep -rn "implements .*HAServerPlugin" --include="*.java" . | grep /src/main/
ha-raft/src/main/java/.../RaftHAPlugin.java:52:public class RaftHAPlugin implements HAServerPlugin, HAReplicationStatsProvider {
```

One HA implementation, so `HAServerPlugin.addPeer`'s interface default is never the live path.

```
$ grep -rn "admin()\.setConfiguration" --include="*.java" .
ha-raft/src/main/java/.../KubernetesAutoJoin.java:263:  final RaftClientReply joinReply = tempClient.admin().setConfiguration(addArgs);
ha-raft/src/main/java/.../RaftClusterManager.java:415:  final RaftClientReply reply = raftHAServer.getClient().admin().setConfiguration(args);
```

Two Raft membership writers. `KubernetesAutoJoin` inserts THIS node into a remote cluster and already
probes the peer with a `GroupInfo` RPC under a bounded `PROBE_RETRY_POLICY` before it does, so it is
the sibling that does not carry the defect - it is where the "probe first" shape comes from.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `POST /api/v1/cluster/peer` -> `PostAddPeerHandler` -> `RaftHAServer.addPeer(String,String,String)` | yes | yes - `Issue7514UnreachablePeerFailsFastIT.addPeerRouteRefusesAnUnreachableAddress` |
| `POST /api/v1/server` `connect cluster <addr>` -> `ServerControlPlane` -> `RaftHAPlugin.connectCluster` -> `RaftHAServer.addPeer(RaftPeer,String)` | yes | yes - `Issue7514UnreachablePeerFailsFastIT.connectClusterRefusesAnUnreachableAddress` |
| gRPC `ConnectCluster` -> `ServerControlPlane.connectCluster` -> same method | yes | yes - `Issue7514UnreachablePeerRefusalTest.theRefusalMapsToInvalidArgumentAndHttp400` pins the type contract the shared mappers key on; the RPC itself is a two-line adapter over the method the IT drives |
| `HAServerPlugin.addPeer(...)` embedded API -> `RaftHAPlugin.addPeer` -> `RaftHAServer.addPeer` | yes | yes - `Issue7514UnreachablePeerFailsFastIT.theEmbeddedAddPeerApiRefusesTheSameWay` |
| `KubernetesAutoJoin` self-insert | argued - different direction (this node joins a remote group) and it already probes the peer with a bounded `GroupInfo` RPC before the `Mode.ADD`; see the grep above | n/a |
| `RaftClusterManager.addPeer` called directly | argued - package-private and, per the grep above, called from no production code | n/a |
| The retry budget itself (scope item 3) | **no** - filed as #7561 | no |

### Reachability

`RaftHAServer.addPeer` is the method `PostAddPeerHandler`, `RaftHAPlugin.connectCluster` and
`RaftHAPlugin.addPeer` all call (grep above). No flag gates the probe off by default:
`HA_ADD_PEER_PROBE_TIMEOUT` defaults to 2000 ms and only a value <= 0 disables it.

### Residual risk

- A peer that accepts a TCP connection on its Raft port but is not a working member of this group
  (wrong cluster name, wrong token, still starting up) still pays the full membership-change budget.
  That case now ends in a sentence rather than a serialized request object, which is scope item 2, but
  it is still slow - scope item 3, filed as a follow-up.
- The probe is a TCP connect, not a Raft handshake. It refuses only when it is certain nothing is
  listening; a successful connect is not proof of a healthy peer and deliberately falls through to the
  behaviour that was there before.
- An address whose port cannot be parsed is not probed (there is nothing to connect to); it falls
  through unchanged.
- The probe runs on the node that received the request, which is not necessarily the leader - neither
  add-peer route is leader-routed. A follower that cannot open a connection to a peer the leader can
  would refuse the add. The refusal names the probing node so that case is diagnosable, and
  `arcadedb.ha.addPeerProbeTimeout=0` turns the probe off.
- A host name resolving to several addresses is probed at the first one `InetSocketAddress` resolves,
  not at all of them.

## Changes

| File | What |
|---|---|
| `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` | new `HA_ADD_PEER_PROBE_TIMEOUT` (`arcadedb.ha.addPeerProbeTimeout`, `Long`, 2000 ms; `0` disables the probe) |
| `ha-raft/.../PeerReachability.java` | new. The probe: a bounded TCP connect that answers *why* an address could not be reached, or `null` when it could - and `null` too when there is nothing to dial, so "not probed" cannot read as "unreachable" |
| `ha-raft/.../UnreachablePeerException.java` | new. Extends `IllegalArgumentException`, which is the type `AbstractServerHttpHandler` already maps to HTTP 400 and `ArcadeDbGrpcAdminService` to gRPC `INVALID_ARGUMENT` |
| `ha-raft/.../RaftHAServer.java` | both `addPeer` overloads now meet at `addPeer(RaftPeer, String)`, which runs `ensurePeerReachable` before delegating. The probe is skipped for a peer already in `getLivePeers()`, mirroring `RaftClusterManager.buildAddArgs` so an add that would have been an idempotent no-op is never refused |
| `ha-raft/.../RaftClusterManager.java` | the give-up message is built by `gaveUpMessage(...)`: what was attempted in words, the budget it was attempted for, a hint, and the Ratis text last and labelled. The 90 s budget is now a field with a package-private constructor so the give-up branch is reachable in a test without waiting it out |

The probe lives in `RaftHAServer` rather than in `RaftClusterManager` deliberately: the manager's job is
to issue a Raft configuration change, and whether an address answers a TCP connection is not part of it.
`RaftHAServer` is also the object that holds the `ContextConfiguration` the budget comes from, and - per
the grep above - the one method every production entry point reaches.

## Test results

| Run | Result |
|---|---|
| `Issue7514UnreachablePeerFailsFastIT` **before** the fix | 3/3 FAIL, 108.5 s / 111.3 s / 119.5 s per method; `ConfigurationException: Failed to add peer localhost_57755` (the address is not even in it), HTTP 500 |
| `Issue7514UnreachablePeerFailsFastIT` **after** | 3/3 pass, 31.0 s for the whole class including cluster startup |
| `Issue7514UnreachablePeerRefusalTest` | 6/6 pass |
| ha-raft full unit suite (`mvn test -pl ha-raft`) | 1412 run, 2 failures - both in `ArcadeStateMachinePerDatabaseHaltTest`, which is red on main already and tracked as #7520 / #7495. Neither `ArcadeStateMachine` nor `RaftLogEntryCodec` nor that test is in this branch's diff (`git diff --name-only HEAD` lists three files, none of them) |
| `Issue7401ConnectClusterJoinsPeerIT`, `RaftUserSeedOnPeerAdd3NodesIT`, `ClusterManagementAuthorizationIT` | 6/6 pass - #7401's is the one that matters, because it re-joins a peer that IS running and so proves the probe does not refuse a legitimate add |
| engine `GlobalConfigurationTest`, `Issue7124BooleanSettingStrictCoercionTest`, `Issue7163DatabaseScopeCallbackTest` | 29/29 pass |
| server `Issue7401ServerControlPlaneConnectClusterTest`, `PluginApiSpecTest`, `CoreApiSpecTest` | 54/54 pass |

## Impact

- `POST /api/v1/cluster/peer`, `connect cluster` and the gRPC `ConnectCluster` RPC answer a client error
  in milliseconds instead of holding an HTTP worker thread for ~2 minutes and answering 500.
- Nothing about a successful add changes: the probe reports only failure, so an address that answers
  proceeds down exactly the path it did before.
- `removePeer` is unchanged except for its give-up message, which now says what was attempted and for
  how long.

## Recommendations

- #7561 is the remaining half: a peer that accepts a connection and never catches up still costs up to
  ~150 s.
- If a deployment's networks make a 2 s TCP handshake unrealistic, raise
  `arcadedb.ha.addPeerProbeTimeout` rather than disabling it; `0` restores the pre-#7514 behaviour
  wholesale.

## Adversarial pass

The orchestrator's Phase 1.5 asks for a subagent that has not been persuaded by the author's reasoning.
**No `Task` tool was available in this environment**, so the pass was run by the author against the tree
instead. That is weaker than the process intends and is recorded as such: the value of the step is the
reviewer's independence, and this run did not have it.

| Finding | Disposition |
|---|---|
| The refusal did not say **which node ran the probe**. Neither add-peer route is leader-routed (`Issue7401ConnectClusterJoinsPeerIT.connectClusterIssuedOnAFollowerStillJoinsThePeer` exists for exactly that), so the probe answers from the network view of whatever node the request landed on. A follower that cannot reach a peer the leader can would refuse an add that would have succeeded, and the message would read identically to a genuinely-down peer | **Fixed here.** `UnreachablePeerException` now takes the probing node and the message ends "probed from node '<id>'". Pinned by `theRefusalNamesTheNodeThatProbed` |
| The two branches on which the probe must **not** refuse - the peer is already a member, and the operator set the budget to 0 - were private and untested. Both are regressions if they break: the first is the documented idempotence of `connect cluster`, the second is the escape hatch | **Fixed here.** The decision moved into `PeerReachability.addRefusalReason(alreadyAMember, probeTimeoutMs, address)`, and all three branches are pinned by `theProbeIsSkippedForAMemberAndWhenTheOperatorTurnedItOff` |
| IPv6: `PeerReachability` splits on the last colon, so a bracketed `[::1]:2434` would hand `InetSocketAddress` a bracketed host | **Not real.** `RaftPeerAddressResolver` emits the bracketed form (it rejects anything else as "Invalid IPv6 peer address"), and a scratch JVM run confirmed `new Socket().connect(new InetSocketAddress(h, p))` resolves both `[::1]` and `::1`. The split is also the same `lastIndexOf(':')` convention the resolver and `RaftClusterManager`'s own HTTP-address derivation already use |
| `ensurePeerReachable` calls `getLivePeers()` before the client is touched, so an add issued before the Raft server exists could NPE somewhere new | **Not real.** `raftGroup` is a `final` field assigned inside the constructor (line 405; the constructor spans 334-460), and `getCommittedPeersOrNull()` already returns null rather than throwing when `raftServer` is null |
| A peer that accepts a TCP connection and never catches up still costs up to ~150 s | **Real, out of scope - filed as #7561.** This is the issue's own scope item 3 |
| A host that resolves to several addresses is probed only at the first one `InetSocketAddress` resolves | **Real, documented as residual risk.** Not filed: the add-peer routes take a literal address an operator typed, and the multi-address case in this codebase is the Kubernetes headless service, which joins through `KubernetesAutoJoin` rather than through these routes |
