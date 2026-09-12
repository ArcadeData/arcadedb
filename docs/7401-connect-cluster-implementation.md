# #7401 - `connect cluster` is a stub on every transport

Issue: https://github.com/ArcadeData/arcadedb/issues/7401
Type: enhancement (labels `enhancement`, `server`, `ha`; milestone 26.10.1)

## The decision the issue asked for

#7401 raised a two-way decision and left it open:

1. **implement** `connect cluster` - "a client-initiated join, routed through the Raft leader the way
   `POST /api/v1/cluster/peer` already adds a peer. If the Raft peer route is the intended way to join,
   `connect cluster` may simply be a thin alias for it, in which case this is small"; or
2. **retire the verb**.

**Decision: option 1, in exactly the shape the issue sketched.** `connect cluster <address>` joins the
server at `<address>` to this server's Raft cluster, by the same atomic `Mode.ADD` membership change that
`POST /api/v1/cluster/peer` issues. Retiring was rejected because #7400 had just added the gRPC RPC for
contract parity, so retiring means removing a published verb from two transports for a capability the
Raft stack already has - `RaftHAPlugin.addPeer` - and only lacked an argument mapping for.

### What made the alias small

`POST /api/v1/cluster/peer` takes `{peerId, address}`; `connect cluster` supplies only an address. Those
are not two different amounts of information, because a Raft peer id in this codebase is a pure function
of the Raft address:

```
$ grep -rn --include='*.java' "replace(':', '_')" --exclude-dir=target ha-raft/
ha-raft/.../RaftPeerAddressResolver.java:199:      final String peerIdStr = raftAddress.replace(':', '_');
ha-raft/.../RaftPeerAddressResolver.java:512:    final String peerIdStr = raftAddress.replace(':', '_');
```

Line 199 is `parsePeerList` (every peer named in `arcadedb.ha.serverList`); line 512 is
`synthesizeK8sScaleUpPeer` (a StatefulSet pod past the end of that list). Both sites were reading the
same rule from a comment, so this change extracts it into `RaftPeerAddressResolver.peerIdForAddress`
and makes all three callers - the two above and the new join path - share it.

The consequence is that `connect cluster` accepts *one entry of `arcadedb.ha.serverList` syntax* and
derives peer id, Raft address, optional HTTP address, priority and optional `name@` display name from
it, by running the single entry through the very parser the server list uses. So the address an operator
writes in the command is the address they would have written in the config, and the id the joined peer
gets is the id it gives itself.

## Invariant

> `connect cluster <address>`, on either transport, adds the server named by `<address>` to this node's
> Raft configuration under the same peer id `arcadedb.ha.serverList` would give it - or refuses with a
> reason that names `<address>`.

## Completeness

### Entry points

```
$ grep -rn --include='*.java' "\.connectCluster(" --exclude-dir=target . | grep /src/main/
grpc-client/.../RemoteGrpcServer.java:792:    call("connect cluster", stub -> stub.connectCluster(
server/.../ServerControlPlane.java:163:   * (javadoc)
server/.../http/handler/PostServerCommandHandler.java:227:    controlPlane.connectCluster(serverAddress);
grpcw/.../ArcadeDbGrpcAdminService.java:935:      controlPlane.connectCluster(req.getServerAddress());

$ grep -rn --include='*.java' "implements HAServerPlugin" --exclude-dir=target .
ha-raft/.../RaftHAPlugin.java:52:public class RaftHAPlugin implements HAServerPlugin, HAReplicationStatsProvider {
server/src/test/.../GetReadyHandlerHATest.java:168:  private static final class FakeHAPlugin implements HAServerPlugin {
```

There is no HTTP `RemoteServer` method, no console command and no Studio control for this verb:

```
$ grep -rn "connect cluster\|connectCluster" --include='*.html' --include='*.js' --include='*.ts' \
    --exclude-dir=target --exclude-dir=node_modules .
(no output)
$ grep -rn "CONNECT_CLUSTER\|connectCluster" --include='*.java' --exclude-dir=target console/src network*/src
(no output)
```

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| HTTP `POST /api/v1/server` -> `connect cluster <addr>` -> `PostServerCommandHandler:144,227` | yes | yes - `Issue7401ConnectClusterJoinsPeerIT` joins a real server into a live 3-node Raft cluster over this verb and asserts the committed configuration; `Issue7400ConnectClusterHttpIT` pins the refusal statuses |
| gRPC `ConnectCluster` -> `ArcadeDbGrpcAdminService:935` | yes | yes - `Issue7400GrpcConnectClusterIT` drives both refusal statuses (`INVALID_ARGUMENT`, `FAILED_PRECONDITION`) and `theAddressReachesTheSharedImplementationUnmodified` proves the argument survives the transport. **Argued** for the join itself: the handler is `requireServerAdmin` plus one delegating call with no logic of its own, so a gRPC repeat of the live-cluster join would exercise the same `ServerControlPlane` method the HTTP IT already drives end to end |
| `RemoteGrpcServer.connectCluster` (Java gRPC client) | yes (wire adapter, unchanged; its javadoc documented the opposite direction and is corrected) | yes - `Issue7400RemoteGrpcConnectClusterIT` |
| `ServerControlPlane.connectCluster` -> `HAServerPlugin.connectCluster` default (an HA implementation without dynamic membership) | yes - `UnsupportedOperationException` is converted to `OperationNotAvailableException`, so the refusal keeps HTTP 500 / gRPC `FAILED_PRECONDITION` instead of degrading to `INTERNAL` | yes - `Issue7401ServerControlPlaneConnectClusterTest` |
| `ServerControlPlane.connectCluster` -> the users seed | yes | yes - `Issue7401ServerControlPlaneConnectClusterTest` asserts the seed runs, runs *after* the join, and that a failing seed does not fail the join |
| `RaftHAPlugin.connectCluster` -> address parsing / peer-id derivation | yes | yes - `Issue7401JoinTargetTest`, one case per server-list syntax the argument can use |
| `RaftHAPlugin.connectCluster` -> the declared leader-election **priority** reaching the committed peer | yes - fixed during the review loop, see below | yes - `Issue7401JoinPriorityTest`, which reads the `SetConfigurationRequest` Ratis is asked to commit |
| `RaftHAPlugin.connectCluster` with the Raft server not started | yes - `ServerException` | **argued**: the same guard, in the same place, with the same message as the sibling membership methods in this class (`addPeer`, `removePeer`, `transferLeadership`, `stepDown`, `leaveCluster`). None of them has a test for it, and one here would pin the guard rather than the join. The *type* differs and is stated here rather than glossed: `grep -n "Raft HA server not started" RaftHAPlugin.java` shows five bare `RuntimeException`s, one `TransactionException` and this one `ServerException`. Nothing observable turns on it - neither is `IllegalArgumentException` nor `OperationNotAvailableException`, so both surface as HTTP 500 / gRPC `INTERNAL` - and the more descriptive type is kept rather than matched downwards; converting the other six is unrelated churn for this PR |
| `arcadedb.ha.serverList` startup path -> `parsePeerList` peer-id derivation | yes (refactor only - now calls the extracted `peerIdForAddress`) | yes - the 155 existing `ha-raft` resolver/K8s/allowlist tests still pass, and `Issue7401JoinTargetTest.theJoinPathAndTheServerListPathAgreeOnTheSameAddress` compares the two derivations directly |
| Kubernetes scale-up -> `synthesizeK8sScaleUpPeer` peer-id derivation | yes (refactor only - same extraction) | yes - `Issue4836K8sScaleUpTest` (7 tests, unchanged, still green) |

No row is blank. Two rows are argued with the evidence above; every other row is fixed here with a test
driving it through that entry point.

### Reachability

`RaftHAPlugin` is the `HAServerPlugin` the ServiceLoader finds whenever `arcadedb.ha.enabled=true` or
`arcadedb.ha.serverList` is non-blank, and `ServerControlPlane` is constructed by both
`PostServerCommandHandler` and `ArcadeDbGrpcAdminService` on every request. Nothing new sits behind a
flag. `Issue7401ConnectClusterJoinsPeerIT` boots a real Raft cluster and drives the verb over HTTP, so
the changed code is proved to run outside its own unit test.

### What testing it changed about the design

The first draft of the live IT joined an address nothing listened on, on the assumption that a committed
Raft member need not be reachable. Ratis refused: `Mode.ADD` does not commit until the new peer has
caught up, so the verb answered 500 after sixty one-second attempts with the `SetConfigurationRequest`
rendered into the message. The IT was rewritten around a server that is actually running, and the
discovery became follow-up #7514 - it is the shape of the mistake this verb invites, and it is not
specific to it: `POST /api/v1/cluster/peer` behaves the same way.

## Tests

New:

- `ha-raft` `Issue7401JoinTargetTest` (10) - the address-to-peer derivation, one case per accepted
  syntax, plus the direct comparison against the server-list derivation.
- `ha-raft` `Issue7401ConnectClusterJoinsPeerIT` (3) - a live 3-node cluster: a member is dropped from
  the committed configuration and re-joined with `connect cluster` over HTTP, from the leader and from a
  follower; a malformed address answers 400 and changes nothing.
- `server` `Issue7401ServerControlPlaneConnectClusterTest` (7) - the transport-independent layer against
  a hand-written `HAServerPlugin`: blank address, HA absent, HA without dynamic membership, address
  passed through unmodified, users seeded after the join, a failing seed not failing the join, and a
  genuine join failure not disguised as an unavailable operation.

Run and green (`-Dmaven.repo.local` isolated):

| Suite | Result |
|---|---|
| `ha-raft` resolver / K8s / allowlist / membership unit tests (10 classes) | 155 passed |
| `ha-raft` `Issue7401ConnectClusterJoinsPeerIT` | 3 passed (41 s) |
| `server` OpenAPI spec tests + `GetReadyHandlerHATest` + the new control-plane test | 153 passed |
| `server` `Issue7400ConnectClusterHttpIT`, `OpenApiSpecGenerationIT` | 20 passed |
| `grpcw` `Issue7304GrpcControlPlaneIT`, `Issue7304GrpcControlPlaneAuthorizationIT`, `Issue7400GrpcConnectClusterIT` | 83 passed |
| `grpc-client` `Issue7304RemoteGrpcServerControlPlaneIT`, `Issue7400RemoteGrpcConnectClusterIT` | 10 passed |

`Issue7401ConnectClusterJoinsPeerIT` ran green twice: before the priority fix (3 passed, 41 s) and
again after it (3 passed, 37 s). In between, several attempts to run it died in
`Error occurred in starting fork` on `java.net.BindException: Address already in use` for Raft port
2434, held by another agent's JVM on this machine (`lsof -nP -iTCP:2434 -sTCP:LISTEN`) - the fixed-port
collision `CLAUDE.md` documents, not a property of the test. The `ha-raft` unit lane lost its tail the
same way: 425 passed before `LeaveClusterTest` crashed its fork on the same bind.

`server` `PostServerCommandHandlerIT` reported 4 failures in this environment
(`createUserRejectsShortPassword`, `userCommandsCaseSensitivity`, `restoreDatabaseCommand`,
`importDatabaseCommandSsePathHonorsTheSameLocalUrlsFlagAtTheFetchLayer`). The class hardcodes
`http://localhost:2480`, and `lsof -nP -iTCP:2480 -sTCP:LISTEN` showed two foreign JVMs holding that port
throughout the run - the port-conflict failure mode `CLAUDE.md` documents. None of the four touches
`connect cluster`, and one of them fails with `ServerIsNotTheLeaderException: Leader address is unknown`
from a server this fixture never starts HA on.

## Existing tests changed

`constraints.md` forbids modifying existing tests. Five assertions had to change anyway, and this is the
exception the issue itself creates: they pin the stub's refusal string
(`"not supported by the current HA implementation"`), which is the sentence this issue removes. No test
was deleted and no coverage was dropped - each changed assertion was rewritten against the new contract
and, where the new contract is stricter, strengthened with the status code it now guarantees.

| Test | Was | Now |
|---|---|---|
| `Issue7400ConnectClusterHttpIT.connectClusterIsRefusedAndNamesTheAddress` | body contains the address and "not supported by the current HA implementation" | body still contains the address; message is the HA-not-enabled refusal |
| `Issue7400ConnectClusterHttpIT.connectClusterWithNoAddressIsRefusedTheSameWay` | refused the same way a filled address is | 400 with "requires the address" - a bare verb is now a client error, not a precondition failure |
| `Issue7400GrpcConnectClusterIT.connectClusterIsRefusedByTheSharedImplementation` | FAILED_PRECONDITION + the stub string | FAILED_PRECONDITION + the HA-not-enabled refusal |
| `Issue7400GrpcConnectClusterIT.connectClusterWithAnEmptyAddressIsRefusedTheSameWay` | FAILED_PRECONDITION + the stub string | INVALID_ARGUMENT, mirroring HTTP's 400 |
| `Issue7400RemoteGrpcConnectClusterIT.connectClusterIsReportedThroughTheSharedErrorMapper` | the stub string | the HA-not-enabled refusal |

`Issue7400GrpcConnectClusterIT.theAddressReachesTheSharedImplementationUnmodified`,
`theClusterPairFailsThePreconditionAlike`, `theClusterPairDeniesAnAuthenticatedNonRootCaller` and
`Issue7304GrpcControlPlaneAuthorizationIT`'s `ConnectCluster` row are untouched and still pass: the
refusal still names the address, both halves of the pair still fail the precondition alike, and the
authorization gate is unchanged.

## Adversarial pass

The orchestrator's Phase 1.5 spawns one subagent that is deliberately kept ignorant of the author's
reasoning. **No `Task` tool was available in this session**, so that exact pass could not run. Two
substitutes were used and are reported as what they are, not as what was asked for:

1. A `code-review` subagent (general-purpose, sees the diff and the tree, not this document) was
   launched at high effort. It had not returned by the time the PR opened; anything it raises is
   handled in the review loop as a further commit.
2. A self-review against the same question. It was not worthless - it found two things, and both are
   in the diff:
   - **A comment asserting something the code does not establish.** An earlier draft said "Replicating
     users is a leader operation, so this is also how a command that landed on a follower gets its join
     without its seed." Reading `RaftGroupCommitter.submitAndWait` shows no leader gate at all - the
     entry goes through a Ratis client, which routes writes to the leader - and `PostAddPeerHandler`
     calls `replicateSecurityUsers` with no leader check either. The sentence was removed rather than
     softened: it was an inference dressed as a fact, and the kind this project's constraints forbid.
   - **An inference stated as a fact about `KubernetesAutoJoin`.** "runs at startup, before the node has
     committed anything of its own" became "runs from `start()` and only while this node knows no leader
     of its own", which is what its retry continuation condition actually says.

A third finding came from the tests rather than from reading, and is recorded under **What testing it
changed about the design** above: the first live IT assumed an unreachable peer could be added, and it
cannot. That became #7514.

## Review finding: the declared priority was parsed and then dropped

The first `claude` review of PR #7517 found a real defect, and it is the most instructive thing in this
change.

`parseJoinTarget` parses a whole server-list entry, and both the object form
(`db2:{raft:2435,priority:7}`) and the four-field positional form (`db2:2435:2481:5`) declare a
leader-election priority. `Issue7401JoinTargetTest.theObjectFormIsAcceptedToo` asserted
`target.peer().getPriority()` and passed throughout. One layer down, `RaftHAPlugin.connectCluster`
handed `addPeer` an id, an address and a name, and `RaftClusterManager.addPeer` built a **fresh**
`RaftPeer` from those three - so the priority was silently replaced by `RaftPeer.Builder`'s default.

It is not cosmetic. `RaftHAServer.selectStepDownTargets` reads each live peer's `getPriority()` and,
once any peer has a positive priority, skips the priority-0 ones as non-electable witnesses
(`RaftHAServer.java`, the `maxPriority > 0 && peer.getPriority() <= 0` guard) - so a priority reset to
the default changes which nodes can take leadership. Every other assertion about the join held while
the cluster got a configuration the operator had not asked for.

**Fixed by handing the parsed `RaftPeer` over whole** - a new `RaftClusterManager.addPeer(RaftPeer,
String)`, a package-private `RaftHAServer` passthrough, and one changed line in
`RaftHAPlugin.connectCluster` - rather than by adding a fourth `priority` argument. The two are
equivalent today; taking the peer whole makes losing the *next* field impossible instead of merely
tested for, and the same defect had by then occurred twice. The three-argument overload stays for
`POST /api/v1/cluster/peer`, whose payload has no priority to pass.

`Issue7401JoinPriorityTest` pins it from `parseJoinTarget` through to the `SetConfigurationRequest`,
which is the last point the value can be lost. It was checked against the defect before being trusted:
reintroducing the rebuild turns three of its four cases red (`expected: 7 but was: 0`), and the fourth -
an entry with no priority, which must still be 0 - correctly stays green.

`RaftAtomicMembershipTest` was deliberately **not** touched, per this project's rule against modifying
existing tests; the new coverage lives in its own class.

### Second review cycle

"A few minor points, no blockers." One was a real accuracy defect in this document and is fixed above:
the coverage table claimed the not-started guard had the same "call shape" as its siblings, and the
exception *type* is not the same - they throw bare `RuntimeException`, this throws `ServerException`.
The claim was narrowed to what `grep` actually shows, the type was kept (nothing observable turns on
it, and it is the more descriptive one), and the six siblings were left alone as unrelated churn.

The other two points were flags rather than requests and needed no change: `http.connect-cluster` now
counts successful joins instead of attempts, which is the house pattern every other wrapper in
`PostServerCommandHandler` already follows and is annotated in `docs/7304-grpc-control-plane.md`; and
the known gaps were noted as honestly disclosed.

### What the reviewer got wrong, and the evidence

The same review reported that the PR description "describes a `LeaderCommandForwarder` that forwards
`POST/PUT/DELETE /api/v1/server/users` to the Raft leader, closing #7380". It does not, and never did:
`gh pr view 7517 --json body` returns a body whose first line is `Closes #7401` and whose summary is
this verb. Nothing was changed in response. Reported back on the thread rather than silently ignored.

### A note on how the fix arrived

An automated fix pass wrote its own version of this fix into the worktree while the review was in
flight. It added the 4-argument plumbing and a javadoc saying "Priority carries through too" - **but
left `RaftHAPlugin.connectCluster` calling the three-argument overload**, so the priority was still
dropped and the new javadoc asserted something the code did not do. Its test passed only because it
called `RaftClusterManager.addPeer` with four arguments directly, bypassing the unwired call site. It
also edited an existing test method. Those edits were stashed rather than committed, and the fix above
was written and verified from scratch. The follow-up it filed, **#7523**, is accurate and is kept.

## Follow-ups filed before the PR opened

- **#7514** - adding an unreachable peer hangs about 60 s and reports a raw Ratis
  `SetConfigurationRequest` as the error. Found by this work, affects `POST /api/v1/cluster/peer`
  equally, and is the most likely operator mistake with either entry point.
- **#7515** - there is still no way to make an already-running node join a cluster it was not configured
  for, which is what this verb meant before the Raft stack landed. Out of scope because it is not an
  argument mapping: it has to decide what happens to the local Raft log, and doing it silently is the
  split-brain the leader-side membership change prevents.
- **#7523** - `POST /api/v1/cluster/peer` still cannot set a joining peer's priority: its JSON payload
  has no field for one. Raised by the priority fix above, and left there deliberately - extending that
  HTTP contract is a different surface from this issue's verb.

## Residual risk

What this change does **not** cover, in plain language:

1. **The joined server must already be running and reachable.** Naming one that is not costs about a
   minute and produces an unreadable error (#7514). The verb does not probe first.
2. **It cannot make this node join someone else's cluster** (#7515). The address names the server being
   added, not a cluster to be joined - the reversal of the pre-Raft meaning, argued in
   `ServerControlPlane.connectCluster`'s javadoc and in this document.
3. **No leader routing of its own.** A request landing on a follower works because the Ratis client
   under the membership change reaches the leader - asserted by
   `Issue7401ConnectClusterJoinsPeerIT.connectClusterIssuedOnAFollowerStillJoinsThePeer`, not assumed -
   but nothing in the HTTP layer forwards it, exactly as `POST /api/v1/cluster/peer` does not.
4. **No Studio control.** `grep` over the static resources finds no UI for either half of the pair, and
   this change adds none; it was not there to fix.
5. **The users seed is best-effort**, as it is on the add-peer route it copies. A seed that fails leaves
   the peer a committed member with a stale user set until the next cluster-wide user change, and says
   so at WARNING.

Nothing else: the coverage table above is the evidence, and its two argued rows carry their reasons.

## Pull request

<https://github.com/ArcadeData/arcadedb/pull/7517>

## Review cycles

| # | Head | What the review said | What changed |
|---|---|---|---|
| 1 | `225b3c2` | One real defect: the leader-election priority was parsed and then dropped, because `connectCluster` passed `addPeer` an id, address and name and `addPeer` rebuilt a fresh `RaftPeer`. Plus a finding that the PR description described a `LeaderCommandForwarder` closing #7380 | Fixed by handing the parsed `RaftPeer` over whole, with `Issue7401JoinPriorityTest` pinning it from the parse to the `SetConfigurationRequest`. The description finding did not hold - `gh pr view 7517 --json body` starts `Closes #7401` - and was answered on the thread |
| 2 | `0dd64bf` | "A few minor points, no blockers." The not-started guard's claimed "call shape" parity with its siblings was wrong on the exception type; the `http.connect-cluster` metric now counts successes rather than attempts; known gaps honestly disclosed | The inaccurate claim was narrowed to what `grep` shows, in this document and in the PR body. The metric point was a flag, not a request, and matches the house pattern; no change |
| 3 | `0226b4d` | "No blocking issues found." One non-blocking suggestion to trim this document's process narrative post-merge | Kept, with the reasoning answered on the thread: the javadoc and the OpenAPI description are the reference documentation, and the two sections named are the near-miss record a later reader could not reconstruct |

An automated fix pass also wrote its own version of the priority fix into the worktree during cycle 1.
It left the call site unwired, so the bug survived while a new javadoc claimed otherwise, and it edited
an existing test method. Those edits were stashed, not committed (`git stash list`, two entries labelled
"superseded"; a patch copy is outside the repo). The follow-up it filed, #7523, is accurate and kept.

## Deferred items

None. No `review-deferred-*.md` notes file was produced by any cycle of this PR - every finding was
either fixed in the branch, answered with evidence on the thread, or filed as a follow-up issue.

## Final state

`clean-approval` after 3 cycles. Merge is the developer's.
