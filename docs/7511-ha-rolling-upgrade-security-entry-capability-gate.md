# #7511 - HA rolling upgrade: a group or API-token change halts every node older than #7373's entry types

Branch: `fix/7511-ha-rolling-upgrade-entry-type-gate`

## The defect

#7373 added two Raft log entry types, `SECURITY_GROUPS_ENTRY` (id 7) and `SECURITY_API_TOKENS_ENTRY`
(id 8). `ArcadeStateMachine.applyTransaction` refuses to skip a committed entry whose type byte it
does not recognise and halts the node instead (`triggerCriticalHalt()`, the #4798 rule). That halt is
correct: skipping the entry would diverge the cluster's security state silently.

What was missing is anything that stops the entry being written in the first place. During a rolling
upgrade the cluster is mixed by construction, so creating a group or minting an API token against an
already-upgraded node committed an entry that halted every node still on the old build - a routine
admin action turning a routine upgrade into a partial outage, with nothing in the API response saying
the cluster was not ready for it.

## The invariant the fix establishes

> A Raft log entry whose type byte did not exist in every supported predecessor build is never
> submitted while any peer of the current Raft configuration has failed to prove it can decode that
> type.

## Analysis

The mechanism this needs already exists. Issue #7219 built a peer capability handshake for exactly
this shape of problem (an optional trailing section of `SCHEMA_ENTRY` that an older peer silently
ignored): `PostCapabilitiesHandler` answers what a node can decode, `PeerCapabilityQuery` asks,
`PeerCapabilityRegistry` caches the answers with a TTL, and `RaftHAServer.peersMissingCapability`
names the peers that have not proved they support a token. A node predating the route answers 404,
which is the whole discriminator - no version parsing, and none would be safe.

The difference between #7219's consumer and this one is what happens on a "no":

* a schema delta has a **degraded mode** - ship the whole document - so #7219 withholds the delta and
  logs;
* an entry type has **no degraded mode**. The entry either is written, and halts the old peer, or it
  is not written at all. So this gate must refuse the operation.

## Completeness

### Enumerating every way to violate the invariant

Writers of the two entry types:

```
$ grep -rn "encodeSecurityGroupsEntry\|encodeSecurityApiTokensEntry" --include="*.java" . | grep /src/main/
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftTransactionBroker.java:450:    final ByteString entry = RaftLogEntryCodec.encodeSecurityGroupsEntry(groupsJson);
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftTransactionBroker.java:458:    final ByteString entry = RaftLogEntryCodec.encodeSecurityApiTokensEntry(apiTokensJson);
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java:679:  public static ByteString encodeSecurityGroupsEntry(final String groupsJson) {
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryCodec.java:687:  public static ByteString encodeSecurityApiTokensEntry(final String apiTokensJson) {
```

Callers of the broker methods, and implementations of the plugin hook they sit behind:

```
$ grep -rn "getTransactionBroker().replicateSecurity" --include="*.java" .
./ha-raft/.../RaftHAPlugin.java:209:      raftHAServer.getTransactionBroker().replicateSecurityUsers(usersJsonArray);
./ha-raft/.../RaftHAPlugin.java:224:      raftHAServer.getTransactionBroker().replicateSecurityGroups(groupsJson);
./ha-raft/.../RaftHAPlugin.java:239:      raftHAServer.getTransactionBroker().replicateSecurityApiTokens(apiTokensJson);

$ grep -rn "implements HAServerPlugin" --include="*.java" . | grep /src/main/
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java:52:public class RaftHAPlugin implements HAServerPlugin, HAReplicationStatsProvider {
```

`RaftHAPlugin` is the only `HAServerPlugin` implementation in `src/main` and the only caller of the
broker's two security-config methods, so its `replicateSecurityGroups` / `replicateSecurityApiTokens`
are a single chokepoint for every submit of entry types 7 and 8. The `HAServerPlugin` defaults are
no-ops for a non-HA server, which submits nothing.

Callers of the plugin hooks - the operator-facing entry points:

```
$ grep -rn "replicateSecurityGroups\|replicateSecurityApiTokens" --include="*.java" . | grep /src/main/ | grep -v ha-raft
server/.../ServerSecurity.java:1024:      ha.replicateSecurityGroups(groupsDocumentWith(database, name, groupConfig).toString());
server/.../ServerSecurity.java:1043:      ha.replicateSecurityGroups(root.toString());
server/.../ServerSecurity.java:1158:      ha.replicateSecurityGroups(getGroupsJsonPayload());
server/.../ServerSecurity.java:1165:      ha.replicateSecurityApiTokens(getApiTokensJsonPayload());
server/.../ServerSecurity.java:1187:      ha.replicateSecurityApiTokens(minted.documentJson());
server/.../ServerSecurity.java:1208:      ha.replicateSecurityApiTokens(document);
```

Siblings - the same shape of bug elsewhere. Every `RaftLogEntryType` constant, and when its id first
shipped:

```
$ grep -n "((byte)" ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftLogEntryType.java
22:  TX_ENTRY((byte) 1),
23:  SCHEMA_ENTRY((byte) 2),
24:  INSTALL_DATABASE_ENTRY((byte) 3),
25:  DROP_DATABASE_ENTRY((byte) 4),
26:  SECURITY_USERS_ENTRY((byte) 5),
34:  BOOTSTRAP_FINGERPRINT_ENTRY((byte) 6),
40:  SECURITY_GROUPS_ENTRY((byte) 7),
45:  SECURITY_API_TOKENS_ENTRY((byte) 8);

$ git tag --contains bbc64d81340ae1909263250b49656efba752dcc0 | head -5      # the id-6 commit
26.5.1
26.6.1
26.7.1
26.7.2
26.7.3
```

Ids 1-5 came in with the Raft HA work itself (`f6afe41595`), id 6 shipped in 26.5.1, and ids 7 and 8
are unreleased (`pom.xml` is `26.10.1-SNAPSHOT`). So ids 7 and 8 are the only ones a supported
rolling upgrade can meet on a peer that cannot decode them.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `POST /api/v1/server/groups` -> `ServerControlPlane.saveGroup` -> `ServerSecurity.saveGroupClusterWide` -> `RaftHAPlugin.replicateSecurityGroups` | yes | yes |
| `DELETE /api/v1/server/groups` -> `ServerControlPlane.deleteGroup` -> `deleteGroupClusterWide` -> same hook | yes | yes |
| gRPC `SaveGroup` / `DeleteGroup` -> `ServerControlPlane` -> same hook | yes | yes (status mapping) |
| `POST /api/v1/server/api-tokens` -> `createApiTokenClusterWide` -> `RaftHAPlugin.replicateSecurityApiTokens` | yes | yes |
| `DELETE /api/v1/server/api-tokens` -> `deleteApiTokenClusterWide` -> same hook | yes | yes |
| gRPC `CreateApiToken` / `DeleteApiToken` -> `ServerControlPlane` -> same hook | yes | yes (status mapping) |
| `POST /api/v1/cluster/addPeer` -> `PostAddPeerHandler` -> `seedSecurityStateClusterWide` (groups + API-token halves) | yes | yes |
| `SECURITY_USERS_ENTRY` (id 5) from `createUserClusterWide` / `dropUserClusterWide` / the users seed | **argued** | - |
| `BOOTSTRAP_FINGERPRINT_ENTRY` (id 6) from `BootstrapElection` | **argued** | yes (the exhaustive switch) |
| `SCHEMA_ENTRY` (id 2) trailing schema-delta section | **argued** | - |
| Studio does not surface cluster readiness for these operations | **filed** | - |
| `GET /api/v1/cluster` answers capabilities only from the leader | **filed** | - |

Arguments, with evidence:

* **`SECURITY_USERS_ENTRY` (id 5)** - introduced by `f6afe41595`, the commit that introduced Raft HA
  itself, so no build that has ever spoken this protocol fails to decode it. The issue says the same.
  Its optional `#7138` extension section is skip-tolerant by construction, not halt-on-unknown.
* **`BOOTSTRAP_FINGERPRINT_ENTRY` (id 6)** - shipped in 26.5.1 (tag evidence above), and is written
  only by `BootstrapElection` at first cluster formation with
  `arcadedb.ha.bootstrapFromLocalDatabase=true`. It is deliberately NOT gated: refusing it would stop
  the cluster forming rather than protect a peer, which is the opposite of what the gate is for. That
  decision is not left to memory - `SecurityEntryCapabilityGate.capabilityFor` is an exhaustive
  `switch` over `RaftLogEntryType` with no `default`, so adding a ninth constant fails to compile
  until whoever adds it decides whether it needs a capability token.
* **`SCHEMA_ENTRY` delta section** - already gated, by the mechanism this fix reuses
  (`RaftReplicatedDatabase.schemaDeltaEnabled`, issue #7219). Its "no" degrades to the whole document
  rather than refusing, because it has a degraded mode and an entry type does not.

## The fix

1. `PeerCapabilities` gains two tokens, `security-groups-entry` and `security-api-tokens-entry`, and
   advertises both in `LOCAL`. They name what a receiver can DECODE, per that class's contract.
2. `SecurityEntryCapabilityGate` maps a `RaftLogEntryType` to the token a peer must advertise before
   the leader may write one, through an exhaustive switch, and refuses with a message that names the
   lagging peers and the reason each is unknown (`PeerCapabilityRegistry.unknownReasonOf`).
3. `RaftHAPlugin.replicateSecurityGroups` / `replicateSecurityApiTokens` run the gate before handing
   the payload to the transaction broker, so nothing is submitted on a refusal.
4. The refusal is `ClusterCapabilityNotReadyException`, a `ServerControlPlane.OperationNotAvailableException`
   subtype: gRPC already maps that parent to `FAILED_PRECONDITION`, and a new arm in
   `AbstractServerHttpHandler` answers HTTP `409 Conflict` - the two statuses the issue asks for.
5. `RaftHAServer.peersMissingCapabilityNow` answers the gate. It reads the registry first and, only
   when that says something is missing, runs ONE synchronous capability round and re-reads. The
   background capability monitor runs on the leader only (#7219), so without the on-demand round a
   group change served by a FOLLOWER - the REST routes do not forward, `PostGroupHandler` calls
   `ServerControlPlane` directly - would see an empty registry and refuse every time.
6. `arcadedb.ha.securityEntryCapabilityGate` (default `true`) turns the gate off for an operator who
   knows every node understands the entries but cannot probe one of them.

## Residual risk (first assessment, before the adversarial pass)

* **A peer that cannot be probed blocks group and API-token changes, revocations included.** "Every
  unknown is a no" is what makes the gate safe - an old build and an unreachable node are the same
  404/timeout at the transport - but it means an admin operation is refused while any node is down,
  not only while any node is old. The escape hatch is the setting in (6), and the refusal message
  names it along with the peer and the reason. Deliberate: the alternative is halting that node when
  it returns.
* **The gate is a leader-and-follower check on the submitting node, not a cluster-wide handshake.**
  It asks every peer of the current Raft configuration, which is the set that will apply the entry;
  a node outside the configuration is not sent entries at all.
* **Studio is untouched** - filed, see the follow-ups below.

## Tests

| Test | Module | What it drives |
|---|---|---|
| `Issue7511SecurityEntryCapabilityGateTest` (16) | `ha-raft` | the tokens, `capabilityFor`'s exhaustive mapping, the decision, the refusal text, and - through `RaftHAPlugin.replicateSecurityGroups` / `replicateSecurityApiTokens` with a mocked broker - that a refusal submits NOTHING |
| `Issue7511SecurityEntryGateRefusalTest` (7) | `server` | every operator-facing entry point: save/delete group, mint/revoke token, and the `addPeer` seed, each asserting the refusal propagates AND that the node serving it kept no half-change |
| `Issue7511ClusterNotReadyHttpStatusTest` (3) | `server` | the HTTP 409, wrapped and unwrapped, and that a plain `OperationNotAvailableException` is NOT swept into it (catch order) |
| `Issue7511GrpcClusterNotReadyStatusTest` (2) | `grpcw` | the gRPC `FAILED_PRECONDITION` through `ArcadeDbGrpcAdminService.toStatus`, the mapper every admin RPC delegates to |
| `Issue7511MixedVersionSecurityEntryIT` (1, `@Tag("slow")`) | `ha-raft` | a REAL 3-node cluster with one node advertising a pre-#7373 capability set: the leader refuses, an upgraded FOLLOWER refuses by asking the peers itself, the lagging node is still running, and the change goes through and converges on all three once it finishes upgrading |

### Proving the tests can fail

The gate call was removed from both `RaftHAPlugin` methods and the ha-raft suite re-run:

```
[ERROR] Tests run: 16, Failures: 2, Errors: 0
[ERROR]   Issue7511SecurityEntryCapabilityGateTest.aRefusedApiTokenChangeSubmitsNothingToTheRaftLog:269
[ERROR]   Issue7511SecurityEntryCapabilityGateTest.aRefusedGroupChangeSubmitsNothingToTheRaftLog:250
```

Exactly the two caller tests, and only those two. The same removal was then run against the integration test:

```
[ERROR] Tests run: 1, Failures: 1 -- Issue7511MixedVersionSecurityEntryIT
Expecting code to raise a throwable.      (x2: the leader arm and the follower arm)
```

so the IT is not passing because something else refuses. The call was then restored (`git diff` back to +36 lines).

### Regression runs

```
mvn -o -pl ha-raft -am test -Dtest='Issue7219CapabilityAdvertisementTest,Issue7219PeerCapabilityRegistryTest,
  Issue7219SchemaEntryTrailingSectionTest,Issue7256SharedAddressCapabilityProbeTest,Issue7301PeerCapabilityReportingTest,
  Issue7331CapabilityForgottenAtProbeFailureTest,Issue7332SharedEndpointPortOffsetTest,Issue7373SecurityConfigEntryCodecTest,
  Issue7373SecurityConfigBrokerEntryTest,Issue7138EntryExtensionSectionTest,RaftLogEntryCodecTest,RaftTransactionBrokerTest,
  HAConfigDefaultsTest,Issue7511SecurityEntryCapabilityGateTest'
  -> Tests run: 144, Failures: 0, Errors: 0

mvn -o -pl server -am test -Dtest='com.arcadedb.server.security.*Test'
  -> Tests run: 89, Failures: 0, Errors: 0          (includes Issue7373ClusterWideGroupsAndTokensTest, 24)

mvn -o -pl server -am test -Dtest='Issue7511ClusterNotReadyHttpStatusTest,Issue5064CommittedRemotelyHttpStatusTest'
  -> Tests run: 8, Failures: 0, Errors: 0

mvn -o -pl grpcw -am test -Dtest='Issue7511GrpcClusterNotReadyStatusTest,Issue7443GrpcMaintenanceSlotStatusTest'
  -> Tests run: 4, Failures: 0, Errors: 0

mvn -o -pl engine test -Dtest='ConfigurationTest,ContextConfigurationTest,GlobalConfigurationTest,
  GlobalConfigurationReadinessHATest,Issue7124BooleanSettingStrictCoercionTest,Issue7163DatabaseScopeCallbackTest,Issue6875*'
  -> Tests run: 79, Failures: 0, Errors: 0
```

## Reachability

The changed code runs on a live path, not only under the new tests:

* `RaftHAPlugin` is the only `HAServerPlugin` implementation in `src/main` (grep above) and is installed by the
  server whenever HA is requested, so both gated methods are the real ones every entry point calls.
* `PeerCapabilities.LOCAL` is what `RaftHAServer.advertisedCapabilities` is initialised from and what
  `PostCapabilitiesHandler` answers with, so the two new tokens go out on the wire from this build with no further
  wiring. That also means a peer running this build answers "yes" to both, which is what makes a fully upgraded
  cluster pass the gate.
* No new setting gates the mechanism off: `arcadedb.ha.securityEntryCapabilityGate` defaults to `true`, and the
  gate additionally reads `true` when no server configuration can be reached at all.

## Follow-ups filed before the PR

* **#7548** - Studio does not surface that the cluster is not ready for group / API-token changes.
* **#7549** - `GET /api/v1/cluster` reports peer capabilities only from the leader, so a readiness check has to
  find the leader first.

## Adversarial pass

The orchestrator's Phase 1.5 spawns an independent subagent for this. The `Task` tool was **disabled in this
session** ("No such tool available: Task"), so the pass was run by the author instead - which removes its main
value, independence - and each finding below was checked against the tree rather than reasoned about. That
limitation is recorded rather than papered over.

| # | Finding | Disposition |
|---|---|---|
| 1 | The gate asks about peers and never about the local node (`RaftHAServer.peersMissingCapability`, `if (!peer.getId().equals(localPeerId))`), so a change issued ON a lagging node is not refused. | **Not a defect.** The gate ships with the decoder: a node old enough to halt has no gate to run, and a node that can decode the entry is not a peer that would halt on it. It is, however, why the IT needs three nodes rather than two - the follower arm has to be exercised from a follower that is not the lagging node. |
| 2 | A declared-but-not-joined peer is not asked. | **Not a defect**, verified: `configuredPeers()` is `ClusterMembership.of(raftGroup.getPeers(), getLivePeers()).configuredPeers()`, the live configuration. A peer outside it is not sent entries, so it cannot halt on one. |
| 3 | An unreachable peer is refused exactly like an old one, so a group change - or a token REVOCATION during an incident - is blocked while any node is down. | **Real, in scope, accepted.** Inherent to "every unknown is a no", which is what makes the gate safe. Mitigated by `arcadedb.ha.securityEntryCapabilityGate`, named in the refusal text, and documented in Residual risk and in `ha-raft/CLAUDE.md`. |
| 4 | `POST /api/v1/cluster/addPeer` now reports `groups` / `API tokens` among its failed seeds whenever the just-added peer cannot be probed yet, where before the seed was submitted regardless. | **Real, in scope, accepted and reported rather than thrown** - `seedSecurityStateClusterWide` is best-effort per document by design, and `Issue7511SecurityEntryGateRefusalTest` pins that the users seed still goes through. The consequence of a failed seed is already tracked by **#7521**. |
| 5 | The on-demand probe runs on the request thread while the `ServerSecurity` monitor is held, for up to `peers x PeerCapabilityRegistry.PROBE_TIMEOUT_MS` (2 s each). | **Bounded and checked.** `grep -n synchronized server/.../ServerSecurity.java` shows every monitor acquisition is on an admin-mutation path; `authenticate` (line 215) and `authenticateByApiToken` (line 1387) are NOT synchronized, so login is unaffected. What it delays is other security-admin mutations, which already block on a full Raft round trip. |
| 6 | A rolling DOWNGRADE is still unprotected: an entry of type 7 or 8 already committed to the durable Raft log halts a node restarted onto a build that predates it. | **Real and out of reach of any gate**, for the reason `ha-raft/CLAUDE.md` already sets out for #7255: negotiation governs what is written next, never what is committed, and the node in trouble is the one without the check. Not filed as a new issue - it is the documented operational boundary of the whole mechanism ("a node being rolled back that far is rebuilt from a snapshot"), now covering entry types as well as sections. |
| 7 | A follower's `GET /api/v1/cluster` still reports no peer capabilities, so readiness cannot be polled anywhere but the leader. | **Filed as #7549.** |
| 8 | Studio offers the group and token controls unconditionally and renders the 409 as a generic error. | **Filed as #7548.** |

## Residual risk

Restated with what the adversarial pass added:

* An unreachable peer blocks group and API-token changes, revocations included (finding 3). The escape hatch is
  `arcadedb.ha.securityEntryCapabilityGate=false`, and the refusal names it.
* `addPeer` reports a refused groups/API-token seed rather than silently submitting one (finding 4); #7521 tracks
  what admitting such a peer means.
* A rolling downgrade past this release is not protected and cannot be (finding 6) - the same boundary #7255
  documents for schema deltas.
* Studio and a follower's cluster payload still say nothing about readiness: #7548, #7549.
* Nothing else. Every row of the coverage table is fixed here, filed, or argued with evidence above.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7555

## Review cycles

### Cycle 1 - `771f11b` - 3 findings, all addressed, none deferred

**CodeRabbit, `AbstractServerHttpHandler.java:761`, Major - "Preserve the cluster refusal details in production
HTTP responses."** *Valid, and the most important finding of the review.* Verified against
`buildErrorBody` (line 1628): `detail` is emitted only when `verbose`, i.e. not when
`arcadedb.server.mode=production`, while `exceptionArgs` is emitted in every mode. The 409 arm passed `null` there,
so in production the entire refusal reached the client as `{"error":"Cluster is not ready for this operation"}` -
no peer, no capability. That is the silence this issue exists to end, reintroduced one mode over.

Fixed, though **not** with the suggested `capabilityNotReady.getMessage()`. `exceptionArgs` is a wire contract
(`RemoteHttpComponent.manageException`, `RaftReplicatedDatabase.reconstructLeaderException`) documented as carrying
"bounded, non-sensitive values"; the message is neither. `ClusterCapabilityNotReadyException` now carries the
capability and the missing peers as fields and renders `toExceptionArgs()` as
`security-groups-entry|arcadedb2[,+N more]` - pipe-separated like `DuplicatedKeyException`'s, capped at five peers.
The per-peer REASONS stay in the message: a reason is free-form probe-failure text that can carry a host, a port or
a JDK exception message, which is the class of content `detail` is concealed for. Covered by
`inProductionModeThePeerAndTheCapabilitySurviveInExceptionArgs` (production mode, asserts `detail` absent,
`exceptionArgs` naming the peer and the capability, and the 404 reason NOT leaking into it) and
`theExceptionArgsAreBoundedOnALargeCluster`. Falsified: reverting the arm to `null` fails the new test with
`JSONObject[exceptionArgs] not found` and nothing else.

**claude, observation 1 - the on-demand probe is held under a monitor shared with USER administration.** Valid
refinement of finding 5 of the adversarial pass, which had only said "other security-admin mutations". Verified:
`ServerSecurity.createUser` (line 301) is `synchronized` on the same monitor, so an unreachable peer delays the next
`createUser` as much as the next group change. No behaviour change - the trade is deliberate - but claude's
suggested "metric/log line" was worth taking: `peersMissingCapabilityNow` now logs the round at FINE with its
elapsed time and what it concluded, so the stall is attributable, and both the gate's and the method's javadoc say
the monitor is shared with user administration.

**claude, observation 2 - a request-thread round can overlap the leader's background round.** Valid, harmless, and
now written down rather than left for the next reader to re-derive. Documented on `peersMissingCapabilityNow`: the
interleaving cannot produce a wrong CAPABLE (recording a capability requires a peer to have answered with it), and
a shared lock was rejected rather than forgotten, because this method runs on a thread already holding the
`ServerSecurity` monitor and making the capability-monitor thread wait on the same lock would put a monitor-held
wait on both sides of a cycle.

No deferred items.
