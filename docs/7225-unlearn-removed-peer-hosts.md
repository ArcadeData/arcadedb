# Issue #7225 - hosts learned from the live Raft configuration are never unlearned

Follow-up to #7132. Two findings, both consequences of #7132 landing.

## Finding ledger

- [x] 1. `learnedHosts` is add-only, so a peer removed from the Raft configuration keeps inbound Raft
      gRPC access (and a DNS lookup per refresh tick) for the rest of the process lifetime.
- [x] 2. The startup fail-open log line counts `lastKnownIps`, which learned hosts now share with
      configured hosts, so it can report a resolved count larger than the configured host count.

Status at PR time: **1 fixed** (replace semantics + sticky pruning), **2 fixed** (the message counts configured
hosts only). One gap found in the adversarial pass and filed as **#7250**.

## Root cause

`PeerAddressAllowlistFilter.learnPeerHosts` merges into `learnedHosts` (`merged.addAll(added)`) and the
class has no removal path at all. `RaftHAServer.refreshPeerAllowlist` feeds it the hosts of the live
Raft configuration on every health-monitor tick, so membership growth is reconciled and membership
*shrink* is not: the departed host stays in `learnedHosts`, keeps being re-resolved by `doResolve()`,
and keeps contributing its IPs to `allowedIps`.

The same shared state carries finding 2: `lastKnownIps` is keyed by every host the filter has ever
resolved - configured *and* learned - while the fail-open message prints it against
`peerHosts.size()`. On a Kubernetes node with the headless-service seed plus a learned member, the
message reads "resolved 3/1 hosts".

One host must survive a membership shrink: the Kubernetes headless-service domain seeded by #7132 in
`installPeerAllowlist`. That is what admits a scale-up pod *before* it is a member, so it cannot be
treated as just another learned host under replace semantics.

## Completeness

### 1. Invariant

After a reconciliation that reads a committed Raft configuration, the filter's learned host set is
exactly the hosts of that configuration plus the hosts explicitly pinned at install time; a host that
has left the configuration contributes no IP to `allowedIps` and no entry to the sticky
`lastKnownIps`/`lastKnownMs` maps. Separately, the startup fail-open message reports a resolved count
that counts only configured hosts, so it can never exceed the configured host count.

### 2. Paths found by command

```
$ grep -rn "learnedHosts *=" ha-raft/src/main ha-raft/src/test
PeerAddressAllowlistFilter.java:106:  private volatile Set<String> learnedHosts = Collections.emptySet();
PeerAddressAllowlistFilter.java:270:      learnedHosts = Collections.unmodifiableSet(merged);
PeerAddressAllowlistFilter.java:451:    return "PeerAddressAllowlistFilter{peerHosts=" ... learnedHosts ...
```

Exactly one writer: `learnPeerHosts` (line 270). No `unlearn`, `forget`, `remove` or `set` sibling.

```
$ grep -rn "learnedHosts" ha-raft/src/main | grep -v "learnedHosts *="
PeerAddressAllowlistFilter.java:218:  isAllowed() reject log line
PeerAddressAllowlistFilter.java:261:  learnPeerHosts() dedup check
PeerAddressAllowlistFilter.java:268:  learnPeerHosts() merge
PeerAddressAllowlistFilter.java:282:  getLearnedHosts()
PeerAddressAllowlistFilter.java:331:  doResolve() resolution loop
```

```
$ grep -rn "learnPeerHosts" --exclude-dir=target .
RaftHAServer.java:3923:      filter.learnPeerHosts(List.of(serviceDomain));   <- k8s headless-service seed
RaftHAServer.java:3976:    filter.learnPeerHosts(memberHosts);              <- health monitor tick
Issue7132AllowlistLearnsRuntimePeersTest.java:70,83,84,85,88,89,108,124   <- tests
```

Two production callers, and they want different semantics: the first pins, the second reconciles.

```
$ grep -rn "lastKnownIps\|lastKnownMs" ha-raft/src/main ha-raft/src/test
PeerAddressAllowlistFilter.java:120,121  declarations
PeerAddressAllowlistFilter.java:207      fail-open log line: lastKnownIps.size() vs peerHosts.size()
PeerAddressAllowlistFilter.java:366,367  written per resolved host
PeerAddressAllowlistFilter.java:374,375  read for sticky retention
PeerAddressAllowlistFilter.java:380,381  removed when the sticky TTL expires
```

Line 207 is also the only read of `lastKnownIps` *outside* `doResolve()`'s monitor, i.e. an unsynchronised
read of a plain `HashMap`. Replacing it with a `volatile int` fixes the count and the race in one move.

```
$ grep -rn "refreshPeerAllowlist" --exclude-dir=target .
HealthMonitor.java:91    HealthTarget default no-op
HealthMonitor.java:271   tick() -> target.refreshPeerAllowlist()   <- runs on EVERY node
RaftHAServer.java:3960   the implementation
HealthMonitorTest.java:78 test double
```

```
$ grep -rn "removePeer" ha-raft/src/main
DeletePeerHandler.java:57 -> RaftHAPlugin.removePeer -> RaftHAServer.removePeer -> RaftClusterManager.removePeer
```

`removePeer` runs `setConfiguration` on the leader; every node - leader included - observes the shrink
through its own committed configuration, which is what the health-monitor tick already reads.

### 3. Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `HealthMonitor.tick` -> `RaftHAServer.refreshPeerAllowlist` -> membership shrink | yes - replace semantics via `setMemberHosts` | yes - `reconcileAllowlistMembership` shrink test |
| `RaftHAServer.refreshPeerAllowlist` while the committed configuration is unreadable (#5271 window) | yes - membership left untouched rather than replaced by the declared list | yes - detached-server test |
| `RaftHAServer.installPeerAllowlist` k8s headless-service seed | yes - pinned, survives every shrink | yes - pin-survives-shrink test |
| `PeerAddressAllowlistFilter.learnPeerHosts` (pin API) | unchanged, add-only by design | yes - existing #7132 tests, unmodified |
| `doResolve()` sticky maps for a dropped host | yes - `lastKnownIps`/`lastKnownMs` pruned to tracked hosts | yes - sticky-revival test |
| `isAllowed` startup fail-open log count | yes - counts configured hosts only | yes - count test |
| `DELETE /api/v1/cluster/peer/{id}` -> `removePeer` | argued: not a separate write path. It commits a new configuration; the reconciliation is the tick that reads it, at most one tick period later | n/a |
| An already-established transport from the removed peer | **filed as #7250** - `transportReady` is the only gate the filter has, and nothing closes a live transport | n/a |
| A departed peer that is ALSO in `arcadedb.ha.serverList` | argued: `serverList` is the declared allowlist and is deliberately not reconciled from membership; removing a declared host is a configuration change | n/a |

No blank rows.

### 5. Reachability

`HealthMonitor.tick()` line 271 calls `target.refreshPeerAllowlist()` unconditionally on every node,
before any lifecycle-state branch, so the reconciliation runs wherever the health monitor runs - the
same path #7132 already relies on. `installPeerAllowlist` is called from `buildParameters`, which is on
the Raft startup path. Neither is behind a new flag; `arcadedb.ha.peerAllowlist.enabled` (default true)
gates the whole filter exactly as before.

## Changes

`ha-raft/src/main/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilter.java`

- `learnedHosts` is split into two sets with deliberately different lifetimes: `pinnedHosts`, written only by
  `learnPeerHosts` and never unlearned, and `memberHosts`, replaced wholesale by the new `setMemberHosts`.
  `learnedHosts` stays as their union, republished under the monitor, so the resolver, `getLearnedHosts()` and
  the log lines keep working off one consistent volatile read.
- `setMemberHosts(Collection)` - replace semantics, re-resolves only when the set actually changed, logs at
  INFO the hosts that left. Configured hosts are skipped on the way in; pinned hosts are untouched.
- `doResolve()` prunes `lastKnownIps`/`lastKnownMs` down to the tracked hosts, so a departed peer's sticky
  last-known-good IPs cannot readmit it later.
- The startup fail-open message now reports `resolvedPeerHosts` (configured hosts covered by the last
  resolution) instead of `lastKnownIps.size()`. That fixes the count and removes the message's unsynchronised
  read of a plain `HashMap`.
- The reject message names the members and the pinned hosts separately, since "learned" now covers both.

`ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

- `refreshPeerAllowlist()` delegates to the new package-private `reconcileAllowlistMembership(Collection)` and
  feeds it `getCommittedPeersOrNull()` rather than `getLivePeers()`. `getLivePeers()` substitutes the DECLARED
  server list when the division cannot be read (#5271), and under replace semantics that would unlearn every
  runtime-joined peer for the whole restart window - the #7132 regression in a new shape. `null` means "no
  membership this tick" and leaves the membership alone; so does a peer list that reduces to no usable host.
- `installPeerAllowlist`'s headless-service seed is documented as a pin.

## Tests

`ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue7225AllowlistUnlearnsRemovedPeersTest.java`, 8 tests:

| Test | Entry point it drives |
|---|---|
| `aHostThatLeftTheConfigurationIsUnlearnedAndItsIpsAreRejected` | `setMemberHosts` shrink |
| `thePinnedHeadlessServiceDomainSurvivesAMembershipShrink` | the k8s seed under a shrink |
| `reconcilingTheSameMembershipIsANoOp` | idempotence, order-independence, configured-host exclusion |
| `aDroppedHostLosesItsStickyLastKnownIps` | `lastKnownIps`/`lastKnownMs` pruning |
| `theResolvedHostCountNeverExceedsTheConfiguredHostCount` | the fail-open log count (finding 2) |
| `reconcilingFromTheCommittedConfigurationUnlearnsARemovedPeer` | `RaftHAServer.reconcileAllowlistMembership` |
| `anUnreadableMembershipLeavesTheLearnedHostsAlone` | the `getCommittedPeersOrNull()` null case |
| `aMembershipThatCarriesNoUsableHostIsIgnored` | the degenerate-membership guard |

### The tests were proved able to fail

Three falsification runs against deliberately reverted behaviour, each restored afterwards:

1. `setMemberHosts` merging instead of replacing + the sticky prune removed + the old `lastKnownIps.size()`
   count: **6 of 8 failed** (the two that survived pin the `RaftHAServer` policy, which that revert did not
   touch).
2. `refreshPeerAllowlist` reading `getLivePeers()` instead of `getCommittedPeersOrNull()`:
   `anUnreadableMembershipLeavesTheLearnedHostsAlone` failed.
3. The `memberHosts.isEmpty()` guard removed: `aMembershipThatCarriesNoUsableHostIsIgnored` failed.

### Results

- `mvn -o -pl ha-raft test -Dtest=Issue7225AllowlistUnlearnsRemovedPeersTest` - 8/8 green.
- `mvn -o -pl ha-raft test -DexcludedGroups=benchmark,vector,slow` - **389 tests, 0 failures, 0 errors**.
  One surefire fork crash on `LeaveClusterTest` in that run is environmental, not a regression: another agent's
  server was holding port 2480 (`lsof -nP -iTCP:2480 -sTCP:LISTEN` showed a foreign java process), and
  `LeaveClusterTest` run on its own is green (2/2).

## Impact

A peer removed from the cluster - by `DELETE /api/v1/cluster/peer/{id}` or a StatefulSet scale-down - now
loses inbound Raft gRPC access on every surviving node within one health-monitor tick instead of at the next
process restart, and stops costing a DNS lookup per tick. Nothing changes for a cluster that only grows: the
scale-up path of #7132 is exercised by its own tests, which are unmodified and still green.

## Residual risk

- **A departed peer that is also declared in `arcadedb.ha.serverList` keeps its access.** By design:
  `serverList` is the operator's declared allowlist, and narrowing it is a configuration change. Removing such
  a peer from the group does not remove it from the configuration.
- **The revocation is not instantaneous.** It lands on the next health-monitor tick of each node, and each node
  ticks independently, so there is a bounded window in which some nodes still admit the departed peer.
- **The Kubernetes headless-service domain still admits every pod backing the service**, including a pod that
  was removed from the Raft group but is still running and still published by the service. That is inherent to
  the #7132 seed: the same property is what admits a scale-up pod before it joins. On a scale-down the pod is
  deleted and leaves the service's A records, which is the ordinary case.
- **An already-established gRPC transport from the removed peer is not closed.** `transportReady` is the only
  gate a `ServerTransportFilter` has; the fix revokes the ability to reconnect, not an open connection. Filed
  as **#7250**.
- **A removed peer keeps no sticky retention.** Pruning `lastKnownIps` means a peer removed and re-added
  inside the sticky window, while its DNS is simultaneously down, is not readmitted from its pre-removal IPs.
  That is the intended direction for a revocation path, but it is a behaviour change for a churn-heavy
  cluster.
- Unchanged and still true: the filter provides no peer identity and is not a substitute for mTLS (#3890).

## Adversarial pass

No `Task` tool was available in this environment, so the pass was run by re-reading the diff against the tree
rather than by a fresh subagent. Findings:

1. **Established transports are never closed** - real, out of scope, filed as **#7250** (evidence: the
   `transportTerminated`/`ServerTransportFilter` grep above; the filter's only gate is `transportReady`).
2. **The Kubernetes headless-service pin still admits a removed-but-running pod** - real, and deliberate: that
   pin is what admits a scale-up pod before it is a member, and a scale-down deletes the pod, which removes it
   from the service's A records. Argued in Residual risk rather than filed.
3. **"The health monitor may not run on every node"** - not real. `RaftHAServer` line 1042 calls
   `healthMonitor.start()` unconditionally on the startup path, and `HealthMonitor.tick()` line 271 calls
   `refreshPeerAllowlist()` before any lifecycle branch.
4. **"`learnedHosts` could drift from its two components"** - not real. `grep -n "pinnedHosts =|memberHosts ="`
   returns exactly the two writers, and both call `republishLearnedHosts()` under the same monitor.

## PR

https://github.com/ArcadeData/arcadedb/pull/7251

## Review cycles

### Cycle 1 - 9b0b7547b2

Reviewer: `claude`. No blocking findings; three items applied, none deferred.

1. **`learnPeerHosts` did not exclude `memberHosts` from its dedup, so pinning a live member would defeat
   unlearning for that host.** Real interaction, unreachable today. Not fixed by refusing the pin - that would
   make a pin evaporate at the next shrink, which is exactly when the caller wanted it. Instead the contract is
   now stated on `learnPeerHosts` (a pin beats membership, deliberately), and
   `aPinnedHostStaysAdmittedEvenWhenMembershipDropsIt` pins it as a test. This is also the interleaving the
   reviewer noted the suite did not touch.
2. **The `dropped` log line would have claimed a host stopped being admitted when a pin still admitted it.**
   Fixed: `dropped.removeAll(pinnedHosts)`, so the one line an operator reads to confirm a revocation cannot
   lie.
3. **The `memberHosts.isEmpty()` guard deserved a comment about joint-consensus reachability.** Added, phrased
   as "defensive, not expected to fire" rather than as an unreachability claim - `RaftConfiguration.getCurrentPeers()`
   during joint consensus was not exhaustively verified here.
4. Cosmetic javadoc wording on `getResolvedPeerHostCount()` - applied.

Re-run: `Issue7225AllowlistUnlearnsRemovedPeersTest` 9/9,
`Issue7132AllowlistLearnsRuntimePeersTest` 9/9 (unmodified), `PeerAddressAllowlistFilterTest` 41/41.

### Cycle 2 - 735788b509

Reviewer: `claude`. No blocking findings; two items applied, none deferred.

1. **`HealthMonitor.HealthTarget.refreshPeerAllowlist()`'s default-method javadoc had drifted** - it still
   described only the #4696 DNS reconciliation while the `RaftHAServer` override describes membership. Updated
   to name all three behaviours (#4696, #7132, #7225).
2. **The degenerate-membership guard logged at FINE.** Bumped to WARNING, matching the precedent a few lines
   above it: the `getCommittedPeersOrNull()` catch was deliberately raised from FINE to WARNING so a genuine
   bug could not hide behind a silent degradation, and this guard skipping a reconciliation is the same shape
   of hiding place. The message now also reports how many peers the configuration carried. The expected case -
   an unreadable membership during a #5271 restart window - returns before this line, so an ordinary restart
   logs nothing here.

Re-run: `Issue7225AllowlistUnlearnsRemovedPeersTest` 9/9, `Issue7132AllowlistLearnsRuntimePeersTest` 9/9,
`PeerAddressAllowlistFilterTest` 41/41, `HealthMonitorTest` 29/29.

### Cycle 3 - fad2d2706e

The `claude` comment on this SHA was the single word `test` - a degenerate bot run carrying no review
content, not an approval and not a finding. CodeRabbit reported "no actionable comments were generated".

The one thing on the PR that WAS actionable was the red `Codacy Static Code Analysis` check, which the same
check passes on comparable merged PRs (#7246, #7212, #7210). Its three findings, read from the Codacy API:

```
$ curl -s 'https://app.codacy.com/api/v3/analysis/organizations/gh/ArcadeData/repositories/arcadedb/pull-requests/7251/issues'
PMD_category_java_codestyle_FieldDeclarationsShouldBeAtStartOfClass x3
  PeerAddressAllowlistFilter.java:121  private volatile Set<String> pinnedHosts
  PeerAddressAllowlistFilter.java:124  private volatile Set<String> memberHosts
  PeerAddressAllowlistFilter.java:154  private volatile int         resolvedPeerHosts
```

All three are the same rule, and the cause is structural rather than anything about the new fields: the
`HostResolver` nested interface was declared before the field block, and PMD counts a nested type as the end
of the field section - so EVERY field in this class already violated the rule, and Codacy reports only the
ones a diff touches. Moved the interface to the bottom of the class, which clears all three and stops the
next field added to this file inheriting the same report. No semantic change; the allowlist tests, the #7132
tests and `Issue3890RaftParametersPublicationTest` are green after it.
