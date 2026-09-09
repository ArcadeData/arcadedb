# Issue #7250 - unlearning a removed peer does not close its already-established Raft gRPC transport

Follow-up to #7225 (itself a follow-up to #7132).

## Problem

`PeerAddressAllowlistFilter` gates inbound Raft gRPC connections at `ServerTransportFilter.transportReady`,
which gRPC calls exactly once per transport. #7225 taught the filter to *unlearn* a host that left the Raft
configuration, so a removed peer stops being **admitted**. Nothing revokes a transport that was already
**established** when the removal happened: gRPC/HTTP-2 connections are long-lived and carry many RPCs, so a
peer connected at the moment it was removed keeps that connection - and everything it can send on it - until
one side closes it for an unrelated reason.

The operator-facing claim was therefore "removing a peer revokes its ability to reconnect", not "removing a
peer revokes its reach".

## Root cause

`ServerTransportFilter` is a one-shot admission gate and carries no handle to the transport it admitted:

```
$ grep -n "public" /tmp/ratis-tp-src/org/apache/ratis/thirdparty/io/grpc/ServerTransportFilter.java
public abstract class ServerTransportFilter {
  public Attributes transportReady(Attributes transportAttrs)
  public void transportTerminated(Attributes transportAttrs)
}
```

Nothing in the shaded gRPC public API hands out a `ServerTransport`, and `NettyServerBuilder` exposes no
per-connection close either (checked every public method of
`org/apache/ratis/thirdparty/io/grpc/netty/NettyServerBuilder.java`: `channelType`, `channelFactory`,
`maxConnectionIdle`, `maxConnectionAge`, ... - all builder-wide, none targeted). Ratis sets none of the
connection-age settings:

```
$ grep -n "maxConnectionIdle\|maxConnectionAge\|keepAlive\|permitKeepAlive" \
    /tmp/ratis-grpc-src/org/apache/ratis/grpc/server/GrpcServicesImpl.java
(no output)
```

So the socket itself cannot be closed from where the allowlist lives. What *can* be revoked is everything the
socket is able to carry: every RPC on a gRPC server goes through the global `ServerInterceptor` chain
(`ServerImpl` applies `builder.interceptors` to every call regardless of service registration order -
`ServerImpl.java:669`), and an in-flight `ServerCall` can be closed.

## Invariant established by the fix

> An address the allowlist stops admitting also stops being able to run Raft gRPC RPCs on a transport it
> established while it was admitted: in-flight RPCs are closed with `PERMISSION_DENIED` and every new RPC on
> that transport is refused.

## Completeness

### Ways the admitted set can shrink (grep, not recall)

```
$ grep -n "allowedIps.set" ha-raft/src/main/java/com/arcadedb/server/ha/raft/PeerAddressAllowlistFilter.java
471:    allowedIps.set(Collections.unmodifiableSet(effective));
```

One writer. Every shrink therefore passes through `doResolve()`:

```
$ grep -rn "doResolve()\|resolveIfStale(" ha-raft/src/main
PeerAddressAllowlistFilter.java:186:    doResolve();            # constructor
PeerAddressAllowlistFilter.java:226:    resolveIfStale(missResolveFloor());   # isAllowed() miss path
PeerAddressAllowlistFilter.java:315:      doResolve();          # learnPeerHosts
PeerAddressAllowlistFilter.java:366:      doResolve();          # setMemberHosts
PeerAddressAllowlistFilter.java:415:    doResolve();            # refresh()
PeerAddressAllowlistFilter.java:431:    resolveIfStale(currentResolveFloor());   # proactiveRefresh()
```

Production callers of the mutators:

```
$ grep -rn "setMemberHosts\|learnPeerHosts\|proactiveRefresh" ha-raft/src/main/java --include="*.java" \
    | grep -v PeerAddressAllowlistFilter.java
RaftHAServer.java:4227:      filter.learnPeerHosts(List.of(serviceDomain));
RaftHAServer.java:4272:    filter.proactiveRefresh();
RaftHAServer.java:4320:    filter.setMemberHosts(committedHosts);
```

Same-shape siblings elsewhere in the tree - none:

```
$ grep -rn "ServerTransportFilter\|transportTerminated" --include="*.java" . | grep -v /target/
ha-raft/.../PeerAddressAllowlistFilter.java        (this class)
ha-raft/.../RaftGrpcServicesCustomizer.java        (installs it)
```

```
$ grep -rn "ServerInterceptor\|addTransportFilter\|\.intercept(" --include="*.java" ha-raft/src/main
RaftGrpcServicesCustomizer.java:44:      result = result.addTransportFilter(f);
```

No `ServerInterceptor` was installed on the Raft port before this change, so nothing competes with the new one.

### Entry-point coverage table

| # | Entry point that can shrink admission | Covered by fix? | Covered by a test? |
|---|---|---|---|
| 1 | `setMemberHosts` - peer removed from the Raft configuration (the reported case) | yes | yes - `aPeerRemovedFromTheRaftConfigurationLosesItsEstablishedTransport` |
| 2 | `RaftHAServer.reconcileAllowlistMembership` - the production wiring of row 1 | yes | yes - `theProductionReconciliationRevokesTheRemovedPeersEstablishedTransport` |
| 3 | `doResolve` via `proactiveRefresh()`/`refresh()` - a host's DNS record no longer carries the old IP | yes | yes - `aPeerWhoseAddressChangedLosesTheTransportOpenedOnTheOldAddress` |
| 4 | `doResolve` via sticky-TTL expiry - last-known-good IPs age out | yes | yes - `aTransportHeldOpenOnAnExpiredStickyAddressIsRevoked` |
| 5 | Startup fail-open window ending (quorum resolves, or the grace elapses) - a transport admitted while the filter was failing open | yes | yes - `aTransportAdmittedUnderTheStartupFailOpenIsRevokedOnceTheGraceEnds` |
| 6 | `isAllowed` miss path (`resolveIfStale`) - same `doResolve` hook as rows 3-5 | yes | yes - covered by row 3, which drives revocation through `refresh()`, and by `aNewRpcOnARevokedTransportIsRefused` |
| 7 | New RPCs started on an already-revoked transport | yes | yes - `aNewRpcOnARevokedTransportIsRefused` |
| 8 | `learnPeerHosts` - **argued**: it only ever widens. It adds to `pinnedHosts` and re-resolves; `doResolve` rebuilds `effective` from `peerHosts | learned`, so a pin cannot remove an IP. The revocation sweep runs there anyway (same `doResolve` hook), so the claim is belt-and-braces rather than load-bearing. | n/a | n/a |
| 9 | Loopback transports - **argued**: `transportReady` returns before the allow check for `address.isLoopbackAddress()`, so no session is ever created for one and nothing can revoke it. | n/a | yes - `aLoopbackTransportIsNeverTrackedAndNeverRevoked` |
| 10 | The TCP/HTTP-2 socket itself stays open after revocation | **no** - filed as #7316 | n/a |

### Residual risk

Row 10 only. After a revocation the peer's socket remains connected until it or the kernel closes it: gRPC's
public API offers no way to close one established transport, and Ratis configures neither `maxConnectionIdle`
nor `maxConnectionAge` (grep above). What that socket can still *do* is nothing - every RPC on a gRPC server
passes the global interceptor chain, and the interceptor refuses every call whose transport session is
revoked - so the residue is a lingering idle connection (a resource cost), not reach. Filed as **#7316** so
the reporter does not have to.

Also unchanged, and out of scope here for the same reasons #7225 gave: the filter provides no peer identity
and is explicitly not a substitute for mTLS (#3890), and gRPC authorization on the Raft port applies
independently.

## Changes

| File | Change |
|---|---|
| `ha-raft/.../PeerTransportSession.java` | **new** - one admitted transport and the RPCs in flight on it; the handle `ServerTransportFilter` does not give out |
| `ha-raft/.../RevocableServerCall.java` | **new** - `ServerCall` wrapper whose single-shot close is arbitrated between the handler and a revoking thread |
| `ha-raft/.../PeerAllowlistCallInterceptor.java` | **new** - enforces the admission decision per RPC: refuses calls on a revoked session, registers the rest so a revocation can cut them |
| `ha-raft/.../PeerAddressAllowlistFilter.java` | attaches a session to every admitted transport, drops it in `transportTerminated`, and sweeps the sessions at the end of `doResolve()` - the one writer of `allowedIps` - marking under the monitor and closing with it released |
| `ha-raft/.../RaftGrpcServicesCustomizer.java` | installs interceptors as well as transport filters |
| `ha-raft/.../RaftHAServer.java` | builds and publishes the interceptor next to the filter; `allowlistInterceptorForTest()` |
| `engine/.../GlobalConfiguration.java` | `arcadedb.ha.peerAllowlist.enabled` now states the accurate operator-facing claim, including what is *not* revoked |
| `ha-raft/src/test/.../Issue7250RevokesEstablishedTransportTest.java` | **new** - 12 tests, one per fixed row of the table plus the lifetime contract |

### Design notes

- **The socket is not the unit of revocation; the RPC is.** gRPC exposes no per-transport close, so the fix
  works on the only surface it does expose. `ServerImpl` runs the builder's interceptors on every call
  whatever order the services were registered in, so one interceptor covers ADMIN, CLIENT and SERVER - which
  is the same reason one transport filter already did.
- **Marking and closing are separated.** `doResolve()` holds the filter's monitor; closing a `ServerCall`
  runs gRPC code. Flipping the session flag under the monitor is what stops the *next* RPC, and it is enough
  to be correct; the in-flight ones are closed by `dispatchRevocations()` after the monitor is released, so
  the filter's lock is never ordered against gRPC internals in two directions.
- **The fail-open window is respected.** `admits()` reproduces `isAllowed`'s decision (minus its
  re-resolution) rather than testing `allowedIps` alone, so a transport admitted while the filter was below
  quorum is not cut until the grace window closes. Testing membership alone would have re-created the
  self-inflicted startup partition #4471 and #4828 exist to prevent.
- **Nothing outlives its transport.** The session set is cleared from `transportTerminated`, and a session
  holds only calls in flight - the interceptor deregisters on `onComplete` and `onCancel`. Both are asserted.
- **Cost on the hot path.** One `Attributes` lookup per RPC, and for a gated transport two small allocations
  per RPC. Raft `appendEntries` is a long-lived stream, so on a follower this is per stream, not per entry.

## Test results

```
$ mvn -o -pl ha-raft -Dtest=Issue7250RevokesEstablishedTransportTest test
Tests run: 14, Failures: 0, Errors: 0, Skipped: 0

# with the revocation sweep in doResolve() disabled, to prove the tests can fail:
Tests run: 14, Failures: 6, Errors: 0, Skipped: 0
  aPeerRemovedFromTheRaftConfigurationLosesItsEstablishedTransport
  aNewRpcOnARevokedTransportIsRefused
  aPeerWhoseAddressChangedLosesTheTransportOpenedOnTheOldAddress
  aTransportHeldOpenOnAnExpiredStickyAddressIsRevoked
  aTransportAdmittedUnderTheStartupFailOpenIsRevokedOnceTheGraceEnds
  theProductionReconciliationRevokesTheRemovedPeersEstablishedTransport

$ mvn -o -pl ha-raft -DexcludedGroups=benchmark,slow,vector test
Tests run: 1291, Failures: 0, Errors: 0, Skipped: 0

# the allowlist's own history plus the classes that flaked on a later full run, re-run together:
$ mvn -o -pl ha-raft -Dtest='PeerAddressAllowlistFilterTest,Issue7132AllowlistLearnsRuntimePeersTest,\
    Issue7225AllowlistUnlearnsRemovedPeersTest,Issue7250RevokesEstablishedTransportTest,RaftHAServerTest,\
    Issue3890RaftParametersPublicationTest,LeaveClusterTest,Issue7037SnapshotInstallSpaceCheckTest' test
Tests run: 131, Failures: 0, Errors: 0, Skipped: 0

$ mvn -o -pl engine -Dtest='*Configuration*Test' test
Tests run: 105, Failures: 0, Errors: 0, Skipped: 0
```

## Reachability

`arcadedb.ha.peerAllowlist.enabled` defaults to `true` (`GlobalConfiguration.java:2140`), so
`installPeerAllowlist` runs on every HA server that has not turned the allowlist off, and it now constructs
and publishes the interceptor on the same branch that constructs the filter. `RaftGrpcServicesCustomizer`
passes it to `NettyServerBuilder.intercept`, and Ratis applies the customizer immediately before building
the server (`GrpcServicesImpl.buildServer`: `customizer.customize(builder, types).build()`).
`theProductionReconciliationRevokesTheRemovedPeersEstablishedTransport` drives the whole production path -
`buildParameters` -> `installPeerAllowlist` -> `reconcileAllowlistMembership` -> revocation - rather than
the filter in isolation.

### A note on two flaky classes

A later full-module run showed `LeaveClusterTest` (forked VM crash) and
`Issue7037SnapshotInstallSpaceCheckTest`. Neither is this change:

```
$ lsof -nP -iTCP:2480 -sTCP:LISTEN
java  14807 frank  262u  IPv6  TCP 127.0.0.1:2480 (LISTEN)
java  93797 frank  263u  IPv6  TCP *:2480 (LISTEN)
$ lsof -nP -iTCP:2434 -sTCP:LISTEN
java  14807 frank  304u  IPv6  TCP *:2434 (LISTEN)
```

Two other JVMs held the fixed HA ports for the duration of that run. `Issue7037SnapshotInstallSpaceCheckTest`
touches nothing in this diff either - it measures page-file sizes under `./target/databases` - and both classes
pass in the 131-test run above, which was taken after the ports cleared.

## Adversarial pass

The orchestrator's independent subagent could not be spawned (the `Task` tool is disabled in this session), so
this pass was self-performed and is **not** the independent read the process asks for - recorded here rather
than presented as one. It still found five things, four of them fixed in this branch before the PR opened:

| # | Finding | Disposition |
|---|---|---|
| 1 | **A transport could be admitted with nothing able to revoke it.** `transportReady` registered the session *after* `isAllowed()` returned, and `isAllowed()` can re-resolve; a reconciliation on another thread that swept in that window never saw the session, so the connection was admitted already un-revokable. | **Fixed** - the session is registered *before* the decision and removed again on the rejection path, and `transportReady` refuses a transport whose session was revoked while it was deciding. `aRejectedTransportIsRegisteredForTheRaceButNotLeftBehind` drives register/revoke/unregister on the rejection path. The concurrent half is correct by construction rather than by test: driving it would need a synchronisation hook in production code, and this doc says so rather than implying a test covers it. |
| 2 | **A throwing handler leaked a call registration.** If `next.startCall` threw, neither `onComplete` nor `onCancel` would arrive, so the wrapper stayed in the session's live-call set - one dead entry per failed RPC on a connection that may live for days, walked by every later revocation. | **Fixed** - the interceptor deregisters and rethrows. Test: `aHandlerThatThrowsDoesNotLeaveTheCallRegistered`. |
| 3 | **A rejected connection logged a revocation.** Consequence of fix 1: a session registered for the race and then swept produced "Revoked the established Raft gRPC transport of X" for a transport that was never admitted, next to the rejection line for the same address. | **Fixed** - the session carries an `admitted` flag set only when `transportReady` publishes it, and `dispatchRevocations` closes but does not log an unadmitted one. |
| 4 | **`getSessions()` handed out the live mutable set.** A test hook that lets a caller corrupt the state it observes. | **Fixed** - returns an unmodifiable view. Test: `theSessionSetIsNotExposedForMutation`. |
| 5 | **Revocation is terminal, and a DNS flap therefore forces a reconnect.** A peer whose name resolves elsewhere for one tick has its live transport cut and must reconnect rather than resuming. | **Not a defect - documented.** The sticky retention covers resolution *failure*, which is the transient case; an answer pointing somewhere else is a different pod. The outcome is a Ratis reconnect that the allowlist admits normally once DNS is right, and the alternative - un-revoking on a later resolution - would make a revocation only as durable as the least trustworthy answer in the window. Stated in `PeerTransportSession.revoke()`'s javadoc. |

## Review cycles

### Cycle 1 - `8bd6dc5`

`claude` reviewed and found **no blocking issues**; it read the concurrency-sensitive paths (register-before-decide,
the interceptor's re-check, the lock discipline between `doResolve` and `dispatchRevocations`, and the single-shot
close arbitration) and reported no correctness bug. It also noted it could not run Maven in its sandbox, so its
read is static and CI is what confirms the suite. Three nits, all addressed:

| Note | Disposition |
|---|---|
| The `arcadedb.ha.peerAllowlist.enabled` text reads awkwardly ("what an already established connection of its could still do"). | **Applied** - rewritten to "loses the reach an already-established connection still gave it". |
| `getSessions()` returns an unmodifiable *view*, not a snapshot, so it still reflects concurrent change. | **Applied** - returns `Set.copyOf(sessions)`. The existing `theSessionSetIsNotExposedForMutation` still holds, since a copy is immutable too. |
| The per-RPC wrapper and set add/remove apply to all inter-node Raft traffic; worth checking it does not add up under connection churn. | **Argued, with one change.** The interceptor adds three short-lived allocations per RPC on a gated transport: the `RevocableServerCall`, the forwarding listener, and the set node. gRPC allocates strictly more than that per RPC on its own - a `ServerCallImpl`, the `Metadata`, the stream and the listener chain - so the marginal cost is a fraction of a baseline the RPC already pays, and it is proportional to RPCs rather than to bytes or log entries, so a busy follower does not pay more per entry. That is a reasoning argument, not a measurement: no benchmark was run, and the doc says so rather than claiming one. The one thing worth changing was unrelated to the wrapper - the per-session live-call set defaulted to 16 slots for a connection that has a handful of RPCs in flight, and is now sized 4. |

### Cycle 2 - `d728df1`

`claude` reviewed again and found **no blocking issues** and no coverage gap; it traced the same concurrency paths
and confirmed the cycle-1 changes. Static read again - it could not run Maven either. Two notes, both about
comments rather than behaviour, and both applied:

| Note | Disposition |
|---|---|
| `resolveIfStale`'s early return skips `dispatchRevocations()` as well as the resolution. Correct once traced - the thread that short-circuits enqueued nothing, and the one that ran `doResolve()` dispatches - but it is a genuine asymmetry with the other three dispatch sites and a later edit could break it silently. | **Applied** - the method now carries a javadoc paragraph stating why the early return is not a dropped revocation, and naming the two edits that would make it one. |
| `PeerTransportSession.revoke()` is `synchronized` although its only caller already holds the filter's monitor, so the keyword is currently redundant and could suggest that is where the sweep's safety lives. | **Applied as a comment, not a removal.** Verified the claim - `grep -rn "\.revoke()" ha-raft/src` finds exactly one caller, `doResolve()`. Kept, because the return value is a promise this method makes and it should be this method that keeps it; the javadoc now says the keyword buys nothing today and why it is there anyway. |
| Per-RPC cost is reasoned rather than measured; the concurrent register/revoke race is correct-by-construction rather than tested. | **No change** - both were already stated as such here rather than implied, which is what the review was acknowledging. |
