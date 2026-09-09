# #7316 - a revoked Raft gRPC peer keeps its TCP/HTTP-2 connection open

Follow-up to #7250 (chain: #7132 -> #7225 -> #7250 -> #7316).

## The gap

#7250 made a peer that stops being admitted by `PeerAddressAllowlistFilter` lose its *reach* on a connection it
had already established: the in-flight Raft RPCs are closed with `PERMISSION_DENIED` and every later RPC on that
transport is refused by `PeerAllowlistCallInterceptor`. It does not close the connection - gRPC's public API hands
out no handle on one established server transport - so the socket and its HTTP/2 framing state survive until the
peer, the kernel, or a network event drops it.

## Root cause

`ServerTransportFilter` has two methods and neither carries a transport handle:

```
$ grep -n "public" org/apache/ratis/thirdparty/io/grpc/ServerTransportFilter.java
public abstract class ServerTransportFilter {
  public Attributes transportReady(Attributes transportAttrs)
  public void transportTerminated(Attributes transportAttrs)
}
```

Every connection-lifetime knob is on the builder instead, and Ratis sets none of them
(`GrpcServicesImpl.newNettyServerBuilder` sets only `SO_REUSEADDR`, `maxInboundMessageSize`, `flowControlWindow`,
the channel type / event loop groups and the SSL context):

```
$ grep -n "maxConnectionIdle\|maxConnectionAge\|keepAlive\|permitKeepAlive" \
    org/apache/ratis/grpc/server/GrpcServicesImpl.java
(no output)
```

so a Raft gRPC connection on ArcadeDB has, before this change, no server-side lifetime bound at all.

## The invariant

> An inbound Raft gRPC connection that carries no active HTTP/2 stream for the configured window is closed by the
> server, instead of living until the peer drops it - which is what a revoked transport becomes, because
> `PeerAllowlistCallInterceptor` refuses every RPC on it.

## Completeness

### Writers of the customizer

```
$ grep -rn "setServicesCustomizer" .
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java:4241
```

One writer. It lives in `installPeerAllowlist`, which returns early when
`arcadedb.ha.peerAllowlist.enabled` is false - so before this change a customizer existed only when the allowlist
did. That is why the idle window is installed from a method that no longer returns early on the allowlist flag:
otherwise `arcadedb.ha.grpcMaxConnectionIdleMs` would be a setting that silently does nothing on exactly the
clusters that set `peerAllowlist.enabled=false`.

### Readers / callers

```
$ grep -rn "buildParameters(" .
ha-raft/.../RaftHAServer.java:998            # startRatis
ha-raft/.../RaftHAServer.java:1531           # restartRatis (recovery)
ha-raft/.../RaftHAServer.java:4175           # the method
+ 11 call sites in ha-raft tests
```

Both production callers run the whole method, so both get the customizer.

### Stale claims that the fix invalidates

Every place that documented "the connection stays open" had to move with the code, or it becomes the next bug
report:

```
$ grep -rn "stays open\|stays connected\|no way to close one established" ha-raft/src \
    engine/src/main/java/com/arcadedb/GlobalConfiguration.java
PeerAddressAllowlistFilter.java:108     # class javadoc
PeerAddressAllowlistFilter.java:607     # the revocation log message an operator reads
PeerTransportSession.java:41,43         # class javadoc
GlobalConfiguration.java:2140           # HA_PEER_ALLOWLIST_ENABLED description
Issue7250RevokesEstablishedTransportTest.java:59   # test class javadoc
```

All five are updated by this change.

### Same-shape siblings

```
$ grep -rln "NettyServerBuilder" --exclude-dir=target .
ha-raft/.../PeerTransportSession.java, ha-raft/.../RaftGrpcServicesCustomizer.java   # this fix
grpcw/src/main/java/com/arcadedb/server/grpc/GrpcServerPlugin.java
grpcw/src/test/java/com/arcadedb/server/grpc/Issue5050GrpcPluginLifecycleTest.java
```

`GrpcServerPlugin` is the only other gRPC listener, and it is not the same shape: it has no allowlist and no
revocation, so it never reaches the "refused every RPC but still connected" state this issue is about. Argued, not
fixed here.

It is worth being precise about what it does have, because the first draft of this paragraph overstated it: the
`keepAliveTime`/`keepAliveTimeout`/`permitKeepAlive*` it sets at lines 181-184 make it detect a peer that has
stopped answering pings. That is not the same bound as `maxConnectionIdle`, which closes a connection that is alive
and simply carrying nothing. So a connection there that is idle but healthy is still unbounded; if that listener
ever wants the same treatment it needs an idle window of its own rather than its existing keepalive settings.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `buildParameters` -> allowlist ON + idle window -> customizer sets `maxConnectionIdle` | yes | yes - `anIdleConnectionIsReapedWhenTheAllowlistIsOn` |
| `buildParameters` -> allowlist OFF + idle window -> customizer still installed | yes | yes - `anIdleConnectionIsReapedWithTheAllowlistDisabled` |
| `buildParameters` -> idle window 0 -> no lifetime bound, connection lingers (documented) | n/a - deliberate | yes - `aZeroIdleWindowLeavesTheConnectionOpen` |
| `buildParameters` -> allowlist OFF + idle window 0 -> no customizer at all | n/a - deliberate | yes - `noCustomizerIsInstalledWhenNothingNeedsOne` |
| `RaftGrpcServicesCustomizer.customize` applies filters + interceptors as before | yes - unchanged | yes - `theAllowlistFilterAndInterceptorAreStillInstalled` |
| A revoked peer that keeps starting RPCs never lets the connection go idle | **no** | no - filed as a follow-up |

## Residual risk

`maxConnectionIdle` measures idleness from the most recent moment the transport's active-stream count reached
zero, not from the connection's age:

```
$ sed -n '/public void onTransportIdle/,/^  }/p' \
    org/apache/ratis/thirdparty/io/grpc/internal/MaxConnectionIdleManager.java
  public void onTransportIdle() {
    isActive = false;
    ...
      nextIdleMonitorTime = ticker.nanoTime() + maxConnectionIdleInNanos;
```

So a revoked peer that keeps *starting* RPCs - each of which the interceptor refuses immediately - pushes the
deadline forward every time and its connection is never reaped. That covers a peer whose Ratis division has not
learned it was removed and is still campaigning, and it covers a deliberate squatter. `maxConnectionAge`, which is
unconditional, is the knob that would close it; it also recycles every healthy Raft connection on the same period,
which is why it is not being turned on in the same change as the fix. Filed as a follow-up (see below).

What the fix does cover: a revoked transport that has gone quiet - the normal removal case, where the removed peer
stops driving Raft RPCs at us - and, more generally, any abandoned or half-dead inbound Raft connection, none of
which had a server-side lifetime bound before.

The replication path is not affected by the default window. `appendEntries` is a bidirectional stream, and Ratis's
log appender opens one per follower and keeps it:

```
$ grep -n "appendEntries" org/apache/ratis/grpc/server/GrpcServerProtocolService.java
242:  public StreamObserver<AppendEntriesRequestProto> appendEntries(

$ grep -n "appendLogRequestObserver" org/apache/ratis/grpc/server/GrpcLogAppender.java
162:  private volatile StreamObservers appendLogRequestObserver;
405:      if (appendLogRequestObserver == null) {
406:        appendLogRequestObserver = new StreamObservers(     # created once, reset only on ERROR/COMPLETE
```

so the follower's inbound connection carries an active stream continuously and never reaches an active-stream count
of zero. What the window can reach are the quiet direction-of-travel connections (a follower's channel to a peer it
only dials to campaign, an idle Ratis admin/client channel); those are closed with a graceful GOAWAY, which a gRPC
`ManagedChannel` answers by reconnecting on the next RPC.

## Test results

Red, before the one-line application of the window in `RaftGrpcServicesCustomizer.customize`, with everything else
already in place - so the two failures are the fix, not the plumbing:

```
[ERROR] Tests run: 5, Failures: 2, Errors: 0, Skipped: 0, Time elapsed: 124.1 s
[ERROR] ...Issue7316ReapsIdleRaftConnectionTest.anIdleConnectionIsReapedWhenTheAllowlistIsOn -- 61.06 s <<< FAILURE!
[ERROR] ...Issue7316ReapsIdleRaftConnectionTest.anIdleConnectionIsReapedWithTheAllowlistDisabled -- 60.01 s <<< FAILURE!
```

Green, after:

```
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 5.838 s
```

The 124 s -> 5.8 s drop is the same evidence read a second way: before the change both connections sat there until
the test's own read timeout expired.

Regression set:

| Run | Result |
|---|---|
| `-pl engine -Dtest='Issue7124BooleanSettingStrictCoercionTest,Issue7163DatabaseScopeCallbackTest'` (the two that enumerate every setting) | `Tests run: 19, Failures: 0, Errors: 0` |
| `-pl ha-raft -Dtest='Issue7250*,Issue7225*,Issue7132*,Issue3890RaftParametersPublicationTest,PeerAddressAllowlistFilterTest,HAConfigDefaultsTest,Issue7316*'` | `Tests run: 90, Failures: 0, Errors: 0` |
| `-pl ha-raft -DexcludedGroups=benchmark,slow,vector` (the module's whole unit lane) | `Tests run: 1045, Failures: 0, Errors: 0` |

The full-lane run still exited 1: surefire could not *start* a fork for `WaitForApplyTest` ("The forked VM
terminated without properly saying goodbye... Error occurred in starting fork"), on a machine that had a dozen
other surefire forks live at the time. Re-running that class on its own is green - `Tests run: 4, Failures: 0,
Errors: 0` in 62 s - so the exit code is the machine, not the change.

## Summary of changes

| File | Change |
|---|---|
| `engine/.../GlobalConfiguration.java` | new `HA_GRPC_MAX_CONNECTION_IDLE_MS` (`arcadedb.ha.grpcMaxConnectionIdleMs`, `Long`, `SCOPE.SERVER`, default `300_000`); `HA_PEER_ALLOWLIST_ENABLED`'s description no longer claims the connection stays open until the peer drops it |
| `ha-raft/.../RaftGrpcServicesCustomizer.java` | takes the window and applies `NettyServerBuilder.maxConnectionIdle` when it is positive |
| `ha-raft/.../RaftHAServer.java` | `installPeerAllowlist` split into `buildPeerAllowlistFilter` (returns the filter or null) and `installGrpcServerCustomizations`, which installs the customizer when *either* half is configured |
| `ha-raft/.../PeerAddressAllowlistFilter.java` | class javadoc and the revocation log message now say what actually happens to the socket |
| `ha-raft/.../PeerTransportSession.java` | same, for the class that documents what a revocation revokes |
| `ha-raft/.../Issue7250RevokesEstablishedTransportTest.java` | class javadoc only - it asserted, in prose, that the socket is never closed. No test code touched |
| `ha-raft/.../Issue7316ReapsIdleRaftConnectionTest.java` | new: five tests, one per row of the coverage table |

## Finding ledger

- [x] 1. A revoked Raft gRPC peer keeps its TCP/HTTP-2 connection open - fixed for a connection that goes quiet;
      the still-retrying case is filed as #7339

## Impact

Default-on, and the default is the behaviour change: a cluster that upgrades gets a five-minute idle bound on its
Raft listener where it had none. The replication path cannot reach that bound (see the `appendEntries` evidence
above); the connections that can are closed with a graceful GOAWAY, and `maxConnectionIdle` only ever fires when
the transport's active-stream count is zero, so no in-flight RPC can be interrupted by it. Set
`arcadedb.ha.grpcMaxConnectionIdleMs=0` to restore the previous unbounded behaviour.

## Adversarial pass

The orchestrator's Phase 1.5 asks for a subagent that has not been persuaded by the author's reasoning. **No
`Task` tool is available in this environment**, so the pass was run inline against the diff rather than by a fresh
agent - which is the weaker form of the check, and is recorded here as such. What it turned up:

| Finding | Disposition |
|---|---|
| "The fix does not close the connection in the case the title describes, if the peer keeps retrying" | Real, out of scope - filed as **#7339**, named in the PR body and in the coverage table |
| "`Issue7250RevokesEstablishedTransportTest`'s class javadoc still states the socket is never closed" | Real, in scope - javadoc corrected in this branch; no test code changed |
| "A negative or absurdly large window could throw or overflow" | Not real. Values `<= 0` never reach gRPC (the `> 0` guard), and `TimeUnit.MILLISECONDS.toNanos` saturates rather than wrapping, so a huge value lands above gRPC's `AS_LARGE_AS_INFINITE` and is read as "disabled" - the same as 0, by a different road |
| "The idle window could cut a long-running Raft RPC" | Not real. `NettyServerHandler` drives the idle manager from `connection.numActiveStreams()` reaching 0, so the timer is only ever running while nothing is in flight |
| "`GrpcServerPlugin` has the same defect" | Not real. It is a different listener with no allowlist and no revocation, and it already sets `keepAliveTime`/`keepAliveTimeout` (lines 181-184) |

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7349

## Review cycles

### Cycle 1 - `8717356`

`claude` reviewed on the PR as an issue comment: no blocking findings, two doc nits, both real and both fixed in
cycle 2:

1. `RaftGrpcServicesCustomizer`'s pre-existing class javadoc said "Ratis 3.2.2" while `ha-raft/pom.xml` pins
   `3.3.0`. Rewritten, and made accurate about *why* one customizer covers every inbound RPC: it is not that all
   three service types share a listener (they do not when `raft.grpc.admin.port`/`raft.grpc.client.port` name their
   own), it is that `GrpcServicesImpl.buildServer` runs the customizer on each builder it constructs -
   `GrpcServicesImpl.java:250, 335, 345`.
2. This doc claimed `GrpcServerPlugin`'s connections are "not unbounded either" because it sets keepalive. That is
   overstated: keepalive detects a peer that stopped answering pings, it does not close an idle-but-healthy
   connection. Paragraph corrected, and the correction left visible rather than quietly reworded.

Nothing was deferred and nothing was skipped.
