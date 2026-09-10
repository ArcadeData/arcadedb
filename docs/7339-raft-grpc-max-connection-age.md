# #7339 - a revoked Raft gRPC peer that keeps retrying never lets its connection go idle

Follow-up to #7316 (chain: #7132 -> #7225 -> #7250 -> #7316).

## The defect

#7316 gave the Raft gRPC listener a server-side connection-lifetime bound,
`arcadedb.ha.grpcMaxConnectionIdleMs` -> `NettyServerBuilder.maxConnectionIdle`. That bound is
conditional on the peer going quiet. gRPC measures idleness from the most recent moment the
transport's active-stream count reached zero, and `NettyServerHandler` drives that from the HTTP/2
stream count, so every RPC - including one the `PeerAllowlistCallInterceptor` answers with
`PERMISSION_DENIED` - opens and closes a stream and pushes the deadline forward by the whole window.

A revoked peer that keeps *starting* RPCs therefore keeps its connection forever. Two ways to reach
it, both from the issue: a removed peer whose Ratis division has not learned it was removed keeps
campaigning and sends a `requestVote` at every election timeout (hundreds of ms), which is shorter
than any idle window that is safe for healthy connections; and a deliberate squatter can simply keep
the connection busy on purpose.

Blast radius is the same as #7316's and no larger: the interceptor refuses every RPC on a revoked
transport, so what survives is a socket plus its HTTP/2 framing state - a resource cost, not reach.

## The invariant

> When `arcadedb.ha.grpcMaxConnectionAgeMs` is greater than zero, every inbound Raft gRPC connection
> is closed once it reaches that age, whatever RPC traffic the peer keeps starting on it.

`maxConnectionAge` is the only knob gRPC exposes that holds it. The timer is scheduled once, in
`NettyServerHandler.handlerAdded`, and fires regardless of stream activity:

```
$ sed -n '/public void handlerAdded/,/maxConnectionIdleManager != null/p' \
    org/apache/ratis/thirdparty/io/grpc/netty/NettyServerHandler.java
  public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
    serverWriteQueue = new WriteQueue(ctx.channel());

    // init max connection age monitor
    if (maxConnectionAgeInNanos != MAX_CONNECTION_AGE_NANOS_DISABLED) {
      maxConnectionAgeMonitor = ctx.executor().schedule(
          new LogExceptionRunnable(new Runnable() {
            @Override
            public void run() {
              if (gracefulShutdown == null) {
                gracefulShutdown = new GracefulShutdown("max_age", maxConnectionAgeGraceInNanos);
```

Two properties of the shaded gRPC (ratis-thirdparty-misc 1.1.0, which ratis 3.3.0 pins) that the
implementation and the tests both depend on, each read out of the source rather than recalled:

- the GOAWAY debug string names the timer that fired - `"max_age"` here, `"max_idle"` in the
  `maxConnectionIdleManager` branch immediately below. That is what lets a test say *which* bound
  closed a connection instead of inferring it from a stopwatch;
- `NettyServer.initChannel` applies a per-connection jitter of +/-10% to the age
  (`(long) ((.9D + Math.random() * .2D) * maxConnectionAgeInNanos)`), so a configured age is a
  centre, not a deadline. No jitter is applied to the idle window.

`NettyServerBuilder.maxConnectionAge` clamps anything under a second up to a second
(`MIN_MAX_CONNECTION_AGE_NANO`) and treats anything from 1000 days up as disabled
(`AS_LARGE_AS_INFINITE`), so the only value this code has to handle itself is the one that means off.

### Why the grace window is always passed

`maxConnectionAgeGrace` defaults to `MAX_CONNECTION_AGE_GRACE_NANOS_INFINITE` in gRPC, and
`GracefulShutdown.graceTimeOverrideMillis` turns that into netty's `-1` - "no timeout". Under an
infinite grace the age bound stops being a bound on exactly the connection that matters most here: a
leader's `AppendEntries` to a follower is one long-lived stream, so after the GOAWAY the connection
would wait for a stream that does not end. `arcadedb.ha.grpcMaxConnectionAgeGraceMs` is therefore
passed on every call, with a finite default of 5 s.

Stated precisely, because the first draft of this said something stronger than the code holds: what
the code rules out is *arriving* at an infinite grace by not setting one. An operator who configures
1000 days or more still gets gRPC's infinite, because `AS_LARGE_AS_INFINITE` is applied inside
`NettyServerBuilder`. That is documented in the setting's own description rather than clamped, since
the same threshold disables the age window itself and clamping one but not the other would be the
more surprising behaviour.

## Completeness

### Commands run

```
$ grep -rn "setServicesCustomizer\|servicesCustomizer" --include='*.java' .
./ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue3890RaftParametersPublicationTest.java:79
./ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue3890RaftParametersPublicationTest.java:116
./ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue7316ReapsIdleRaftConnectionTest.java:106,145,164,196
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java:4335

$ grep -rn "new RaftGrpcServicesCustomizer" --include='*.java' .
./ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java:4335
```

One install site: `RaftHAServer.installGrpcServerCustomizations`, reached only from
`buildParameters`. Nothing else configures the Raft listener's `NettyServerBuilder`.

```
$ grep -rn "NettyServerBuilder" --include='*.java' . | grep -v /test/
ha-raft/.../PeerTransportSession.java:42,46          (comments)
ha-raft/.../RaftGrpcServicesCustomizer.java:24,46,70,71
grpcw/.../GrpcServerPlugin.java:39,169,175,338,342

$ grep -n "maxConnectionIdle\|maxConnectionAge\|keepAlive\|permitKeepAlive" \
    grpcw/src/main/java/com/arcadedb/server/grpc/GrpcServerPlugin.java
181:        .permitKeepAliveTime(10, TimeUnit.SECONDS)
182:        .permitKeepAliveWithoutCalls(true)
183:        .keepAliveTime(30, TimeUnit.SECONDS)
184:        .keepAliveTimeout(10, TimeUnit.SECONDS)
```

`grpcw` is the one other gRPC listener in the tree, and it sets no lifetime bound either - the same
*shape*. It is argued rather than fixed below.

### Entry-point coverage

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `buildParameters` -> `installGrpcServerCustomizations` -> `RaftGrpcServicesCustomizer.customize` -> `maxConnectionAge`, allowlist ON | yes | yes - `theAgeBoundClosesAConnectionTheIdleWindowCannotReach` |
| same, allowlist OFF (the age is a connection-lifetime setting, not an allowlist setting - the trap #7316 had to be split apart to avoid) | yes | yes - `theAgeBoundIsInstalledWithTheAllowlistDisabled` |
| the grace window reaches the builder and is finite | yes | yes - `theGraceWindowIsHonouredAndNeverInfinite` |
| age = 0 leaves a busy connection alone (pre-26.10.1 behaviour, and the default) | yes | yes - `aBusyConnectionSurvivesTheIdleWindowWhenTheAgeIsDisabled` (this is the #7339 repro) |
| age = 0, idle = 0, allowlist off -> no customizer at all | yes | yes - `noCustomizerIsInstalledWhenNothingNeedsOne` (#7316's row 4, re-asserted with the age at 0) |
| a negative grace from a config typo must not throw out of `NettyServerBuilder.maxConnectionAgeGrace`'s `checkArgument` at startup | yes | yes - `aNegativeGraceIsClampedRatherThanCrashingStartup` |
| a live multi-node cluster with the age enabled keeps replicating across the recycle | yes | yes - `Issue7339RaftConnectionAgeRecyclingIT` |
| `grpcw`'s client-facing listener has no lifetime bound either | **argued** | n/a |
| a non-zero DEFAULT for the age window | **argued** (measurement recorded below; default stays 0) | n/a |

### Argued rows

**`grpcw`.** The defect this issue chain is about is a connection that outlives the *revocation of
its peer's reach*: #7250 gave the Raft listener an address allowlist whose decision can flip while a
connection is established, and #7316/#7339 are about closing the socket that decision stranded.
`GrpcServerPlugin` has no such mechanism - it authenticates every call against the server's own user
registry, there is no address-derived per-transport verdict that can change underneath an open
connection, and therefore no "revoked but still connected" state for an age bound to remedy. Its
lack of a lifetime bound is an ordinary resource-management question about a client-facing listener,
with a different threat model and a different blast radius, and it is not made better or worse by
this change. Not filed: it is a design question for the grpcw listener, not a gap in this fix.

**The default.** The issue asks for the setting to default to 0 and for a measurement before any
non-zero default. Both are honoured: `HA_GRPC_MAX_CONNECTION_AGE_MS` defaults to `0L`, so nothing
changes for an existing cluster that does not opt in, and `Issue7339RaftConnectionAgeRecyclingIT` is
the measurement - a real 3-node Raft cluster with the age set to 3 s and the grace to 1 s, i.e. a
recycle roughly every 3 s per connection against Ratis's default election timers, writing
continuously across at least four recycle periods. What it can prove is that the recycle does not
cost a leadership change or a lost write at that period; what it cannot prove is the right value for
a production cluster on a real network, which is why the default stays off and the decision stays
with the maintainer.

## Adversarial pass

Run by the author rather than by an independent subagent: this environment exposes no `Task` tool, so
the orchestrator's "one subagent that has not been convinced" could not be spawned. Recorded here
with that caveat, since it is a weaker check than the one the process asks for.

| Finding | Verdict | Disposition |
|---|---|---|
| The comment claimed "this code never passes gRPC's infinite" for the grace. gRPC applies `AS_LARGE_AS_INFINITE` inside `NettyServerBuilder`, so a configured 1000 days or more still lands on infinite and the age bound stops bounding. | real, in scope | fixed here: the claim is narrowed to what the code holds, and the setting's description states the threshold |
| The first version of the IT passed while observing *zero* connections. It counted distinct `PeerTransportSession` instances, and `PeerAddressAllowlistFilter.transportReady` returns early for `isLoopbackAddress()` without registering one - so on an in-process cluster there was nothing to count and the assertion measured nothing. | real, in scope | fixed here: the IT now opens an `Http2ConnectionProbe` against the live leader's Raft port and asserts the GOAWAY names `max_age`. Verified falsifiable by the sabotage below |
| `@Tag("slow")` on the unit-test class would have hidden it from CI entirely: the `unit-tests` lane passes `-DexcludedGroups=slow,...` across every module, and the `slow-unit-tests` lane runs `-pl engine` only, so an `ha-raft` class tagged `slow` runs in no lane at all. | real, in scope | the unit test is deliberately untagged. The IT keeps `@Tag("slow")` because the `ha-integration-tests` lane passes no group filter and `failsafe.excludedGroups` defaults to empty, so a tag there is inert - which matches the module's existing ITs |
| Does the age window tear down a snapshot transfer? A snapshot install is the one long operation on a Raft cluster that a period of seconds could plausibly starve. | not real | the bulk payload does not travel on the Raft gRPC listener: `SnapshotHttpHandler` serves it over HTTP at `GET /api/v1/ha/snapshot/{database}`, and a follower behind the compacted log downloads it from there |
| The customizer is applied to Ratis's ADMIN and CLIENT listeners as well as SERVER, so the age recycles Ratis client channels too. | real, out of scope to change | intended, and identical to what #7316's idle window already does: `GrpcServicesImpl.buildServer` runs the customizer on each builder it constructs. Noted under residual risk rather than filed - it is the documented behaviour of the setting, not a gap |
| The window is bound once, when `buildParameters` runs at plugin startup, so changing the setting on a running node does nothing until it restarts. | real, out of scope | true of every `arcadedb.ha.grpc*` setting including #7316's, and changing it would mean rebuilding the Ratis server. Noted under residual risk |

## Verification

```
$ mvn -o -pl ha-raft test -Dtest=Issue7339BoundsBusyRaftConnectionTest
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0     (10.7 s)

$ mvn -o -pl ha-raft verify -Dit.test=Issue7339RaftConnectionAgeRecyclingIT -DskipTests=true -DskipITs=false
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0     (43.0 s)

$ mvn -o -pl ha-raft test -DexcludedGroups=benchmark,vector
Tests run: 415, Failures: 0, Errors: 0, Skipped: 0
```

The 415-test run ends in a `BUILD FAILURE` that is not a regression: `LeaveClusterTest`'s fork could
not start because another agent's build on this machine held port 2434
(`java.net.BindException: Address already in use`, the fixed-port hazard `CLAUDE.md` documents). No
test failed.

### Proving the tests can fail

Sabotaging the fix - `if (false && maxConnectionAgeMs > 0)` in `RaftGrpcServicesCustomizer` - turns
exactly the three age assertions red and leaves the defect-reproducing one green:

```
Tests run: 7, Failures: 3
  theAgeBoundClosesAConnectionTheIdleWindowCannotReach     expected "max_age" but was null
  theAgeBoundIsInstalledWithTheAllowlistDisabled           expected "max_age" but was null
  aZeroGraceStillClosesTheConnection                       expected "max_age" but was null
  aBusyConnectionSurvivesTheIdleWindowWhenTheAgeIsDisabled PASSES - it is the pre-fix behaviour
```

The failure lines carry the number that matters: `Probe: 580 RPCs, widest gap between two of them
107 ms, JVM stalled 0 ms`. 580 refused RPCs over sixty seconds against a one-second idle window, with
the connection never closed, is the defect stated as a measurement.

Sabotaging the *harness* instead - raising the drumbeat from 100 ms to 2 000 ms, above the idle
window - closes the connection before the first beat, which is what proves the idle window is armed
and that the drumbeat is genuinely what defeats it rather than the window being absent.

The same sabotage was **not** run against `Issue7339RaftConnectionAgeRecyclingIT`: every attempt died
before the fork started, on the port 2434 collision above, and waiting out another agent's build is
not bounded. What stands instead is that the IT's probe assertion is the same assertion as
`theAgeBoundIsInstalledWithTheAllowlistDisabled`, against the same helper, and that one is shown red
under sabotage. The IT's own vacuity was caught and fixed by the adversarial pass, which is the
failure mode a sabotage run would have been looking for.

## Review cycle 1 - PR #7419

Two points from the `claude` review on `14f2b71b`, both checked rather than accepted.

**"the new `arcadedb-engine` test-jar dependency will not resolve under `-am test`."** The reasoning
was the gremlin/graphql trap `CLAUDE.md` documents: `maven-jar-plugin`'s `test-jar` goal binds to
`package`, which a `test`-phase reactor run never reaches. Measured instead of argued, with the
engine test-jar deleted from the local repository AND `engine/target` removed entirely:

```
$ ls .m2repo/com/arcadedb/arcadedb-engine/26.10.1-SNAPSHOT/
_remote.repositories  arcadedb-engine-26.10.1-SNAPSHOT-sources.jar
arcadedb-engine-26.10.1-SNAPSHOT.jar  arcadedb-engine-26.10.1-SNAPSHOT.pom
maven-metadata-local.xml                       <- no -tests.jar

$ mvn -o -pl engine clean
[INFO] Deleting .../engine/target

$ mvn -o -pl ha-raft -am test -Dtest=Issue7339BoundsBusyRaftConnectionTest \
      -Dsurefire.failIfNoSpecifiedTests=false
[INFO] Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS

$ ls engine/target/*.jar
no matches                                     <- nothing was packaged, and nothing needed to be
```

Maven substitutes the reactor module's `target/test-classes` for the `tests`-classified dependency,
which the gremlin case cannot do because its dependency is on the `shaded` uber-jar - an artifact
with no directory equivalent. So the trap does not reach this module pair.

The review did land on a real defect in that command, just not the predicted one: with `-am`,
`-Dtest=` is applied to every module in the reactor, so surefire fails the *upstream* module with
`No tests matching pattern "Issue7339BoundsBusyRaftConnectionTest" were executed!` before ha-raft
runs at all. `-Dsurefire.failIfNoSpecifiedTests=false` is what makes it work, and the PR's test plan
now says so.

**"`Http2ConnectionProbe` replenishes only the connection-level flow-control window."** Correct, and
it is now stated in the class javadoc together with why per-stream replenishment is deliberately
absent rather than merely missing: RFC 9113 lets a peer treat a WINDOW_UPDATE on a closed stream as a
connection error, and every stream this probe opens is closed by the server almost at once. The
behaviour is unchanged; a future reuser now sees the constraint before hitting it.

## Residual risk

- The age bound is per connection and unconditional, so it recycles healthy Raft connections too.
  The IT shows a 3 s period surviving on a loopback 3-node cluster; it says nothing about a WAN
  cluster, a cluster with tuned election timers, or one with hundreds of databases. Anyone turning
  this on should start well above their election timeout.
- With the age disabled - the default - #7339 is unchanged: a peer that keeps starting RPCs keeps
  its connection. The fix is an available remedy, not an automatic one.
- gRPC's +/-10% jitter means the age is a centre, not a deadline: an individual connection can live
  up to 10% longer than the configured value.
- The window is applied to Ratis's ADMIN and CLIENT listeners as well as the SERVER one, so it
  recycles Ratis client channels on the same period. That is what #7316's idle window already does.
- Both windows are bound once, when `buildParameters` runs at plugin startup. Changing either on a
  running node has no effect until it restarts.
