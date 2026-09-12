# #7507 - the follower-to-leader HTTP forward has no request timeout

Issue: https://github.com/ArcadeData/arcadedb/issues/7507
Type: bug (labels: `bug`, `ha`, `server`, `concurrency`, milestone 26.10.1)

## Symptom

`LeaderCommandForwarder` dials the leader with a bare `HttpClient.newHttpClient()` and builds the
request without `.timeout(...)`. There is neither a connect timeout nor a response deadline. The
method runs on an Undertow **worker** thread - every calling handler returns `true` from
`mustExecuteOnWorkerThread()` - so a leader that accepts the connection and then never answers holds
that worker until the OS tears the socket down. Enough concurrent admin requests and the follower
stops serving anything.

## Root cause

```java
// server/.../handler/LeaderCommandForwarder.java (before)
private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();   // no connectTimeout
...
final HttpRequest.Builder builder = HttpRequest.newBuilder().uri(leaderUri); // no .timeout(...)
```

`HttpClient.newHttpClient()` has no connect timeout, and a request built without `.timeout(...)`
has no response deadline, so `HttpClient.send` blocks for as long as the peer keeps the socket open.

## Why the obvious fix is wrong

`forwardToLeaderIfReplica` carries seven commands, three of which legitimately run for minutes:

```
$ sed -n '100,106p' server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java
    if (command_lc.startsWith(CREATE_DATABASE) || command_lc.startsWith(DROP_DATABASE) ||
        command_lc.startsWith(CREATE_USER) || command_lc.startsWith(DROP_USER) ||
        command_lc.startsWith(RESTORE_BACKUP) || command_lc.startsWith(RESTORE_DATABASE) ||
        command_lc.startsWith(IMPORT_DATABASE)) {
```

A single `HA_PROXY_READ_TIMEOUT` (30 s) applied to every forward would start aborting `restore
backup`, `restore database` and `import database` - exactly the operations that most need to reach
the leader. The connect timeout is unconditionally safe; the response deadline has to distinguish
the long-running commands from the rest.

## Invariant

**Every follower-to-leader HTTP forward issued by `LeaderCommandForwarder` completes, or fails with
a typed HTTP 504, within a configured finite deadline - it can never park an Undertow worker thread
indefinitely.**

## Completeness

### Enumerate every way to violate it

Callers of the forwarder (every entry point that can park a worker on it):

```
$ grep -rn 'forwardIfReplica' --include='*.java' */src/main/java
server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java:643
server/src/main/java/com/arcadedb/server/http/handler/PutUserHandler.java:54
server/src/main/java/com/arcadedb/server/http/handler/DeleteUserHandler.java:50
server/src/main/java/com/arcadedb/server/http/handler/PostUserHandler.java:53
server/src/main/java/com/arcadedb/server/http/handler/LeaderCommandForwarder.java:98   (the declaration)
```

Same-shape siblings - a `java.net.http.HttpClient` built with no connect timeout, or a request built
with no `.timeout(...)`, sent from a server thread:

```
$ grep -rn 'HttpClient.newHttpClient()' --include='*.java' */src/main/java
ha-raft/.../RaftReplicatedDatabase.java:309
server/.../handler/PostBatchHandler.java:237
server/.../handler/LeaderCommandForwarder.java:61

$ grep -n '\.timeout(' server/src/main/java/com/arcadedb/server/http/handler/PostBatchHandler.java
(no output)
$ grep -n '\.timeout(' ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java
(no output)
```

Both of those are unbounded the same way, on different call paths (see the table).

The remaining `HttpClient` users in `src/main/java` are bounded already:

```
$ grep -n '.timeout(\|mustExecuteOnWorkerThread' server/src/main/java/com/arcadedb/server/ai/AiChatHandler.java
95:  protected boolean mustExecuteOnWorkerThread() {
243:        .timeout(Duration.ofMinutes(5))
411:        .timeout(Duration.ofSeconds(15))
468:        .timeout(Duration.ofSeconds(120))
```

(`AiActivateHandler` 15 s, `AiAnalyzeProfilerHandler` 120 s, all three with
`.connectTimeout(Duration.ofSeconds(10))` on the client; `LeaderProxy` sets both.)

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `POST /api/v1/server` `create database` / `drop database` / `create user` / `drop user` -> `LeaderCommandForwarder.forwardIfReplica` (default deadline) | yes | yes |
| `POST /api/v1/server` `restore backup` / `restore database` / `import database` -> same, long-running deadline | yes | yes |
| `POST /api/v1/server/users` -> `PostUserHandler` -> `forwardIfReplica` (default deadline) | yes | yes |
| `PUT /api/v1/server/users` -> `PutUserHandler` -> `forwardIfReplica` (default deadline) | yes | yes |
| `DELETE /api/v1/server/users` -> `DeleteUserHandler` -> `forwardIfReplica` (default deadline) | yes | yes |
| leader black-holes the TCP connect (no accept) -> client connect timeout | yes | yes |
| leader accepts and never answers -> request deadline -> HTTP 504 | yes | yes |
| `POST /api/v1/batch/{db}` -> `PostBatchHandler.forwardBatchToLeader` (bare `HttpClient`, no request deadline) | no - filed as #7542 | no |
| `RaftReplicatedDatabase.forwardCommandToLeader` (bare `HttpClient`, no request deadline) | no - filed as #7543 | no |
| `LeaderProxy.tryProxy` | argued: already bounded (`connectTimeout` + `builder.timeout(readTimeoutMs)`), and dead code - `grep -rn 'new LeaderProxy' --include='*.java' .` returns nothing |
| `AiChatHandler`, `AiActivateHandler`, `AiAnalyzeProfilerHandler` | argued: connect timeout on the client and an explicit `.timeout(...)` on every request (greps above) |
| `RemoteHttpComponent` (network module) | argued: client-side driver, not a server worker thread; sets `.connectTimeout(Duration.ofSeconds(60))` |

### Reachability

`LeaderCommandForwarder` is constructed on a live path - `HttpServer` line 153,
`this.leaderCommandForwarder = new LeaderCommandForwarder(this)` - and reached from all four
handlers above. No feature flag gates it. The connect timeout is read once when the client is
built (an `HttpClient`'s connect timeout is immutable); the response deadlines are re-read from
`ContextConfiguration` on every forward, so `SET SERVER SETTING arcadedb.ha.proxyReadTimeout` takes
effect without a restart.

### Residual risk

A forwarded `restore` or `import` still occupies its Undertow worker thread for as long as it runs,
now up to `HA_PROXY_LONG_COMMAND_TIMEOUT` (1 h by default) rather than forever. That is the bug
fixed - bounded instead of unbounded - but it is not the same as cheap: several concurrent forwarded
restores still hold several workers for the duration. Handing the forward off to a thread that is not
a request worker would remove even that, and is a different change from this one. Flagged rather than
filed: nobody has hit it, and the shape of the fix depends on whether the client is meant to keep
holding the connection.

The two rows marked "filed" above still use an unbounded client: `/api/v1/batch`'s forward (#7542)
and the Raft SQL-write forward (#7543). Both are outside this issue - different call paths, and
their deadlines are separate policy questions (the batch forward streams a body of arbitrary size;
a forwarded SQL command's duration is bounded by the query, not by a command name) - and both now
have their own issue.

## The fix

### `GlobalConfiguration`

- **New** `HA_PROXY_LONG_COMMAND_TIMEOUT` (`arcadedb.ha.proxyLongCommandTimeout`, `Long`, default
  `3_600_000` ms): the deadline for a forwarded `restore backup` / `restore database` /
  `import database`. Finite, because it is still a worker thread that waits, but wide enough not to
  abort a real restore.
- `HA_PROXY_READ_TIMEOUT` and `HA_PROXY_CONNECT_TIMEOUT` keep their keys and defaults (30 s / 5 s);
  their descriptions said "for the leader proxy in `AbstractServerHttpHandler`", which named only
  `LeaderProxy` - dead code, nothing constructs it. They now say what they actually bound and where.

### `LeaderCommandForwarder`

- The static `HttpClient.newHttpClient()` becomes a per-instance client built with
  `.connectTimeout(HA_PROXY_CONNECT_TIMEOUT)`. One `LeaderCommandForwarder` per `HttpServer`, so the
  client picks up that server's configuration instead of a JVM-wide default.
- A new package-private `Transport` owns the client and the deadlines. `Transport.newRequest` is the
  only way the class builds a request and it attaches `.timeout(responseTimeout(longRunningCommand))`
  unconditionally, so no call site can issue an unbounded forward.
- `Transport.send` issues the exchange with `sendAsync` and awaits it with the same deadline applied
  to the **whole** exchange, cancelling the future when it expires. `HttpRequest.timeout` alone is not
  enough - see the adversarial pass below - but it is kept, because it aborts the exchange at the JDK
  level rather than only releasing this thread.
- A blown deadline comes back as **HTTP 504** naming the leader address, the deadline that actually
  applied (read back off the request, so the message cannot quote a number the forward was never
  given) and the setting that bounds it, plus one `WARNING` in the follower's log. Previously an
  `HttpTimeoutException` would have escaped as an `IOException` and degraded to a generic 500.
- A connect timeout gets its own 504 naming `HA_PROXY_CONNECT_TIMEOUT`, because it is a different
  failure pointing at a different knob - and it is the one case where the follower can promise the
  command did not run.
- The client is released in `HttpServer.stopService()`. It is per-`HttpServer` now rather than a JVM
  static, and a `java.net.http.HttpClient` owns a selector thread and an executor.
- `forwardIfReplica` gains a 5-argument overload taking `longRunningCommand`; the existing 4-argument
  signature delegates with `false`, so the three `/server/users` handlers are unchanged and get the
  ordinary deadline.
- 0 or a negative configured value clamps to 1 ms rather than falling back to the default. Both JDK
  builders reject a non-positive `Duration`, and treating 0 as "no timeout" would put back the exact
  behaviour this issue removes.

### `PostServerCommandHandler`

- New package-private `isLongRunningForwardedCommand(String)`, and the forward passes its result
  through. `restore backup`, `restore database` and `import database` are true; `create database`,
  `drop database`, `create user`, `drop user` are false.

## Tests

`server/src/test/java/com/arcadedb/server/http/handler/Issue7507LeaderForwardTimeoutTest.java`, 9 tests:

| Test | What it pins |
|---|---|
| `aLeaderThatAcceptsAndNeverAnswersIsGivenUpOnAndAnswered504` | the reported failure over a real socket: a `ServerSocket` that accepts and never replies. `StallAwareStopwatch.assertGaveUpWithin` is the tripwire between the 1 s deadline and the unbounded wait; the answer is 504 and names the address and the setting |
| `aLeaderThatAnswersTheHeadersAndThenStallsMidBodyIsAlsoGivenUpOn` | the case `HttpRequest.timeout` alone does not catch: a complete response head promising 100 bytes, then five (adversarial finding 1) |
| `aLeaderThatCannotBeConnectedToIsAnsweredWithItsOwnGatewayTimeout` | a failure to connect names `HA_PROXY_CONNECT_TIMEOUT`, not the response one, and says the command did not run |
| `everyForwardedRequestCarriesTheDefaultDeadline` | the `POST` / `PUT` / `DELETE` `/server/users` shapes all come out with the configured deadline attached |
| `longRunningCommandsGetTheLongerDeadline` | a long-running forward uses `HA_PROXY_LONG_COMMAND_TIMEOUT`, and it is the larger of the two |
| `onlyRestoreAndImportAreClassifiedAsLongRunning` | all seven forwarded commands, each classified |
| `theClientCarriesTheConfiguredConnectTimeout` | the connect half of the bound |
| `responseDeadlineIsReReadOnEveryForward` | `SET SERVER SETTING` moves the deadline without a restart |
| `zeroOrNegativeTimeoutClampsInsteadOfDisablingTheBound` | 0 is not a back door to the old behaviour |

`Issue7507ForwarderClientLifecycleTest.java`, 1 test: a real server on a free port, started and
stopped, asserting `stopService()` releases the forwarder's `HttpClient` (adversarial finding 3).

### Proof the tests can fail

Four mutations, each run against the tests they should break - the table at the end of this document.

### Test results

```
mvn -o -pl server test -Dtest=Issue7507LeaderForwardTimeoutTest,Issue7507ForwarderClientLifecycleTest,OpenApiSpecGeneratorTest
  Tests run: 13, Failures: 0, Errors: 0, Skipped: 0

mvn -o -pl server test -Dtest=Issue7507LeaderForwardTimeoutTest,LeaderProxyTest,ExecutionResponseTest,PostBatchHandlerForwardRequestTest
  Tests run: 24, Failures: 0, Errors: 0, Skipped: 0

mvn -o -pl engine test -Dtest=ConfigurationTest,ContextConfigurationTest,GlobalConfigurationTest,GlobalConfigurationReadinessHATest,Issue7121GlobalConfigurationSweepTest,...
  Tests run: 109, Failures: 0, Errors: 0, Skipped: 0

mvn -o -pl ha-raft test -Dtest=HAConfigDefaultsTest
  Tests run: 9, Failures: 0, Errors: 0, Skipped: 0

mvn -o install -DskipTests      # full reactor, main + test sources of every module
  BUILD SUCCESS
```

Not run locally: the server-module suites that bind port 2480. Two other JVMs held that port for the
whole session (`lsof -nP -iTCP:2480 -sTCP:LISTEN`), and a run against an occupied port reports
authentication errors rather than a port conflict. Those suites do not reach the changed code anyway -
`forwardIfReplica` returns null immediately when `getHA()` is null, which is every non-HA test server -
and CI runs them.

## Impact

- A wedged leader now costs a follower one worker thread for at most `HA_PROXY_READ_TIMEOUT`
  (30 s by default), or `HA_PROXY_LONG_COMMAND_TIMEOUT` (1 h) for a restore or an import, instead of
  until the OS tears the socket down.
- Clients of the four forwarding routes can see a new status: **504** where they previously saw the
  request hang and eventually fail as a 500. The body is JSON with an `error` field, the same shape
  every other error on these routes uses.
- No behaviour change when the leader answers normally, and none at all on a leader or a non-HA server:
  `forwardIfReplica` still returns null before touching the transport.
- One `java.net.http.HttpClient` per `HttpServer` rather than one per JVM, released on `stopService()`.


## Adversarial pass

No isolated subagent was available in this session (no `Task` tool), so the pass was run against the
cold diff rather than by a second agent - a weaker version of the exercise, and worth saying so. It
found five things; four were real and are fixed here.

### 1. `HttpRequest.timeout` does not cover a body that stalls after the headers (REAL - fixed here)

The first version of the fix attached `.timeout(...)` to the request and called `HttpClient.send`.
That bounds the wait for the response **headers** only. Measured, against a socket that answered a
complete response head promising `Content-Length: 100` and then wrote five bytes:

```
HttpRequest.timeout = 1500ms, waited > 180s, send() had still not returned
```

A leader wedged *after* answering would therefore still have parked the worker thread - the exact bug
this issue is about, surviving its own fix. `Transport.send` now awaits the exchange with
`sendAsync(...).get(deadline)` so the bound covers the body, and
`aLeaderThatAnswersTheHeadersAndThenStallsMidBodyIsAlsoGivenUpOn` pins it.

### 2. The connect timeout named the response-timeout setting (REAL - fixed here)

`HttpConnectTimeoutException` extends `HttpTimeoutException`, so the single catch answered a failure
to *connect* with "did not answer within ... (arcadedb.ha.proxyReadTimeout)", sending the operator to
the wrong setting. Verified against a live JDK 21 runtime dialling 192.0.2.1 (RFC 5737 TEST-NET-1)
with an 800 ms connect timeout:

```
cause=java.net.http.HttpConnectTimeoutException  msg=HTTP connect timed out
cause.cause=java.net.ConnectException
```

There is now a separate arm and a separate message. It is not driven over a real socket in the tests -
no address reliably black-holes a connect on every CI network - so the branch is argued by the
measurement above and the message pinned by
`aLeaderThatCannotBeConnectedToIsAnsweredWithItsOwnGatewayTimeout`.

### 3. The per-server `HttpClient` was never released (REAL - fixed here)

Giving the forwarder the server's configured connect timeout meant one client per `HttpServer` in
place of one JVM static, and each owns a selector thread and an executor. Nothing closed it.
`HttpServer.stopService()` now calls `LeaderCommandForwarder.close()`, and
`Issue7507ForwarderClientLifecycleTest` starts a real server on a free port, stops it, and asserts the
client is terminated (it fails if the one-line wiring is removed).

### 4. The 504 told the client the command had not run (REAL - fixed here)

The first message said "The command was not executed on this node: retry it". True but misleading: the
leader may well be executing it, and `restore database` is not something to retry blind. The message
now says the command may still be running on the leader and to check there before retrying. The
connect-timeout 504 is the only one that says the command did not run, because that is the only case
where the follower knows.

### 5. The new 504 was undocumented in the OpenAPI spec (REAL - fixed here)

`POST /api/v1/server` and the three write operations on `/api/v1/server/users` now declare it.
Deliberately not added to the shared helpers: `createCommandResponses()` is also used by
`POST /api/v1/command/{database}`, and `createAdminResponses()` by the groups, API-token and
list-users operations, none of which forward.

### Not real

- *"`ConnectException` (connection refused) now surfaces differently."* It does not. `sendAsync` wraps
  it in an `ExecutionException`, and the `cause instanceof IOException io -> throw io` arm rethrows the
  original, so a refused connection reaches the handler exactly as before.
- *"The group and API-token routes forward too."* They do not:
  `grep -rn 'forwardIfReplica' --include='*.java' */src/main/java` lists four call sites and none of
  them is `PostGroupHandler`, `DeleteGroupHandler`, `PostApiTokenHandler` or `DeleteApiTokenHandler`.
  That those routes do not forward at all is a separate gap, not this one.

## Proof the tests can fail (all four mutations)

| Mutation | Result |
|---|---|
| drop `.timeout(...)` from `Transport.newRequest` | `Tests run: 3, Failures: 3` |
| rethrow instead of mapping the timeout to 504 | `aLeaderThatAcceptsAndNeverAnswers... » HttpTimeout request timed out` |
| widen the future's await to 600 s (an unbounded wait in practice) | `There was a timeout in the fork` under `-Dsurefire.timeout=45` |
| remove `leaderCommandForwarder.close()` from `stopService()` | `Tests run: 1, Failures: 1` |
