# #7508 - the follower-to-leader forward always dials `http://`

Issue: https://github.com/ArcadeData/arcadedb/issues/7508
Branch: `fix/7508-ha-leader-forward-https-scheme`
Type: bug (labels `bug`, `server`, `ha`, milestone 26.10.1)

## Report

Every follower-to-leader forward builds an `http://` URL from `HAServerPlugin.getLeaderAddress()`,
which is the only leader endpoint the interface exposes. On an SSL cluster the forward therefore
dials the plaintext listener, while every other peer-to-peer RPC in the cluster already prefers the
peer's HTTPS endpoint.

## Analysis

### What the report gets right

`LeaderCommandForwarder` and `LeaderProxy` both hardcode the scheme, and `HAServerPlugin` exposes
only one address:

```
$ grep -rn '"http://"' --include='*.java' . | grep -v '/test/'
ha-raft/.../LeaderDatabaseQuery.java:136:      return new Endpoint("http://" + httpAddr + "/api/v1/cluster/bootstrap-state", false);
ha-raft/.../RaftHAServer.java:936:    final String url = (https ? "https://" : "http://") + followerAddr + "/api/v1/cluster/resync/"
ha-raft/.../BootstrapElection.java:453:        .uri(URI.create("http://" + httpAddr + "/api/v1/cluster/bootstrap-state"))
ha-raft/.../PeerAuthSessionQuery.java:144:        : "http://" + dial.httpAddress() + ROUTE;
ha-raft/.../RaftHAPlugin.java:427:          new URL("http://" + targetAddr + "/api/v1/server").openConnection();
ha-raft/.../RaftReplicatedDatabase.java:3522:        .uri(URI.create("http://" + leaderHttpAddress + "/api/v1/command/" + getName()))
ha-raft/.../SnapshotInstaller.java:977:      final String snapshotUrl = (https ? "https://" : "http://") + endpoint + "/api/v1/ha/snapshot/" + databaseName;
ha-raft/.../PeerCapabilityQuery.java:170:      return "http://" + httpAddr + "/api/v1/cluster/capabilities";
server/.../LeaderProxy.java:106:    final String urlString = "http://" + leaderAddress + path + ...
server/.../LeaderCommandForwarder.java:144:      leaderUri = URI.create("http://" + leaderHttpAddress + targetPath);
server/.../PostBatchHandler.java:1586:    String url = "http://" + leaderAddress + "/api/v1/batch/" + databaseName;
(non-HA hits in integration/, network/, engine/ omitted - they parse user-supplied URLs)
```

`SnapshotInstaller`, `RaftHAServer.forceResyncStalledReplica`, `LeaderDatabaseQuery.chooseEndpoint`,
`PeerCapabilityQuery.chooseUrl` and `PeerAuthSessionQuery` all already select the scheme with the
same rule: `useSSL && httpsAddress != null`. Only the *forward* path does not.

### What the report gets wrong, and the impact that is actually real

The report frames the failure as "a cluster that listens on HTTPS only - the plain HTTP listener
disabled". **ArcadeDB has no such mode.** `HttpServer.buildUndertowServer` binds the plain listener
unconditionally and adds the HTTPS one on top:

```
$ sed -n '354,372p' server/src/main/java/com/arcadedb/server/http/HttpServer.java
  private Undertow buildUndertowServer(...) {
    final Undertow.Builder builder = Undertow.builder()
        ...
        .addHttpListener(httpPortListening, host)      <- unconditional
        ...
    if (configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL)) {
      final SSLContext sslContext = createSSLContext();
      builder.addHttpsListener(httpsPortListening, host, sslContext)
```

So the connection error the issue describes needs the plain port to be blocked from outside the
process (a firewall, a NetworkPolicy, a service mesh that only exposes the TLS port) rather than
simply unconfigured. Two consequences are real without any of that, though, and they are what this
fix is actually for:

1. **The forward silently downgrades inter-node traffic to cleartext on an SSL cluster.** The
   forwarded request carries either the client's own `Authorization` header (Basic credentials or an
   API token, relayed verbatim) or the cluster token in `X-ArcadeDB-Cluster-Token`, plus the whole
   request body. An operator who set `arcadedb.ssl.enabled` gets every other peer-to-peer RPC
   encrypted and this one in the clear.
2. **On a cluster that declares `https` ports but not `http` ones, the forward is refused outright.**
   The HTTP address is then *derived* as the peer's Raft host plus **this** node's HTTP port
   (`RaftHAServer.resolveHttpAddress`), so `isOwnHttpAddress(leaderAddress)` answers true and the
   forwarder throws `ServerIsNotTheLeaderException` - while the correctly declared HTTPS endpoint sits
   unused in `httpsAddresses`.

## Completeness

### Invariant

> A node forwarding a request to the cluster leader dials the leader's HTTPS endpoint whenever SSL is
> enabled and one resolves for it, and plain HTTP only when it does not - the same rule every other
> peer-to-peer dial in the cluster already follows.

### Enumeration

Writers/readers of the leader endpoint:

```
$ grep -rn "getLeaderAddress\|getLeaderHttpAddress" --include='*.java' . | grep -v '/test/' | grep -v '/remote/'
server/.../HAServerPlugin.java:109:  String getLeaderAddress();                         <- the only endpoint on the interface
server/.../GetServerHandler.java:121:      final String leaderServer = ha.getLeaderAddress();      <- reports it, never dials
server/.../PostBatchHandler.java:1562:    final String leaderAddress = ha.getLeaderAddress();     <- DIALS
server/.../LeaderCommandForwarder.java:125:    final String leaderHttpAddress = ha.getLeaderAddress(); <- DIALS
server/.../AbstractServerHttpHandler.java:715/717                                     <- error text only
ha-raft/.../RaftHAPlugin.java:390:  public String getLeaderAddress()                   <- delegates to RaftHAServer
ha-raft/.../RaftHAServer.java:1851:  public String getLeaderHttpAddress()
ha-raft/.../ArcadeStateMachine.java:3382,4369                                         <- snapshot source, already HTTPS-aware
ha-raft/.../RaftReplicatedDatabase.java:3489/3522                                     <- DIALS
grpcw/.../GrpcErrorMapper.java:201, ArcadeDbGrpcAdminService.java:1180                <- error text only (gRPC refuses, never forwards)
```

Same-shape siblings - every unattended peer dial, whether or not it is a leader forward - are the
`"http://"` grep above.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `LeaderCommandForwarder.forwardIfReplica` - `POST /api/v1/server`, `POST`/`PUT`/`DELETE /api/v1/server/users` | yes | yes |
| `PostBatchHandler.forwardBatchToLeader` - `POST /api/v1/batch/{db}` | yes | yes |
| `RaftReplicatedDatabase.forwardCommandToLeaderViaRaft` - SQL write on a follower | yes | yes |
| `LeaderProxy.tryProxy` | yes, but **nothing constructs LeaderProxy** - see Reachability (#7547) | the refusal branch only; its HTTPS dial reads the body through `exchange.startBlocking()`, which a detached exchange cannot serve |
| `RaftHAPlugin.shutdownRemoteServer` - peer dial, not a leader forward | no - filed as #7546 | no |
| `BootstrapElection.bootstrapStateRequest` - peer dial before any leader exists | no - filed as #7546 | no |
| `LeaderDatabaseQuery`, `PeerCapabilityQuery`, `PeerAuthSessionQuery`, `SnapshotInstaller`, `RaftHAServer.forceResyncStalledReplica`, `PostVerifyDatabaseHandler` | argued: already select the scheme with the same rule | n/a |

### Reachability

`LeaderProxy` is **not on any live path**:

```
$ grep -rn "new LeaderProxy" --include='*.java' .
(no output)
```

`HttpServer` constructs `LeaderCommandForwarder` (line 153) but never a `LeaderProxy`, so the three
`arcadedb.ha.proxy*` settings it reads configure nothing today. Its scheme is fixed here for
consistency, and the fix is a no-op at runtime until something wires the proxy up. Filed as #7547.

The other three entry points are reachable: `LeaderCommandForwarder` from four HTTP routes,
`PostBatchHandler.forwardBatchToLeader` from `POST /api/v1/batch/{db}`, and
`RaftReplicatedDatabase.forwardCommandToLeaderViaRaft` from every write submitted on a follower.

### Residual risk

- No integration test starts a genuine HTTPS HA cluster: the scheme decision is proved by unit tests
  per entry point, not by an end-to-end TLS forward, so hostname verification, SNI and real
  truststore loading on this path stay uncovered. The issue reporter did not reproduce on one
  either. **Filed as #7563.**
- Truststore rotation rebuilds the forward's HTTPS client from the truststore's path, password,
  mtime and size - the same fingerprint `TrustedHttpClientCache` uses - but the two implementations
  are separate.

## The fix

One decision point, `server/src/main/java/com/arcadedb/server/http/handler/LeaderDial.java`, which every
forward now asks instead of concatenating a scheme of its own:

```java
public record LeaderDial(String address, boolean https, HttpClient client) {
  public static LeaderDial resolve(final HAServerPlugin ha, final HttpClient plainClient) { ... }
  public String url(final String pathWithQuery) { ... }
}
```

It reads two new methods on `HAServerPlugin`, both defaulting to "there is no HTTPS endpoint", so every
implementation but `RaftHAPlugin` is left exactly where it was:

- `getLeaderHttpsAddress()` - the endpoint to prefer, or `null`;
- `getPeerHttpsClient()` - a client that validates the peer certificate against this node's truststore.

`RaftHAPlugin` serves the first from `RaftHAServer.getLeaderHttpsAddress()` and the second from a
`TrustedHttpClientCache` of its own (`forwardHttpsClients`), closed with the server. The address is
withheld - never refused - in the three cases the pure `RaftHAServer.preferredLeaderHttpsAddress` covers:
SSL off, no HTTPS endpoint resolved, or the endpoint resolved is this node's own.

The self-address guard each caller already ran moved onto the address actually dialled, and is asked only
of the plain-HTTP branch: `isOwnHttpAddress` compares against this node's *HTTP* listener and cannot speak
for an HTTPS endpoint, which is why the plugin owns that check for the HTTPS one. That is what closes the
second failure mode above - a cluster that declares `https` ports and not `http` ones now forwards over
TLS instead of refusing itself.

Files changed:

| File | Change |
|---|---|
| `server/.../HAServerPlugin.java` | two default methods: `getLeaderHttpsAddress()`, `getPeerHttpsClient()` |
| `server/.../http/handler/LeaderDial.java` | new - the shared decision |
| `server/.../http/handler/LeaderCommandForwarder.java` | dials `LeaderDial`, self-check on the plain branch only |
| `server/.../http/handler/PostBatchHandler.java` | same; `forwardBatchToLeader` relaxed to package-private for the test |
| `server/.../http/handler/LeaderProxy.java` | same (see Reachability - #7547) |
| `ha-raft/.../RaftHAServer.java` | `getLeaderHttpsAddress()` + pure `preferredLeaderHttpsAddress`, `forwardHttpsClients` cache closed in `stop()` |
| `ha-raft/.../RaftHAPlugin.java` | implements both new plugin methods |
| `ha-raft/.../RaftReplicatedDatabase.java` | the SQL write forward dials `LeaderDial` |

## Tests

- `server/src/test/java/com/arcadedb/server/http/handler/Issue7508LeaderForwardSchemeTest.java` - 10 tests.
  Five drive `LeaderDial.resolve` (HTTPS wins; no HTTPS endpoint; an HTTPS endpoint with no client; trust
  material unreadable; no leader at all). Five drive the two reachable server-module entry points through a
  recording `HttpClient`, asserting the exact URI dialled, including the case where the HTTP self-address
  check used to refuse a forward that now travels over TLS.
- `ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue7508LeaderHttpsEndpointTest.java` - 5 tests on the
  withhold policy, loopback equivalence included.

Proved able to fail: with `getLeaderHttpsAddress()` forced to `null` (the pre-fix behaviour), 4 of the 10
server tests and 2 of the 5 ha-raft tests go red; restoring the fix returns all 15 to green.

`RaftReplicatedDatabase`'s forward is covered at the decision it makes (`LeaderDial.resolve` plus
`RaftHAServer.preferredLeaderHttpsAddress`), not at its HTTP send: driving that needs a live TLS cluster.

### Regression runs

| Suite | Result |
|---|---|
| `mvn -o -pl server test -DexcludedGroups=benchmark,vector,slow` | 1105 tests, 10 failures + 5 errors |
| `mvn -o -pl ha-raft test -DexcludedGroups=benchmark,vector,slow` | 449 tests, 0 failures; `LeaveClusterTest` crashes its fork |

Both sets of failures are pre-existing on this machine and were reproduced on the base commit
(`99b6692780`) in a throwaway worktree:

- the `server` failures are the fixed-port trap `CLAUDE.md` documents. `lsof -nP -iTCP:2480 -sTCP:LISTEN`
  shows two foreign JVMs holding 2480/2481, and the failing classes - `AutoCommitParameterTest`,
  `HttpBodySizeLimitTest`, `Issue6220TruncateHttpDefaultTest`, `ClusterInternalAuthTest`,
  `PostClusterAuthSessionHandlerTest` - are exactly the ones that bind them. At base they fail the same way
  (11 failures + 5 errors of the same 24 tests). None of them reaches a leader forward: a single-node test
  server has no HA plugin, so `forwardIfReplica` returns before `LeaderDial` is consulted.
- `LeaveClusterTest` (a 3-node `BaseRaftHATest` cluster on fixed ports) crashes its surefire fork
  identically at base.

## Adversarial pass

The orchestrator's isolated subagent could not be spawned - no `Task` tool is exposed in this environment -
so the pass was run by re-reading the diff against the issue rather than by an uncontaminated reader. That
is a weaker pass and is recorded as such. Four findings, all fixed before the PR opened:

1. **`RaftReplicatedDatabase` vetted one address and dialled another.** It awaits the leader's HTTP address
   through `awaitLeaderAddress(raft::getLeaderHttpAddress, ...)` and runs `isOwnHttpAddress` on *that*
   string, but the first version of the change built its URL from `LeaderDial.resolve`, which re-reads
   `getLeaderAddress()`. A leadership change between the two reads would have dialled an address the
   self-check never saw. Fixed: only the encrypted half is taken from the dial; the plain-HTTP address stays
   the awaited one. `LeaderProxy` got the same treatment, since its plain address is the caller's parameter.
2. **A comment described code six lines below it.** The new dial resolution had been inserted between the
   #6191 self-address comment and the `if` it explains. Fixed by moving the resolution above that block.
3. **`LeaderDial`'s Javadoc claimed "every other peer-to-peer dial in the cluster" already selects its
   scheme.** The `"http://"` grep in the Enumeration section disproves that: `BootstrapElection` and
   `RaftHAPlugin.shutdownRemoteServer` do not. Reworded to name the five that do and to point at #7546 for
   the two that do not.
4. **A per-forward cost on the HTTPS branch.** `getPeerHttpsClient()` goes through
   `TrustedHttpClientCache.clientFor`, which stats the truststore twice and SHA-256s its password on every
   call to decide whether to rebuild - sized in #7301 for a probe every few seconds, not for a request. Kept
   as is, with the reasoning stated rather than assumed: the cost is paid only when the dial is actually
   HTTPS (a non-SSL cluster never calls it), and it is two `stat`s and one short digest against a
   cross-node HTTP round trip the call is about to make. If the forward ever needs more, the fix is a
   time-bounded revalidation inside that cache, which would serve the capability probe too.

---

# Appendix: review log

Everything above is the durable record - the invariant, the enumeration, the coverage table, the
design and the residual risk. What follows is dated correspondence with the PR reviewers, kept so a
later reader can see why a given branch behaves as it does rather than having to reopen the PR.

## Review cycle 1 - `7198db162a`

### CodeRabbit, inline on `HAServerPlugin.java:120` - "Fail closed when TLS is required" (Major, CWE-319) - APPLIED, in part

Correct for one of the two fallback branches, and the distinction matters enough to state:

- **No HTTPS endpoint resolves for the leader.** That is what every SSL cluster answers when it left the optional
  5th field of `arcadedb.ha.serverList` out, and dialling the plain listener is what it did before this PR. Failing
  closed here would turn a working cluster into a broken one on upgrade, and it is the case `PeerDialAddress`,
  `LeaderDatabaseQuery`, `PeerCapabilityQuery` and `SnapshotInstaller` all deliberately fall back on (#6221). Left
  falling back.
- **An HTTPS endpoint IS named and no client can be built for it.** Here the operator has said where the forward
  belongs and this PR's new code path silently downgraded it. `SnapshotInstaller.buildSSLContext` already falls
  back to the JVM default truststore when none is configured, so a null-or-throwing client means the trust
  material itself could not be loaded - a misconfiguration, not an "undeclared" state. **Now refused.**

`LeaderDial` gained `refusal` / `refused()`, the shape `PeerDialAddress` already uses, and each caller turns it
into its own error: `ServerIsNotTheLeaderException` for `LeaderCommandForwarder` and `RaftReplicatedDatabase`, a
503 for `PostBatchHandler`, `false` for `LeaderProxy`. Three tests added - the refusal at the decision, and at each
of the two reachable entry points - and the two that asserted the old downgrade now assert the refusal. A fourth
new test pins the branch that must NOT be refused, so a later "make it stricter" cannot quietly break every
cluster that never declared its `https` ports.

### claude - `LeaderDial.HTTPS_CLIENT_FALLBACK_WARNED` is static, not per-server - NOT APPLIED, with evidence

The premise does not hold. A static one-time latch is the established pattern for exactly this notice:

```
$ grep -rn "static final AtomicBoolean" --include='*.java' ha-raft/src/main server/src/main
ha-raft/.../SnapshotHttpHandler.java:104:  private static final AtomicBoolean WARNED_MISCONFIGURED_LIMIT = ...
ha-raft/.../LeaderDatabaseQuery.java:73:   private static final AtomicBoolean PLAIN_HTTP_FALLBACK_WARNED = ...
ha-raft/.../SnapshotInstaller.java:158:    private static final AtomicBoolean PLAIN_HTTP_FALLBACK_WARNED = ...
ha-raft/.../PeerCapabilityQuery.java:81:   private static final AtomicBoolean PLAIN_HTTP_FALLBACK_WARNED = ...
```

Three of those carry the same "SSL is on but this dial fell back to plain HTTP" notice this one carries.
`TrustedHttpClientCache` is per-server because it owns a *resource* - an `HttpClient` with a selector thread and a
connection pool - whose lifetime must match the server's, which is a different question from how often a log line
repeats. Taking the reviewer's own stated alternative ("or explicitly deciding to leave as-is, since it's
log-only"): the latch is kept and its Javadoc now names the three precedents and the JVM-wide scope, so the
decision is on the page rather than implied. The message already says "logged only once".

The reviewer's other points - the per-forward `TrustedHttpClientCache` cost, the `LeaderProxy` dead code, the
uncovered `RaftReplicatedDatabase` send - restate gaps this PR already declares (#7546, #7547); no change.

### CodeRabbit pre-merge check - "Docstring Coverage 26.92%" - NOT APPLIED

Counts every function in the 10 touched files, not the ones this diff adds; the added members carry Javadoc.
Writing docstrings for untouched methods to clear a bot threshold is not in scope for a bug fix.

## Review cycle 2 - `33a9d64128`

CodeRabbit re-verified its own finding against the new code and accepted the split: "`LeaderDial.resolve`
now refuses the forward when `getLeaderHttpsAddress()` returns an endpoint but `getPeerHttpsClient()` is
unavailable or throws ... The remaining HTTP fallback occurs only when no HTTPS endpoint resolves. This
preserves compatibility for clusters that never declared HTTPS endpoints." Thread answered by the bot
itself; no code change.

The `claude` review found no bugs and said nothing blocks the merge. Its five points:

1. **Lock contention on the new hot path.** `forwardHttpsClients.clientFor()` is `synchronized` and is now
   called by every HTTP worker thread forwarding on an SSL cluster, where the existing
   `capabilityHttpsClients` had one scheduled caller. During a truststore rotation the thread that observes
   the change holds the monitor across `previous.close()`, which waits for in-flight sends. **No change**:
   this is the tradeoff the field's Javadoc already states, it is bounded by the forwards' own request
   timeouts, and it happens only when an operator rotates a certificate. Replacing it would mean a
   revalidation window inside `TrustedHttpClientCache`, which is a change to the capability probe's
   behaviour too and does not belong in this fix.
2. **Per-forward cost** of the two `stat`s and the password digest - already reasoned about above. No change.
3. **No live-TLS integration test.** Agreed, and it is the one gap unit tests structurally cannot close on
   a path that decides whether credentials are encrypted. **Filed as #7563**, with the fixture shape and the
   other peer-to-peer dials the same fixture would cover.
4. **This document mixes a durable design record with an ephemeral review log.** Fair. Split with the
   appendix heading above rather than dropped: the orchestrating workflow requires the review history to be
   recorded here, so it is now labelled as correspondence instead of reading as design.
5. **`LeaderProxy` is dead code.** Already declared, tracked as #7547. No change.

## Review cycle 3 - `8bae0665e1`

The `claude` review found no bugs and nothing blocking. Three minor findings, all applied:

1. **Fully-qualified names in the new test fixtures.** `CLAUDE.md` asks for imports. Mechanical; the
   `RecordingHttpClient`/`CannedResponse` members now use `SSLContext`, `SSLParameters`, `SSLSession`,
   `CookieHandler`, `ProxySelector`, `Authenticator`, `Duration`, `CompletableFuture` and `Executor` by name.
2. **The coverage table overclaimed `LeaderProxy`.** It said "yes (scheme choice only)" while nothing
   exercised the branch at all - `LeaderProxyTest` is a placeholder that never calls `tryProxy`. Corrected,
   and a test was added for the half that can be driven: the refusal short-circuit.
   **That test found a real ordering flaw.** The refusal was checked *after* `readBodyCapped`, so a request
   that was never going to be relayed still buffered an upload of up to `arcadedb.ha.proxyMaxBodySize`,
   holding a request thread and that much heap. The dial resolution and its refusal now sit before the body
   read, next to the loop-prevention refusal that is there for the same reason.
3. **A theoretical "just became leader" window** in `RaftReplicatedDatabase`, where the refusal is checked
   before the `raft.isLeader()` branch. It cannot produce a false positive, and the reasoning is now a
   comment rather than an assumption: a refusal needs `getLeaderHttpsAddress()` to have named an endpoint,
   and on a node that has just become the leader `getLeaderId()` and `localPeerId` are the same id, so that
   method compares one `resolveHttpsAddress()` against itself and withholds. The existing
   `nothingIsOfferedWhenTheResolvedHttpsEndpointIsThisNodesOwn` pins exactly that input.

### Regression re-run after cycle 3

`mvn -o -pl ha-raft test -DexcludedGroups=benchmark,vector,slow` reached 1409 tests this time - the earlier
449 was cut short by `LeaveClusterTest` crashing its fork - with 2 failures in
`ArcadeStateMachinePerDatabaseHaltTest`, a WAL-entry-parsing assertion that touches nothing in this change.
Reproduced identically on base commit `99b6692780`.

## Final state

`clean-approval` - the latest review from each bot says nothing blocks the merge, the one CodeRabbit
thread was re-verified and answered by CodeRabbit itself, and the three known gaps carry issue numbers
(#7546, #7547, #7563). Every review finding across the three cycles is either applied or answered with
evidence; none is deferred.
