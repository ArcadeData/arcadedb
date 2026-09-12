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
| `LeaderProxy.tryProxy` | yes, but **nothing constructs LeaderProxy** - see Reachability (#7547) | yes (scheme choice only) |
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
  per entry point, not by an end-to-end TLS forward. The issue reporter did not reproduce on one
  either.
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
