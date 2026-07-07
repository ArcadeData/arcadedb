# Bolt HA-aware ROUTE response Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Bolt `ROUTE` response reflect real HA cluster topology - advertise the leader as the writer and followers as readers - so `neo4j://` drivers can discover and route against an ArcadeDB cluster.

**Architecture:** The `bolt` module reaches the cluster only through the `HAServerPlugin` interface (server module). We add per-node Bolt address knowledge to HA membership by extending the `HA_SERVER_LIST` object form with an optional `bolt:<port>` field, resolve it in `RaftHAServer` (with a homogeneous-cluster fallback mirroring the existing HTTP handling), surface it via two new `HAServerPlugin` methods, and rebuild the routing table from live membership in `BoltNetworkExecutor.handleRoute`. Single-node behavior is preserved as the fallback.

**Tech Stack:** Java 21, Maven multi-module, Apache Ratis (Raft), JUnit 5 + AssertJ, neo4j-java-driver (test), custom PackStream Bolt codec.

> **Post-review deltas (final code differs from the task steps below):** code review reshaped three points.
> (1) The two-method interface (`getLeaderBoltAddress()` + `getReplicaBoltAddresses()`) was collapsed into a
> single `HAServerPlugin.BoltRoutingTable getBoltRoutingTable()` computed from one `getLeaderId()` read, to
> remove the writer/reader leader-disagreement window; `readers` is an immutable `List.copyOf`. (2)
> `handleRoute` is gated behind an authenticated session (`state == READY`) because ROUTE enumerates cluster
> Bolt endpoints. (3) The fallback distinguishes HA-active-but-leaderless (advertise `READ` + `ROUTE` only,
> never `WRITE`) from true single-node (`WRITE`/`READ`/`ROUTE`), and uses `socket.getLocalPort()` for the
> self address. The task steps below capture the original plan; the spec's section 2/3 reflect the final design.

## Global Constraints

- Java 21+; import classes, do not use fully-qualified names inline.
- Use `final` on variables and parameters; single-statement `if` needs no braces.
- No new dependencies. No Claude authorship in source. Remove any debug `System.out`.
- Never use the em dash character in code, comments, or docs.
- Comments state behavioral invariants only - no issue numbers in Javadoc/comments.
- New/changed backend code must have a test; compile the touched modules before declaring done.
- Cluster ITs are slow: annotate `@Tag("slow")` at class level.
- The bolt module must NOT reference any `ha-raft` class in `src/main/java` (ha-raft is test-scope only there); production Bolt code uses only `com.arcadedb.server.HAServerPlugin`.
- `neo4j+s`/TLS reuse existing `BoltSslHelper`; no new TLS work. No change to advertised server identity (`Neo4j/5.26.0 compatible ...`).

## File Structure

- Modify `ha-raft/.../raft/RaftPeerAddressResolver.java` - parse optional object-form `bolt:` field; add `boltAddresses` to `ParsedPeerList`.
- Modify `ha-raft/.../raft/RaftHAServer.java` - store `boltAddresses`; add `deriveBoltAddress`, `resolveBoltAddress`, `getLeaderBoltAddress`, `getReplicaBoltAddresses`, `boltFallbackWarned`.
- Modify `server/.../server/HAServerPlugin.java` - two new default methods.
- Modify `ha-raft/.../raft/RaftHAPlugin.java` - override the two new methods.
- Modify `bolt/.../bolt/BoltNetworkExecutor.java` - HA-aware `handleRoute` with single-node fallback.
- Create `bolt/src/test/java/com/arcadedb/bolt/Bolt5002RoutingTableIT.java` - 3-node cluster IT.
- Modify `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerAddressParsingTest.java` - `bolt:` parsing unit tests.
- Modify `bolt/src/test/java/com/arcadedb/bolt/BoltProtocolIT.java` - single-node ROUTE regression assertion.
- Modify `bolt/conformance/spec.yaml` - flip `CONN-004` to `passing`.

---

### Task 1: Parse optional `bolt:` field in HA_SERVER_LIST object form

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPeerAddressResolver.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerAddressParsingTest.java`

**Interfaces:**
- Produces: `ParsedPeerList` record now has a 5th component `Map<RaftPeerId,String> boltAddresses()`, populated only when an object-form entry declares `bolt:<port>`. Constructor arity becomes 5.

- [ ] **Step 1: Write the failing tests** (append to `RaftHAServerAddressParsingTest`)

```java
  @Test
  void objectFormParsesBoltPort() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:{raft:2434,http:2480,bolt:7687}", 2434);
    final var peerId = parsed.peers().get(0).getId();
    assertThat(parsed.boltAddresses()).containsEntry(peerId, "myhost:7687");
    assertThat(parsed.httpAddresses()).containsEntry(peerId, "myhost:2480");
  }

  @Test
  void objectFormWithoutBoltLeavesBoltAddressesEmpty() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:{raft:2434,http:2480}", 2434);
    assertThat(parsed.boltAddresses()).isEmpty();
  }

  @Test
  void positionalFormNeverPopulatesBoltAddresses() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:2434:2480:7:2490", 2434);
    assertThat(parsed.boltAddresses()).isEmpty();
  }

  @Test
  void namedObjectFormParsesBoltPort() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("alpha@myhost:{raft:2434,bolt:7690}", 2434);
    final var peerId = parsed.peers().get(0).getId();
    assertThat(parsed.boltAddresses()).containsEntry(peerId, "myhost:7690");
    assertThat(parsed.peerNames()).containsEntry(peerId, "alpha");
  }

  @Test
  void unknownObjectKeyStillThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("myhost:{raft:2434,ftp:21}", 2434))
        .isInstanceOf(ServerException.class);
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl ha-raft test -Dtest=RaftHAServerAddressParsingTest`
Expected: FAIL - `boltAddresses()` method does not exist on `ParsedPeerList` (compile error).

- [ ] **Step 3: Implement the parser changes**

In `ParsedPeerList` record, add the 5th component and update its Javadoc:

```java
  public record ParsedPeerList(List<RaftPeer> peers, Map<RaftPeerId, String> httpAddresses,
                               Map<RaftPeerId, String> httpsAddresses, Map<RaftPeerId, String> peerNames,
                               Map<RaftPeerId, String> boltAddresses) {
  }
```

In `parsePeerList`, add the map and populate it. Near the other maps:

```java
    final Map<RaftPeerId, String> boltAddresses = new HashMap<>(entries.size());
```

Add a local `String boltAddress = null;` beside `httpAddress`/`httpsAddress`, set it in the object-form branch:

```java
      final int braceIdx = entry.indexOf('{');
      if (braceIdx >= 0) {
        final PeerSpec spec = parseObjectForm(entry, braceIdx, defaultPort);
        final String host = applyDnsSuffix(spec.host, k8sDnsSuffix);
        raftAddress = host + ":" + spec.raftPort;
        if (spec.httpPort != null)
          httpAddress = host + ":" + spec.httpPort;
        if (spec.httpsPort != null)
          httpsAddress = host + ":" + spec.httpsPort;
        if (spec.boltPort != null)
          boltAddress = host + ":" + spec.boltPort;
        priority = spec.priority;
      } else {
```

After the existing `httpsAddresses.put(...)` / `peerNames.put(...)` block:

```java
      if (boltAddress != null)
        boltAddresses.put(peer.getId(), boltAddress);
```

Update the `return new ParsedPeerList(...)` to pass the new map:

```java
    return new ParsedPeerList(
        Collections.unmodifiableList(peers),
        Collections.unmodifiableMap(httpAddresses),
        Collections.unmodifiableMap(httpsAddresses),
        Collections.unmodifiableMap(peerNames),
        Collections.unmodifiableMap(boltAddresses));
```

Extend `PeerSpec` with a nullable `boltPort`:

```java
  private static final class PeerSpec {
    final String  host;
    final int     raftPort;
    final Integer httpPort;
    final Integer httpsPort;
    final Integer boltPort; // null when not specified
    final int     priority;

    PeerSpec(final String host, final int raftPort, final Integer httpPort, final Integer httpsPort,
        final Integer boltPort, final int priority) {
      this.host = host;
      this.raftPort = raftPort;
      this.httpPort = httpPort;
      this.httpsPort = httpsPort;
      this.boltPort = boltPort;
      this.priority = priority;
    }
  }
```

In `parseObjectForm`, add `Integer boltPort = null;` beside the others, a `case "bolt"` branch, update the `default` message, and pass `boltPort` to the constructor:

```java
    int raftPort = defaultPort;
    Integer httpPort = null;
    Integer httpsPort = null;
    Integer boltPort = null;
    int priority = 0;
```

```java
        switch (key) {
        case "raft" -> raftPort = parseIntField(value, "raft", entry);
        case "http" -> httpPort = parseIntField(value, "http", entry);
        case "https" -> httpsPort = parseIntField(value, "https", entry);
        case "bolt" -> boltPort = parseIntField(value, "bolt", entry);
        case "priority" -> priority = parseIntField(value, "priority", entry);
        default -> throw new ServerException(
            "Unknown key '" + key + "' in peer address '" + entry + "'. Supported keys: raft, http, https, bolt, priority");
        }
```

```java
    return new PeerSpec(host, raftPort, httpPort, httpsPort, boltPort, priority);
```

Update the `parsePeerList` class Javadoc: document that the object form accepts an optional `bolt:` field (`host:{raft:2434,http:2480,bolt:7687}`), that the `boltAddresses` map is populated only when `bolt:` is present, and that `bolt:` is object-form only (the positional colon form has no Bolt field).

- [ ] **Step 4: Fix the other `ParsedPeerList` consumer**

`RaftHAServer` reads `parsed.httpAddresses()` etc. Adding a record component is additive for existing reads, but confirm no other `new ParsedPeerList(` construction exists:

Run: `rg -n "new ParsedPeerList\(" ha-raft/src`
Expected: exactly one call site (inside `parsePeerList`). If any other exists, update its arity.

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn -q -pl ha-raft test -Dtest=RaftHAServerAddressParsingTest`
Expected: PASS (all methods, including the pre-existing ones).

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftPeerAddressResolver.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerAddressParsingTest.java
git commit -m "feat(#5002): parse optional bolt: field in HA_SERVER_LIST object form"
```

---

### Task 2: Resolve per-peer Bolt addresses in RaftHAServer

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerAddressParsingTest.java` (for the static helper)

**Interfaces:**
- Consumes: `ParsedPeerList.boltAddresses()` (Task 1); existing `getLeaderId()`, `localPeerId`, `raftGroup.getPeers()`, `peerRaftAddress(RaftPeerId)`, `extractHost(String)`, `configuration`.
- Produces:
  - `static String deriveBoltAddress(String raftAddress, int boltPort)` - `host:boltPort` or `null` when `boltPort <= 0` / host unresolvable.
  - `String getLeaderBoltAddress()` - resolved leader Bolt address or `null`.
  - `String getReplicaBoltAddresses()` - comma-separated non-leader Bolt addresses, or `""`.

- [ ] **Step 1: Write the failing test for the static helper** (append to `RaftHAServerAddressParsingTest`)

```java
  @Test
  void deriveBoltAddressCombinesRaftHostWithBoltPort() {
    assertThat(RaftHAServer.deriveBoltAddress("db1:2434", 7687)).isEqualTo("db1:7687");
  }

  @Test
  void deriveBoltAddressReturnsNullForNonPositivePort() {
    assertThat(RaftHAServer.deriveBoltAddress("db1:2434", 0)).isNull();
    assertThat(RaftHAServer.deriveBoltAddress("db1:2434", -1)).isNull();
  }

  @Test
  void deriveBoltAddressHandlesIpv6Literal() {
    assertThat(RaftHAServer.deriveBoltAddress("[::1]:2434", 7687)).isEqualTo("[::1]:7687");
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl ha-raft test -Dtest=RaftHAServerAddressParsingTest#deriveBoltAddressCombinesRaftHostWithBoltPort`
Expected: FAIL - `deriveBoltAddress` does not exist (compile error).

- [ ] **Step 3: Implement RaftHAServer changes**

Add the field beside `httpFallbackWarned` (around line 118):

```java
  private final    Map<RaftPeerId, String> boltAddresses      = new HashMap<>();
  // Logged at most once: warns operators that Bolt routing addresses are derived (not explicitly configured).
  private final    AtomicBoolean           boltFallbackWarned = new AtomicBoolean(false);
```

In the constructor, after `this.httpsAddresses.putAll(parsed.httpsAddresses());` (around line 181):

```java
    this.boltAddresses.putAll(parsed.boltAddresses());
```

Add the static helper next to `deriveHttpAddress` (around line 1270):

```java
  /**
   * Derives a Bolt address (host:boltPort) by combining a peer's Raft host with the given Bolt port.
   * Returns {@code null} when the port is not positive or the host cannot be extracted. Package-private
   * for testing.
   */
  static String deriveBoltAddress(final String raftAddress, final int boltPort) {
    if (boltPort <= 0)
      return null;
    final String host = extractHost(raftAddress);
    return host != null ? host + ":" + boltPort : null;
  }
```

Add the resolver mirroring `resolveHttpAddress` (place near `resolveHttpAddress`, around line 1200):

```java
  /**
   * Resolves the client-reachable Bolt address (host:boltPort) of a peer. Returns the address declared
   * with the object-form {@code bolt:} field when present; otherwise derives it from the peer's Raft host
   * plus this node's local Bolt port. The fallback is correct only for homogeneous deployments where every
   * node listens on the same Bolt port (e.g. a Kubernetes StatefulSet); a one-time WARNING is logged so
   * operators declare explicit Bolt ports for heterogeneous clusters. Returns {@code null} when the peer is
   * unknown or the local Bolt port is unavailable.
   */
  private String resolveBoltAddress(final RaftPeerId peerId) {
    if (peerId == null)
      return null;
    final String configured = boltAddresses.get(peerId);
    if (configured != null)
      return configured;
    final int localBoltPort = configuration.getValueAsInteger(GlobalConfiguration.BOLT_PORT);
    final String derived = deriveBoltAddress(peerRaftAddress(peerId), localBoltPort);
    if (derived != null && boltFallbackWarned.compareAndSet(false, true))
      LogManager.instance().log(this, Level.WARNING,
          "HA Bolt routing addresses are not configured in '%s': deriving peer Bolt endpoints from each peer's Raft host plus this node's Bolt port (%d). "
              + "This is correct only when every node listens on the same Bolt port (e.g. a Kubernetes StatefulSet). For clusters with heterogeneous "
              + "Bolt ports, declare them explicitly using the 'host:{raft:..,bolt:..}' object syntax in %s.",
          GlobalConfiguration.HA_SERVER_LIST.getKey(), localBoltPort, GlobalConfiguration.HA_SERVER_LIST.getKey());
    return derived;
  }
```

Add the two public methods (near `getLeaderHttpAddress` / `getReplicaAddresses`):

```java
  public String getLeaderBoltAddress() {
    return resolveBoltAddress(getLeaderId());
  }

  public String getReplicaBoltAddresses() {
    final RaftPeerId leaderId = getLeaderId();
    final RaftPeerId excludeId = leaderId != null ? leaderId : localPeerId;
    final StringBuilder sb = new StringBuilder();
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (!peer.getId().equals(excludeId)) {
        final String boltAddr = resolveBoltAddress(peer.getId());
        if (boltAddr != null) {
          if (!sb.isEmpty())
            sb.append(",");
          sb.append(boltAddr);
        }
      }
    }
    return sb.toString();
  }
```

Confirm `GlobalConfiguration` is already imported in `RaftHAServer` (it is, used elsewhere).

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl ha-raft test -Dtest=RaftHAServerAddressParsingTest`
Expected: PASS.

- [ ] **Step 5: Compile the module**

Run: `mvn -q -pl ha-raft -am -DskipTests install`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java \
        ha-raft/src/test/java/com/arcadedb/server/ha/raft/RaftHAServerAddressParsingTest.java
git commit -m "feat(#5002): resolve per-peer Bolt addresses with homogeneous fallback"
```

---

### Task 3: Expose Bolt addresses on HAServerPlugin

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/HAServerPlugin.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`

**Interfaces:**
- Consumes: `RaftHAServer.getLeaderBoltAddress()`, `getReplicaBoltAddresses()` (Task 2).
- Produces: `HAServerPlugin.getLeaderBoltAddress()` (default `null`), `HAServerPlugin.getReplicaBoltAddresses()` (default `""`), consumed by `BoltNetworkExecutor` (Task 4).

- [ ] **Step 1: Add default methods to the interface**

In `HAServerPlugin.java`, after `getReplicaAddresses()` (around line 112):

```java
  /**
   * Returns the client-reachable Bolt address (host:port) of the current leader, or null when unknown
   * or HA is not active. Used to build the writer entry of the Bolt ROUTE routing table.
   */
  default String getLeaderBoltAddress() {
    return null;
  }

  /**
   * Returns a comma-separated list of client-reachable Bolt addresses for the non-leader replicas,
   * or an empty string when none. Used to build the reader entries of the Bolt ROUTE routing table.
   */
  default String getReplicaBoltAddresses() {
    return "";
  }
```

- [ ] **Step 2: Override in RaftHAPlugin**

In `RaftHAPlugin.java`, after the `getReplicaAddresses()` override (around line 262):

```java
  @Override
  public String getLeaderBoltAddress() {
    return raftHAServer != null ? raftHAServer.getLeaderBoltAddress() : null;
  }

  @Override
  public String getReplicaBoltAddresses() {
    return raftHAServer != null ? raftHAServer.getReplicaBoltAddresses() : "";
  }
```

- [ ] **Step 3: Compile server + ha-raft**

Run: `mvn -q -pl server,ha-raft -am -DskipTests install`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/HAServerPlugin.java \
        ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java
git commit -m "feat(#5002): expose leader/replica Bolt addresses on HAServerPlugin"
```

---

### Task 4: Build the HA-aware routing table in handleRoute

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java` (`handleRoute`, around line 927)
- Test: `bolt/src/test/java/com/arcadedb/bolt/BoltProtocolIT.java`

**Interfaces:**
- Consumes: `server.getHA()` returning `HAServerPlugin` with `getLeaderBoltAddress()` / `getReplicaBoltAddresses()` (Task 3).
- Produces: ROUTE `SUCCESS` whose `rt.servers` lists leader as `WRITE`+`ROUTE` and each replica as `READ`+`ROUTE`, or the single-node self table when HA is absent/leaderless.

- [ ] **Step 1: Write the single-node ROUTE regression test** (append to `BoltProtocolIT`, reuse the raw-socket pattern already in this file)

```java
  @Test
  void routeTableSingleNodeReturnsSelfForAllRoles() throws Exception {
    try (Socket socket = new Socket("localhost", 7687)) {
      final OutputStream rawOut = socket.getOutputStream();

      final ByteBuffer handshake = ByteBuffer.allocate(20);
      handshake.put((byte) 0x60).put((byte) 0x60).put((byte) 0xB0).put((byte) 0x17);
      handshake.putInt(0x00020404);
      handshake.putInt(0x00000004);
      handshake.putInt(0x00000003);
      handshake.putInt(0x00000000);
      handshake.flip();
      rawOut.write(handshake.array());
      rawOut.flush();

      final DataInputStream rawIn = new DataInputStream(socket.getInputStream());
      final byte[] negotiatedVersion = new byte[4];
      rawIn.readFully(negotiatedVersion);

      final BoltChunkedOutput chunkedOut = new BoltChunkedOutput(rawOut);
      final BoltChunkedInput chunkedIn = new BoltChunkedInput(socket.getInputStream());

      // HELLO
      PackStreamWriter hello = new PackStreamWriter();
      hello.writeStructureHeader(BoltMessage.HELLO, 1);
      hello.writeMap(Map.of("user_agent", "route-regression/1.0", "scheme", "basic",
          "principal", "root", "credentials", DEFAULT_PASSWORD_FOR_TESTS));
      chunkedOut.writeMessage(hello.toByteArray());
      chunkedIn.readMessage(); // HELLO SUCCESS

      // ROUTE (routing map, bookmarks, extra{db})
      PackStreamWriter route = new PackStreamWriter();
      route.writeStructureHeader(BoltMessage.ROUTE, 3);
      route.writeMap(Map.of());
      route.writeList(List.of());
      route.writeMap(Map.of("db", getDatabaseName()));
      chunkedOut.writeMessage(route.toByteArray());

      final byte[] response = chunkedIn.readMessage();
      assertThat(response[1]).as("ROUTE must succeed").isEqualTo(BoltMessage.SUCCESS);

      final Map<String, Object> body = new PackStreamReader(response).readSuccessMetadata();
      @SuppressWarnings("unchecked")
      final Map<String, Object> rt = (Map<String, Object>) body.get("rt");
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> servers = (List<Map<String, Object>>) rt.get("servers");

      // Single-node: the same self address appears under WRITE, READ and ROUTE.
      final String self = addressForRole(servers, "WRITE");
      assertThat(self).isNotBlank();
      assertThat(addressForRole(servers, "READ")).isEqualTo(self);
      assertThat(addressForRole(servers, "ROUTE")).isEqualTo(self);
    }
  }

  @SuppressWarnings("unchecked")
  private static String addressForRole(final List<Map<String, Object>> servers, final String role) {
    for (final Map<String, Object> s : servers) {
      if (role.equals(s.get("role"))) {
        final List<String> addrs = (List<String>) s.get("addresses");
        return addrs.isEmpty() ? null : addrs.get(0);
      }
    }
    return null;
  }
```

Add any missing imports to `BoltProtocolIT`: `com.arcadedb.bolt.packstream.PackStreamReader`, `com.arcadedb.bolt.packstream.PackStreamWriter`, `com.arcadedb.bolt.message.BoltMessage`, `com.arcadedb.bolt.BoltChunkedInput`, `com.arcadedb.bolt.BoltChunkedOutput`, `java.util.List`. First confirm `PackStreamReader.readSuccessMetadata()` exists; if the accessor differs, use the actual method name (run `rg -n "readSuccessMetadata|public Map.*read.*[Mm]etadata|Map<String, Object> read" bolt/src/main/java/com/arcadedb/bolt/packstream/PackStreamReader.java`). If no map-returning reader exists, parse via the existing reader entrypoint used elsewhere in `BoltProtocolIT` for SUCCESS bodies.

- [ ] **Step 2: Run the regression test against current (single-node) code**

Run: `mvn -q -pl bolt test -Dtest=BoltProtocolIT#routeTableSingleNodeReturnsSelfForAllRoles`
Expected: PASS - current `handleRoute` already returns self for all roles. This test guards the fallback so the Task-4 change must not regress it.

- [ ] **Step 3: Rewrite `handleRoute` with the HA-aware branch**

Replace the body of `handleRoute` (keep the FAILED guard and the response send) with:

```java
  private void handleRoute(final RouteMessage message) throws IOException {
    if (state == State.FAILED) {
      sendIgnored();
      return;
    }

    final Map<String, Object> rt = new LinkedHashMap<>();
    rt.put("ttl", GlobalConfiguration.BOLT_ROUTING_TTL.getValueAsLong());
    rt.put("db", message.getDatabase() != null ? message.getDatabase() : databaseName);

    final List<Map<String, Object>> servers = new ArrayList<>();

    final HAServerPlugin ha = server.getHA();
    final String leaderBolt = ha != null ? ha.getLeaderBoltAddress() : null;

    if (leaderBolt != null) {
      // HA cluster: leader is the writer + router, followers are readers + routers.
      final List<String> readers = new ArrayList<>();
      final String replicaCsv = ha.getReplicaBoltAddresses();
      if (replicaCsv != null && !replicaCsv.isEmpty())
        for (final String addr : replicaCsv.split(","))
          if (!addr.isBlank())
            readers.add(addr.trim());

      final List<String> routers = new ArrayList<>();
      routers.add(leaderBolt);
      routers.addAll(readers);

      servers.add(roleEntry(List.of(leaderBolt), "WRITE"));
      servers.add(roleEntry(readers.isEmpty() ? List.of(leaderBolt) : readers, "READ"));
      servers.add(roleEntry(routers, "ROUTE"));
    } else {
      // Single-node (or HA not ready): advertise this node for every role.
      final String address = getBoltAddress(GlobalConfiguration.BOLT_PORT.getValueAsInteger());
      servers.add(roleEntry(List.of(address), "WRITE"));
      servers.add(roleEntry(List.of(address), "READ"));
      servers.add(roleEntry(List.of(address), "ROUTE"));
    }

    rt.put("servers", servers);
    sendSuccess(CollectionUtils.singletonMap("rt", rt));
  }

  private static Map<String, Object> roleEntry(final List<String> addresses, final String role) {
    final Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("addresses", addresses);
    entry.put("role", role);
    return entry;
  }
```

Add the import `com.arcadedb.server.HAServerPlugin` if not already present. Confirm `server` field type exposes `getHA()` returning `HAServerPlugin` (`rg -n "public HAServerPlugin getHA" server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`).

- [ ] **Step 4: Run the single-node regression again**

Run: `mvn -q -pl bolt test -Dtest=BoltProtocolIT#routeTableSingleNodeReturnsSelfForAllRoles`
Expected: PASS - fallback path unchanged in behavior (still self for all roles).

- [ ] **Step 5: Run the existing routing test to confirm no regression**

Run: `mvn -q -pl bolt test -Dtest=BoltProtocolIT#routingConnection`
Expected: PASS.

- [ ] **Step 6: Compile the bolt module**

Run: `mvn -q -pl bolt -am -DskipTests install`
Expected: BUILD SUCCESS.

- [ ] **Step 7: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltProtocolIT.java
git commit -m "feat(#5002): build HA-aware Bolt ROUTE table from live cluster membership"
```

---

### Task 5: Multi-node cluster IT (CONN-004 acceptance)

**Files:**
- Create: `bolt/src/test/java/com/arcadedb/bolt/Bolt5002RoutingTableIT.java`

**Interfaces:**
- Consumes: `BaseRaftHATest` harness (`findLeaderIndex()`, `getServerDatabase(int,String)`, `waitForAllServers()`, `getServerCount()`, `getDatabaseName()`, `DEFAULT_PASSWORD_FOR_TESTS`, `restartServer`/`stopServer` as available); the full Task 1-4 chain.
- Produces: the acceptance test proving CONN-004.

- [ ] **Step 1: Write the IT**

```java
package com.arcadedb.bolt;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.io.DataInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Certifies that the Bolt ROUTE response reflects real HA topology: the true leader is advertised as
 * the writer and the followers as readers, and a neo4j:// driver can route reads and writes accordingly.
 */
@Tag("slow")
class Bolt5002RoutingTableIT extends BaseRaftHATest {

  private static final int    BASE_BOLT_PORT = 57697;
  private static final String VERTEX_TYPE    = "Bolt5002Route";

  private Driver driver;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    // Object form so each node declares its own Bolt port (nodes share localhost, differ by port).
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:{raft:").append(2434 + i)
        .append(",http:").append(2480 + i)
        .append(",bolt:").append(BASE_BOLT_PORT + i).append("}");
    }
    return sb.toString();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));
    config.setValue(GlobalConfiguration.SERVER_PLUGINS.getKey(), "Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    config.setValue(GlobalConfiguration.BOLT_PORT.getKey(), String.valueOf(BASE_BOLT_PORT + index));
  }

  @AfterEach
  void closeDriver() {
    if (driver != null) {
      driver.close();
      driver = null;
    }
  }

  @Test
  void routeTableReflectsLeaderAndFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final String leaderBolt = "localhost:" + (BASE_BOLT_PORT + leaderIndex);
    final List<String> followerBolt = new ArrayList<>();
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex)
        followerBolt.add("localhost:" + (BASE_BOLT_PORT + i));

    // Ask any node (a follower, to prove routing is not leader-local) for its routing table.
    final int askIndex = (leaderIndex + 1) % getServerCount();
    final Map<String, Object> rt = fetchRoutingTable(BASE_BOLT_PORT + askIndex);

    assertThat(addressesForRole(rt, "WRITE")).containsExactly(leaderBolt);
    assertThat(addressesForRole(rt, "READ")).containsExactlyInAnyOrderElementsOf(followerBolt);
    assertThat(addressesForRole(rt, "ROUTE")).contains(leaderBolt).containsAll(followerBolt);
  }

  @Test
  void neo4jSchemeRoutesReadsAndWrites() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    driver = GraphDatabase.driver(
        "neo4j://localhost:" + (BASE_BOLT_PORT + leaderIndex),
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
    driver.verifyConnectivity();

    try (Session session = driver.session(SessionConfig.builder().withDatabase(getDatabaseName()).build())) {
      session.executeWrite(tx -> {
        tx.run("CREATE (n:" + VERTEX_TYPE + " {name: 'routed'})").consume();
        return null;
      });
    }
    waitForAllServers();

    try (Session session = driver.session(SessionConfig.builder().withDatabase(getDatabaseName()).build())) {
      final long count = session.executeRead(tx ->
          tx.run("MATCH (n:" + VERTEX_TYPE + ") RETURN count(n) AS c").single().get("c").asLong());
      assertThat(count).isEqualTo(1L);
    }
  }

  @Test
  void writerClassificationTracksLeaderChange() throws Exception {
    final int firstLeader = findLeaderIndex();
    assertThat(firstLeader).isGreaterThanOrEqualTo(0);

    // Stop the current leader and wait for a new election.
    stopServer(firstLeader);
    int newLeader = -1;
    for (int attempt = 0; attempt < 60 && newLeader < 0; attempt++) {
      final int candidate = findLeaderIndex();
      if (candidate >= 0 && candidate != firstLeader)
        newLeader = candidate;
      else
        Thread.sleep(500);
    }
    assertThat(newLeader).as("A new leader must be elected after the old one stops").isGreaterThanOrEqualTo(0);

    final int askIndex = newLeader; // ask a surviving node
    final Map<String, Object> rt = fetchRoutingTable(BASE_BOLT_PORT + askIndex);
    assertThat(addressesForRole(rt, "WRITE"))
        .containsExactly("localhost:" + (BASE_BOLT_PORT + newLeader));
  }

  // --- raw Bolt ROUTE helper -------------------------------------------------

  private Map<String, Object> fetchRoutingTable(final int boltPort) throws Exception {
    try (Socket socket = new Socket("localhost", boltPort)) {
      final OutputStream rawOut = socket.getOutputStream();
      final ByteBuffer handshake = ByteBuffer.allocate(20);
      handshake.put((byte) 0x60).put((byte) 0x60).put((byte) 0xB0).put((byte) 0x17);
      handshake.putInt(0x00020404);
      handshake.putInt(0x00000004);
      handshake.putInt(0x00000003);
      handshake.putInt(0x00000000);
      handshake.flip();
      rawOut.write(handshake.array());
      rawOut.flush();

      final DataInputStream rawIn = new DataInputStream(socket.getInputStream());
      final byte[] negotiated = new byte[4];
      rawIn.readFully(negotiated);

      final BoltChunkedOutput out = new BoltChunkedOutput(rawOut);
      final BoltChunkedInput in = new BoltChunkedInput(socket.getInputStream());

      final PackStreamWriter hello = new PackStreamWriter();
      hello.writeStructureHeader(BoltMessage.HELLO, 1);
      hello.writeMap(Map.of("user_agent", "bolt5002/1.0", "scheme", "basic",
          "principal", "root", "credentials", DEFAULT_PASSWORD_FOR_TESTS));
      out.writeMessage(hello.toByteArray());
      in.readMessage();

      final PackStreamWriter route = new PackStreamWriter();
      route.writeStructureHeader(BoltMessage.ROUTE, 3);
      route.writeMap(Map.of());
      route.writeList(List.of());
      route.writeMap(Map.of("db", getDatabaseName()));
      out.writeMessage(route.toByteArray());

      final byte[] response = in.readMessage();
      assertThat(response[1]).as("ROUTE must succeed").isEqualTo(BoltMessage.SUCCESS);
      final Map<String, Object> body = new PackStreamReader(response).readSuccessMetadata();
      @SuppressWarnings("unchecked")
      final Map<String, Object> rt = (Map<String, Object>) body.get("rt");
      return rt;
    }
  }

  @SuppressWarnings("unchecked")
  private static List<String> addressesForRole(final Map<String, Object> rt, final String role) {
    final List<Map<String, Object>> servers = (List<Map<String, Object>>) rt.get("servers");
    for (final Map<String, Object> s : servers)
      if (role.equals(s.get("role")))
        return (List<String>) s.get("addresses");
    return List.of();
  }
}
```

- [ ] **Step 2: Reconcile harness method names**

Confirm the exact `BaseRaftHATest`/`BaseGraphServerTest` API the test uses. Run:

```bash
rg -n "protected .*findLeaderIndex|void stopServer|void restartServer|getServerDatabase|waitForAllServers|DEFAULT_PASSWORD_FOR_TESTS|getServerAddresses" \
  ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseRaftHATest.java \
  server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java
```

Adjust the test to the real signatures: if `stopServer(int)` is absent, use the actual stop/kill API (e.g. `getServer(i).stop()` plus the harness's server-stop bookkeeping) exactly as other HA leader-change ITs do - locate one with `rg -ln "new leader|stopServer|getServer\(.*\)\.stop" ha-raft/src/test`. Also verify `PackStreamReader.readSuccessMetadata()` (Task 4, Step 1); use the real accessor name if different.

- [ ] **Step 3: Run the IT**

Run: `mvn -q -pl bolt test -Dtest=Bolt5002RoutingTableIT`
Expected: PASS (all three methods). Cluster ITs are slow; allow several minutes.

- [ ] **Step 4: Commit**

```bash
git add bolt/src/test/java/com/arcadedb/bolt/Bolt5002RoutingTableIT.java
git commit -m "test(#5002): certify HA-aware Bolt ROUTE against a 3-node cluster"
```

---

### Task 6: Flip CONN-004 to passing in the conformance spec

**Files:**
- Modify: `bolt/conformance/spec.yaml` (`CONN-004`, around line 99-115)

**Interfaces:**
- Consumes: nothing. Produces: authoritative conformance status.

- [ ] **Step 1: Update the CONN-004 entry**

Change `current_status: expected-fail` to `current_status: passing`. Remove the `known_limitation:` block and the `tracking_issue: "#4890"` line (both no longer apply). If a residual limitation must be recorded, replace `known_limitation` with a short note that heterogeneous Bolt ports require the `host:{raft:..,bolt:..}` object syntax in `HA_SERVER_LIST`; otherwise remove it entirely. Keep the `steps`, `description`, and `applicable_driver_versions` fields intact.

- [ ] **Step 2: Sanity-check the YAML**

Run: `rg -n "CONN-004" -A15 bolt/conformance/spec.yaml`
Expected: shows `current_status: passing` and no dangling `known_limitation`/`tracking_issue` keys under CONN-004.

- [ ] **Step 3: Commit**

```bash
git add bolt/conformance/spec.yaml
git commit -m "docs(#5002): mark CONN-004 passing in the Bolt conformance spec"
```

---

### Task 7: Full verification and cross-module regression

**Files:** none (verification only).

- [ ] **Step 1: Compile the whole reactor for touched modules**

Run: `mvn -q -pl engine,server,ha-raft,bolt -am -DskipTests install`
Expected: BUILD SUCCESS.

- [ ] **Step 2: Run the focused regression set**

Run:
```bash
mvn -q -pl ha-raft test -Dtest=RaftHAServerAddressParsingTest
mvn -q -pl bolt test -Dtest=BoltProtocolIT#routingConnection+routeTableSingleNodeReturnsSelfForAllRoles
mvn -q -pl bolt test -Dtest=BoltFollowerForwardingIT
mvn -q -pl bolt test -Dtest=Bolt5002RoutingTableIT
```
Expected: all PASS.

- [ ] **Step 3: Confirm no stray debug output or fully-qualified names were introduced**

Run: `git diff main --stat && rg -n "System.out" $(git diff --name-only main -- '*.java')`
Expected: no `System.out` in changed files.

- [ ] **Step 4: Final commit if any cleanup was needed**

```bash
git add -A && git commit -m "chore(#5002): verification cleanup" || echo "nothing to clean up"
```

---

## Self-Review notes

- Spec coverage: Address resolution (Task 1-2), interface bridge (Task 3), ROUTE handler + fallback (Task 4), multi-node acceptance incl. leader-change (Task 5), single-node regression (Task 4 Step 1), conformance flip (Task 6), whole-reactor verification (Task 7). All spec sections mapped.
- Homogeneous fallback + one-time WARNING mirrors the established `resolveHttpAddress` pattern (spec constraint).
- `bolt:` is object-form only; positional form untouched (Task 1 tests assert this).
- Type consistency: `getLeaderBoltAddress()`/`getReplicaBoltAddresses()` names identical across RaftHAServer, HAServerPlugin, RaftHAPlugin, and BoltNetworkExecutor. `roleEntry`/`addressesForRole`/`fetchRoutingTable` helper names consistent within their files.
- Open reconciliations flagged inline for the implementer: `PackStreamReader.readSuccessMetadata()` accessor name (Task 4/5) and the harness stop-server API (Task 5 Step 2). These are verify-then-adapt steps, not placeholders.
