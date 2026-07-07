# Bolt: HA-aware ROUTE response with multi-server routing table (#5002)

- Issue: https://github.com/ArcadeData/arcadedb/issues/5002
- Tracking: #4890 (Group C protocol/type-fidelity gaps)
- Epic: #4882 (Bolt Driver Compatibility Certification)
- Conformance scenario: `CONN-004` in `bolt/conformance/spec.yaml` (currently `expected-fail`)

## Problem

`BoltNetworkExecutor.handleRoute` returns **this node's own address** as the `WRITE`,
`READ`, and `ROUTE` server, regardless of cluster topology. Against a real HA cluster a
`neo4j://` driver therefore cannot discover the leader/followers or route reads versus
writes: routing/discovery is unproven, and the ROUTE table lies about the topology.

The `neo4j://` driver does not merely read the routing table; it **connects** to the
advertised addresses and routes subsequent work to them. Any address the ROUTE table
returns must be genuinely reachable on the Bolt port.

## Constraints discovered

- The `bolt` module depends on `arcadedb-server` in `provided` scope and on `arcadedb-ha-raft`
  in `test` scope only. Production Bolt code can reach the cluster **only** through the
  `HAServerPlugin` interface (package `com.arcadedb.server`). It cannot reference any ha-raft
  class at compile time.
- `HAServerPlugin` today exposes `isLeader()`, `getLeaderName()`, `getLeaderAddress()` and
  `getReplicaAddresses()` - but the two address methods return **HTTP** addresses
  (`host:httpPort`), not Bolt addresses.
- HA membership (`HA_SERVER_LIST`) records each peer's host plus Raft/HTTP/HTTPS ports and a
  leader-election priority. It records **no Bolt port**. `RaftPeerAddressResolver` already
  supports an extensible **object form** `host:{raft:2434,http:2480,https:2490,priority:10}`
  and a positional colon form (max 5 fields), plus an optional `name@` prefix.
- `RaftHAServer.resolveHttpAddress` already establishes the pattern: use the explicitly
  declared `host:port` if present, otherwise derive `peerHost:localPort` (homogeneous-cluster
  assumption) and log a throttled one-time WARNING telling operators to declare ports
  explicitly for heterogeneous clusters.
- Test harness `BaseRaftHATest` runs an in-process N-node cluster on `localhost` with distinct
  ports per node. `BoltFollowerForwardingIT` already drives a 3-node cluster over Bolt using
  `BASE_BOLT_PORT + index`. This means the "peer host + my own Bolt port" shortcut is wrong in
  the test harness (all nodes share `localhost`, differ only by port) - the per-node Bolt port
  must be knowable.

## Design

### 1. Address resolution (ha-raft module)

**`RaftPeerAddressResolver`**
- Extend the object-form peer spec to accept an optional `bolt:<port>` field:
  `host:{raft:2434,http:2480,bolt:7687}`.
- `PeerSpec` gains a nullable `boltPort`; `parseObjectForm` reads `bolt:`.
- `ParsedPeerList` gains `Map<RaftPeerId,String> boltAddresses`, populated only when `bolt:`
  is declared, keyed by `RaftPeerId` exactly like `httpAddresses`/`httpsAddresses`.
- The positional colon form is left at its current 5-field maximum. A 6th positional field
  would be unreadable; `bolt:` is **object-form only** and documented as such in the Javadoc.

**`RaftHAServer`**
- Store `boltAddresses` from the parsed list (new final `Map<RaftPeerId,String>`).
- `resolveBoltAddress(RaftPeerId)` mirrors `resolveHttpAddress`: return the declared
  `host:boltPort` if present; otherwise derive `peerRaftHost:localBoltPort` where
  `localBoltPort = configuration.getValueAsInteger(GlobalConfiguration.BOLT_PORT)`, logging a
  throttled one-time WARNING (same contract as HTTP). Returns `null` when the peer is unknown.
- `getLeaderBoltAddress()` returns the leader's resolved Bolt address (or `null`).
- `getReplicaBoltAddresses()` returns a comma-separated list of resolved Bolt addresses for
  every peer except the leader (or this node when the leader is unknown), skipping any that
  resolve to `null` - an exact parallel of `getReplicaAddresses()`.

### 2. Interface (server module)

**`HAServerPlugin`** gains a single snapshot method with a safe default so non-Raft or absent
implementations need no change:
```java
record BoltRoutingTable(String writer, List<String> readers) {}
default BoltRoutingTable getBoltRoutingTable() { return null; }
```
**`RaftHAPlugin`** overrides it, delegating to `RaftHAServer`, which derives the writer and readers from
a single `getLeaderId()` read and returns `readers` as an immutable `List.copyOf`.

> Implementation note (post-review): an earlier draft exposed two methods
> (`getLeaderBoltAddress()` + `getReplicaBoltAddresses()`). Code review flagged that reading the leader
> twice opens a window where the writer and reader sets can disagree about who the leader is, so the
> final design collapses them into one snapshot method.

### 3. ROUTE handler (bolt module)

**`BoltNetworkExecutor.handleRoute`**: ROUTE enumerates every peer's Bolt endpoint, so it is gated
behind an authenticated session (`state == READY`), matching the other request handlers - a ROUTE sent
before HELLO/LOGON completes is refused. When authenticated, obtain `server.getHA()` and its
`getBoltRoutingTable()` snapshot. When the snapshot is non-null (HA active with a known leader), build the
routing table:
- leader (writer) Bolt address -> `WRITE` and `ROUTE`
- each replica (reader) Bolt address -> `READ` and `ROUTE`
- `ttl` from `BOLT_ROUTING_TTL`, `db` from the message/session (unchanged).

When the snapshot is null, fall back by HA state:
- **HA active but no leader known yet** (mid-election): advertise this node as `READ` + `ROUTE` only -
  never `WRITE`, since it may be a follower - so a driver keeps reading and re-routes after the TTL
  instead of writing to a follower.
- **No HA** (true single-node): advertise this node as `WRITE`/`READ`/`ROUTE` (unchanged).

The self address uses the connection's actual bound Bolt port (`socket.getLocalPort()`), not the global
default, so a non-default listener port is advertised correctly.

Because the table is rebuilt on every ROUTE call and the leader/replica split is taken from
live membership, **reader/writer classification tracks leader changes automatically**: after
the TTL expires (or on a connection failure) the driver re-issues ROUTE and receives the new
topology.

Roles are emitted as three separate server entries (`WRITE`, `READ`, `ROUTE`) consistent with
the existing single-node shape and the Neo4j routing-table contract. When a node holds two
roles (leader is both writer and router) its address appears in both the `WRITE` and `ROUTE`
entries.

### 4. Tests

- **`Bolt5002RoutingTableIT extends BaseRaftHATest`** (`@Tag("slow")`), `getServerCount() == 3`.
  - Override `getServerAddresses()` to emit the object form with a per-node `bolt:<port>`
    (e.g. `localhost:{raft:2434,http:2480,bolt:57687}`), and `onServerConfiguration` to bind
    those same Bolt ports (mirrors `BoltFollowerForwardingIT`).
  - Discover the true leader via `findLeaderIndex()`.
  - Connect a `neo4j://localhost:<anyNodeBoltPort>` driver and request the routing table
    (drive `getServerAddresses()`/`SHOW ...` via the driver, or assert on a routed session):
    assert the `WRITE` entry is the true leader's Bolt address and the `READ` entries are the
    two followers' Bolt addresses.
  - Run a routed **write** and a routed **read** through the `neo4j://` session to prove the
    advertised addresses are actually reachable and routable end to end.
  - Leader-change check: stop the current leader, wait for re-election, re-request the routing
    table, assert the new leader is now classified `WRITE`.
- **`RaftPeerAddressResolver` unit test**: `bolt:` present in object form populates
  `boltAddresses`; absent leaves it empty; object + positional entries mix correctly; `name@`
  prefix still works alongside `bolt:`.
- **Single-node regression**: assert non-HA ROUTE is unchanged (self as `WRITE`/`READ`/`ROUTE`).
  Extend or add alongside the existing ROUTE coverage in `BoltProtocolIT`.

### 5. Conformance spec

Flip `CONN-004` in `bolt/conformance/spec.yaml` from `current_status: expected-fail` to the
passing status used by the other closed Group C gaps, and remove the `known_limitation` block
(or restate any residual limitation - e.g. `bolt:` must be declared for heterogeneous Bolt
ports - as an accepted, documented note rather than a failure).

## Scope guardrails (YAGNI)

- No changes to the positional colon syntax (no 6th positional field).
- No new standalone advertised-address config key; no gossip/registry.
- No Bolt-side load balancing beyond writer/reader/router classification.
- `neo4j+s` routing over TLS reuses the existing `BoltSslHelper`; no new TLS work.
- No change to the advertised server identity (`Neo4j/5.26.0 compatible ...`).

## Verification

- `mvn -q -pl ha-raft -am test -Dtest=RaftPeerAddressResolver*` for the parser unit test.
- `mvn -q -pl bolt -am test -Dtest=Bolt5002RoutingTableIT` for the cluster IT (`slow` tag,
  run explicitly).
- Re-run `BoltFollowerForwardingIT` and `BoltProtocolIT` ROUTE coverage to confirm no
  regression in existing Bolt behavior.
- Compile the whole reactor (`mvn -q -DskipTests install` on the touched modules) before
  declaring done.

## Acceptance criteria (from #5002)

- [ ] `CONN-004` passes against a multi-node cluster: a `neo4j://` driver discovers writer +
  reader(s) and routes accordingly (scenario re-enabled).
- [ ] Single-node (non-HA) ROUTE behavior unchanged.
- [ ] Reader/writer classification tracks leader changes.
