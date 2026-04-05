# E2E HA Testing with TestContainers + Toxiproxy for Raft

## Context

The `feature/2043-ha-test` branch contains an `e2e-ha` module that uses TestContainers with Toxiproxy to test ArcadeDB's HA cluster under realistic network fault conditions. That module was written against the legacy HA protocol. This design covers porting it to the `ha-redesign` branch to test the new Raft-based HA implementation exclusively.

## Goals

- Test the Raft HA implementation under realistic network conditions (latency, partitions, packet loss)
- Validate leader election, failover, split-brain handling, and data consistency using actual Docker containers
- Provide a reproducible, CI-friendly test suite that exercises the full server stack

## Non-goals

- Testing the legacy HA implementation
- Performance benchmarks (deferred until scenario tests stabilize)
- Custom Docker image builds (existing image already includes `ha-raft`)

## Architecture

### Module structure

```
e2e-ha/                          # New module, opt-in via Maven profile
  pom.xml
  src/test/java/com/arcadedb/containers/ha/
    SimpleHaScenarioIT.java      # 2-node basic replication
    ThreeInstancesScenarioIT.java # 3-node replication + consistency
    LeaderFailoverIT.java         # Leader kill + re-election
    NetworkDelayIT.java           # Latency/jitter injection
    PacketLossIT.java             # Packet loss simulation
    NetworkPartitionIT.java       # Quorum under partition
    NetworkPartitionRecoveryIT.java # Resync after partition heals
    SplitBrainIT.java            # Minority leader steps down
    RollingRestartIT.java         # Zero-downtime rolling restart
  src/test/resources/
    logback-test.xml
```

### Base infrastructure (in `load-tests/`)

Reuse and adapt three classes from `load-tests/` module as a test-jar dependency:

- **ContainersTestTemplate** - Cluster lifecycle, Toxiproxy setup, container creation
- **DatabaseWrapper** - HTTP/gRPC client for schema creation, data insertion, record counting
- **ServerWrapper** - Container host/port accessor record

### Dependencies

```xml
<!-- e2e-ha/pom.xml key dependencies -->
<dependency>
  <groupId>com.arcadedb</groupId>
  <artifactId>arcadedb-e2e-perf</artifactId>
  <type>test-jar</type>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>testcontainers</artifactId>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>toxiproxy</artifactId>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>junit-jupiter</artifactId>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.awaitility</groupId>
  <artifactId>awaitility</artifactId>
  <scope>test</scope>
</dependency>
```

## Key Adaptations from Legacy

### 1. Container configuration (ContainersTestTemplate)

Legacy:
```java
-Darcadedb.ha.enabled=true
-Darcadedb.ha.quorum=majority
-Darcadedb.ha.serverRole=any
-Darcadedb.ha.serverList={server1}server1:2424,{server2}server2:2424
-Darcadedb.ha.replicationQueueSize=1024
```

Raft:
```java
-Darcadedb.ha.enabled=true
-Darcadedb.ha.implementation=raft
-Darcadedb.ha.quorum=majority
-Darcadedb.ha.raft.port=2434
-Darcadedb.ha.serverList=server1:2434:2480,server2:2434:2480,server3:2434:2480
```

Changes:
- Add `ha.implementation=raft`
- Server list format: `host:raftPort:httpPort` (no curly-brace names)
- Remove `ha.serverRole` (Raft elects automatically)
- Remove `ha.replicationQueueSize` (Raft manages its own log)
- Expose port **2434** (Raft gRPC consensus) in container config

### 2. Toxiproxy target port

Legacy proxied port 2424 (binary protocol). Raft must proxy port **2434** — that's where consensus traffic flows. This is the critical path for partition/latency tests.

Port 2480 (HTTP) should also be proxied for tests that simulate client-facing network issues, but the primary fault injection target is 2434.

### 3. Leader detection — new cluster status endpoint

**Prerequisite**: Add a lightweight HTTP endpoint to expose Raft cluster status.

**Endpoint**: `GET /api/v1/cluster`

**Response**:
```json
{
  "implementation": "raft",
  "clusterName": "arcadedb",
  "localPeerId": "peer-0",
  "isLeader": true,
  "leaderId": "peer-0",
  "leaderHttpAddress": "server1:2480",
  "peers": [
    { "id": "peer-0", "address": "server1:2434:2480", "role": "LEADER" },
    { "id": "peer-1", "address": "server2:2434:2480", "role": "FOLLOWER" },
    { "id": "peer-2", "address": "server3:2434:2480", "role": "FOLLOWER" }
  ]
}
```

**Implementation**: Register this endpoint in `RaftHAPlugin` during server startup. Uses `RaftHAServer.isLeader()`, `getLeaderId()`, and Ratis `RaftServer.getDivision().getInfo()` for peer roles.

**Why not reuse `GetServerHandler?mode=cluster`**: That handler is tightly coupled to legacy `HAServer`. A dedicated endpoint is cleaner and avoids forcing Raft into the legacy interface shape.

## Test-by-test porting notes

### Direct ports (logic unchanged, config only)

| Test | What it does | Adaptation |
|------|-------------|------------|
| `SimpleHaScenarioIT` | 2-node replication | Raft config, use `/api/v1/cluster` for status |
| `ThreeInstancesScenarioIT` | 3-node replication + consistency | Raft config |
| `NetworkDelayIT` | Latency/jitter injection | Toxiproxy on port 2434 |
| `PacketLossIT` | Packet loss simulation | Toxiproxy on port 2434 |

### Require semantic adjustments

| Test | What changes |
|------|-------------|
| `LeaderFailoverIT` | Leader detection via `/api/v1/cluster`. Raft leader election is automatic; no explicit role assignment needed. |
| `NetworkPartitionIT` | Proxy port 2434. Assertions must account for Raft semantics: leader in minority partition steps down automatically (no stale-leader writes). |
| `NetworkPartitionRecoveryIT` | Proxy port 2434. Rejoining node catches up via Raft log replay, not legacy resync protocol. May need adjusted timeouts. |
| `SplitBrainIT` | Raft guarantees single leader by design. Test verifies old leader steps down in minority partition, majority elects new leader. Simpler than legacy split-brain. |
| `RollingRestartIT` | Raft peer list is static; restarted nodes rejoin automatically. Needs timeouts for Raft log catch-up after restart. |

### Deferred (performance benchmarks)

- `ElectionTimeBenchmarkIT` — Port after scenario tests are stable
- `FailoverTimeBenchmarkIT` — Port after scenario tests are stable
- `ReplicationThroughputBenchmarkIT` — Port after scenario tests are stable

## Implementation order

1. **Cluster status endpoint** (`/api/v1/cluster`) in `RaftHAPlugin`
2. **Adapt `ContainersTestTemplate`** in `load-tests/` for Raft config (keep backward-compatible or parameterized)
3. **Create `e2e-ha/` module** with pom.xml and logback-test.xml
4. **Port simple tests first**: `SimpleHaScenarioIT`, `ThreeInstancesScenarioIT`
5. **Port fault injection tests**: `NetworkDelayIT`, `PacketLossIT`
6. **Port partition/failover tests**: `LeaderFailoverIT`, `NetworkPartitionIT`, `NetworkPartitionRecoveryIT`, `SplitBrainIT`
7. **Port `RollingRestartIT`** last (most complex lifecycle management)

## Decisions

- **CI frequency**: Nightly only. Too resource-heavy for every PR (3 Docker containers + Toxiproxy, 2-3GB heap each).
- **Legacy support**: Strip legacy HA config from `ContainersTestTemplate` entirely on this branch. Raft-only.

## Implementation status

- [x] Cluster status endpoint (`GET /api/v1/cluster`) in `RaftHAPlugin`
- [x] `ContainersTestTemplate` adapted for Raft-only config in `load-tests/`
- [x] `e2e-ha/` module created with pom.xml, logback-test.xml, registered in root pom
- [x] `load-tests/pom.xml` exports test-jar for e2e-ha dependency
- [x] `SimpleHaScenarioIT` ported
- [x] `ThreeInstancesScenarioIT` ported
- [x] `NetworkDelayIT` ported
- [x] `PacketLossIT` ported
- [x] `LeaderFailoverIT` ported
- [x] `NetworkPartitionIT` ported
- [x] `NetworkPartitionRecoveryIT` ported
- [x] `SplitBrainIT` ported
- [x] `RollingRestartIT` ported
