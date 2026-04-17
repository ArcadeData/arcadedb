# Remove Old HA Stack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the legacy custom HA implementation from the `server` module, leaving the Raft-based `ha-raft` module as the single HA stack.

**Architecture:** Create a thin `HAServerPlugin` interface in `com.arcadedb.server` that extracts the public API consumed by HTTP handlers, MCP tools, and test utilities. Move `HAReplicatedDatabase` to the same package. Update `RaftHAPlugin` to implement the new interface and set itself as `ArcadeDBServer.haServer`. Then delete all old HA code and legacy-only configuration entries.

**Tech Stack:** Java 21, Maven, Apache Ratis 3.2.2

**Spec:** `docs/superpowers/specs/2026-04-07-remove-old-ha-stack-design.md`

---

### Task 1: Create HAServerPlugin Interface

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/HAServerPlugin.java`

This interface captures the public HA API consumed outside the old HA implementation. It extends `ServerPlugin` and includes the three enums currently nested in `HAServer`.

- [ ] **Step 1: Create the interface file**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server;

import java.util.Map;

/**
 * Public interface for the High Availability server plugin. Consumed by HTTP handlers,
 * MCP tools, backup tasks, and test utilities. The single production implementation
 * is {@code RaftHAPlugin} in the ha-raft module.
 */
public interface HAServerPlugin extends ServerPlugin {

  enum QUORUM {
    NONE, ONE, TWO, THREE, MAJORITY, ALL;

    public int quorum(final int numberOfServers) {
      return switch (this) {
        case NONE -> 0;
        case ONE -> 1;
        case TWO -> 2;
        case THREE -> 3;
        case MAJORITY -> numberOfServers / 2 + 1;
        case ALL -> numberOfServers;
      };
    }
  }

  enum ELECTION_STATUS {
    DONE, VOTING_FOR_ME, VOTING_FOR_OTHERS, LEADER_WAITING_FOR_QUORUM
  }

  enum SERVER_ROLE {
    ANY, REPLICA
  }

  boolean isLeader();

  String getLeaderName();

  ELECTION_STATUS getElectionStatus();

  String getClusterName();

  Map<String, Object> getStats();

  int getConfiguredServers();

  /**
   * Returns the HTTP address (host:port) of the current leader, or null if unknown.
   */
  String getLeaderAddress();

  /**
   * Returns a comma-separated list of replica HTTP addresses, or empty string if none.
   */
  String getReplicaAddresses();

  /**
   * Sends a shutdown command to a remote server in the cluster.
   */
  void shutdownRemoteServer(String serverName);

  /**
   * Disconnects this node from the cluster (closes Raft server and client).
   */
  void disconnectCluster();
}
```

- [ ] **Step 2: Verify compilation**

Run:
```bash
cd server && mvn compile -q
```
Expected: BUILD SUCCESS (no consumers yet, just the interface)

- [ ] **Step 3: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/HAServerPlugin.java
git commit -m "feat(ha): create HAServerPlugin interface for HA abstraction

Extract the public HA API (enums QUORUM, ELECTION_STATUS, SERVER_ROLE and
key methods) into a stable interface in com.arcadedb.server. This will
replace the concrete HAServer class as the return type of getHA().

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Move HAReplicatedDatabase to Server Package

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/HAReplicatedDatabase.java`
- Delete: `server/src/main/java/com/arcadedb/server/ha/HAReplicatedDatabase.java`

- [ ] **Step 1: Create the new file with updated package and import**

```java
/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server;

/**
 * Common interface for replicated database wrappers. Allows server-layer code to interact
 * with the replicated database abstraction without knowing which concrete HA implementation
 * is active.
 */
public interface HAReplicatedDatabase {

  /**
   * Propagates the (just-created) database to all replicas. Must be called on the leader after
   * creating a new database so that replicas are made aware of it before any transactions arrive.
   */
  void createInReplicas();

  /**
   * Returns the write quorum configured for this database.
   */
  HAServerPlugin.QUORUM getQuorum();

  /**
   * Returns {@code true} if the local node is currently the HA leader.
   */
  boolean isLeader();

  /**
   * Returns the HTTP address (host:port) of the current leader, or {@code null} if the leader is
   * unknown or its HTTP address is not configured.
   */
  String getLeaderHttpAddress();
}
```

- [ ] **Step 2: Delete the old file**

```bash
rm server/src/main/java/com/arcadedb/server/ha/HAReplicatedDatabase.java
```

- [ ] **Step 3: Update import in PostCommandHandler.java**

In `server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java`, change:
```java
import com.arcadedb.server.ha.HAReplicatedDatabase;
```
to:
```java
import com.arcadedb.server.HAReplicatedDatabase;
```

- [ ] **Step 4: Update imports in PostServerCommandHandler.java**

In `server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java`, change:
```java
import com.arcadedb.server.ha.HAReplicatedDatabase;
```
to:
```java
import com.arcadedb.server.HAReplicatedDatabase;
```

- [ ] **Step 5: Update import in GetServerHandler.java**

In `server/src/main/java/com/arcadedb/server/http/handler/GetServerHandler.java`, change:
```java
import com.arcadedb.server.ha.HAReplicatedDatabase;
```
to:
```java
import com.arcadedb.server.HAReplicatedDatabase;
```

- [ ] **Step 6: Update import in RaftReplicatedDatabase.java**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`, change:
```java
import com.arcadedb.server.ha.HAReplicatedDatabase;
```
to:
```java
import com.arcadedb.server.HAReplicatedDatabase;
```

- [ ] **Step 7: Update QUORUM reference in RaftReplicatedDatabase.java**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java`, change the import:
```java
import com.arcadedb.server.ha.HAServer;
```
to:
```java
import com.arcadedb.server.HAServerPlugin;
```

And change the `getQuorum()` method (around line 880):
```java
  public HAServer.QUORUM getQuorum() {
    // Raft consensus is inherently majority-based
    return HAServer.QUORUM.MAJORITY;
  }
```
to:
```java
  public HAServerPlugin.QUORUM getQuorum() {
    // Raft consensus is inherently majority-based
    return HAServerPlugin.QUORUM.MAJORITY;
  }
```

- [ ] **Step 8: Verify compilation**

Run:
```bash
mvn compile -pl server,ha-raft -q
```
Expected: BUILD SUCCESS

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor(ha): move HAReplicatedDatabase to com.arcadedb.server package

Relocate the shared interface out of the ha/ package (which will be
deleted) into the server package. Update all consumer imports.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Update RaftHAPlugin to Implement HAServerPlugin

**Files:**
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`

The `RaftHAPlugin` becomes the implementation of `HAServerPlugin`. It delegates to `RaftHAServer` for Raft-specific operations. We also need to add a few methods to `RaftHAServer` that don't exist yet (`getClusterName`, `getStats`, `getConfiguredServers`, `getElectionStatus`).

- [ ] **Step 1: Add missing methods to RaftHAServer**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`:

Add a `clusterName` field. In the constructor (around line 92), after:
```java
    final String clusterName = configuration.getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME);
```
add:
```java
    this.clusterName = clusterName;
```

Add the field declaration near the other fields (after line 78):
```java
  private final String                     clusterName;
```

Add these methods at the end of the class (before the closing brace):

```java
  public String getClusterName() {
    return clusterName;
  }

  public int getConfiguredServers() {
    return raftGroup.getPeers().size();
  }

  public String getLeaderName() {
    final RaftPeerId leaderId = getLeaderId();
    if (leaderId == null)
      return null;
    final String display = peerDisplayNames.get(leaderId);
    return display != null ? display : leaderId.toString();
  }

  public Map<String, Object> getStats() {
    final Map<String, Object> stats = new HashMap<>();
    stats.put("localPeerId", localPeerId.toString());
    stats.put("isLeader", isLeader());
    stats.put("configuredServers", getConfiguredServers());

    if (clusterMonitor != null) {
      final Map<String, Long> lags = clusterMonitor.getReplicaLags();
      if (!lags.isEmpty())
        stats.put("replicaLags", lags);
    }

    final List<Map<String, String>> replicas = new ArrayList<>();
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (!peer.getId().equals(localPeerId)) {
        final Map<String, String> replicaInfo = new HashMap<>();
        replicaInfo.put("id", peer.getId().toString());
        replicaInfo.put("address", peer.getAddress().toString());
        final String httpAddr = httpAddresses.get(peer.getId());
        if (httpAddr != null)
          replicaInfo.put("httpAddress", httpAddr);
        replicas.add(replicaInfo);
      }
    }
    stats.put("replicas", replicas);
    return stats;
  }

  public String getReplicaAddresses() {
    final StringBuilder sb = new StringBuilder();
    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (!peer.getId().equals(localPeerId)) {
        final String httpAddr = httpAddresses.get(peer.getId());
        if (httpAddr != null) {
          if (!sb.isEmpty())
            sb.append(",");
          sb.append(httpAddr);
        }
      }
    }
    return sb.toString();
  }
```

Add necessary imports at the top:
```java
import java.util.List;
import java.util.ArrayList;
```
(Check if `List` and `ArrayList` are already imported; add only if missing.)

- [ ] **Step 2: Update RaftHAPlugin to implement HAServerPlugin**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`:

Change the import:
```java
import com.arcadedb.server.ServerPlugin;
```
to:
```java
import com.arcadedb.server.HAServerPlugin;
```

Change the class declaration:
```java
public class RaftHAPlugin implements ServerPlugin {
```
to:
```java
public class RaftHAPlugin implements HAServerPlugin {
```

Remove the existing `isLeader()` method (lines 108-110), and replace it plus add all the interface methods. Add these methods before the `isRaftEnabled()` method:

```java
  @Override
  public boolean isLeader() {
    return raftHAServer != null && raftHAServer.isLeader();
  }

  @Override
  public String getLeaderName() {
    return raftHAServer != null ? raftHAServer.getLeaderName() : null;
  }

  @Override
  public HAServerPlugin.ELECTION_STATUS getElectionStatus() {
    // Raft handles elections internally; report DONE when a leader exists
    if (raftHAServer == null)
      return ELECTION_STATUS.DONE;
    return raftHAServer.getLeaderId() != null ? ELECTION_STATUS.DONE : ELECTION_STATUS.VOTING_FOR_ME;
  }

  @Override
  public String getClusterName() {
    return raftHAServer != null ? raftHAServer.getClusterName() : null;
  }

  @Override
  public java.util.Map<String, Object> getStats() {
    return raftHAServer != null ? raftHAServer.getStats() : java.util.Collections.emptyMap();
  }

  @Override
  public int getConfiguredServers() {
    return raftHAServer != null ? raftHAServer.getConfiguredServers() : 1;
  }

  @Override
  public String getLeaderAddress() {
    return raftHAServer != null ? raftHAServer.getLeaderHttpAddress() : null;
  }

  @Override
  public String getReplicaAddresses() {
    return raftHAServer != null ? raftHAServer.getReplicaAddresses() : "";
  }

  @Override
  public void shutdownRemoteServer(final String serverName) {
    if (raftHAServer == null)
      throw new RuntimeException("Raft HA server not started");

    final String leaderAddr = raftHAServer.getLeaderHttpAddress();
    // Find the target server's HTTP address from the Raft peer list
    String targetAddr = null;
    for (final var peer : raftHAServer.getRaftGroup().getPeers()) {
      final String httpAddr = raftHAServer.getHttpAddresses().get(peer.getId());
      if (httpAddr != null && (peer.getId().toString().contains(serverName) || httpAddr.contains(serverName))) {
        targetAddr = httpAddr;
        break;
      }
    }
    if (targetAddr == null)
      throw new com.arcadedb.server.ServerException("Cannot find server '" + serverName + "' in the cluster");

    // Send shutdown via HTTP POST to the target server
    try {
      final java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
          new java.net.URL("http://" + targetAddr + "/api/v1/server").openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");

      final String token = configuration.getValueAsString(com.arcadedb.GlobalConfiguration.HA_CLUSTER_TOKEN);
      if (token != null && !token.isEmpty())
        conn.setRequestProperty("Authorization", "Bearer " + token);

      conn.getOutputStream().write("{\"command\":\"shutdown\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      conn.getResponseCode(); // trigger the request
      conn.disconnect();
    } catch (final java.io.IOException e) {
      throw new RuntimeException("Failed to shutdown remote server '" + serverName + "'", e);
    }
  }

  @Override
  public void disconnectCluster() {
    if (raftHAServer != null)
      raftHAServer.stop();
  }
```

Also update `isRaftEnabled()` - since `HA_IMPLEMENTATION` will be removed later, the check should just be HA_ENABLED. But for now (while the config entry still exists), keep backward compatibility:

```java
  private boolean isRaftEnabled() {
    if (configuration == null)
      return false;
    if (!configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      return false;
    // Accept both explicit "raft" and the absence of HA_IMPLEMENTATION (default when legacy is removed)
    final String impl = configuration.getValueAsString(GlobalConfiguration.HA_IMPLEMENTATION);
    return impl == null || impl.isEmpty() || "raft".equalsIgnoreCase(impl);
  }
```

- [ ] **Step 3: Expose getRaftGroup() and getHttpAddresses() in RaftHAServer**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java`, add these public accessors if they don't already exist:

```java
  public RaftGroup getRaftGroup() {
    return raftGroup;
  }

  public Map<RaftPeerId, String> getHttpAddresses() {
    return httpAddresses;
  }
```

- [ ] **Step 4: Update RaftHAPlugin.startService() to register as haServer**

In `RaftHAPlugin.startService()`, after the line:
```java
      server.rewrapDatabases();
```
add:
```java
      // Register this plugin as the HA server so ArcadeDBServer.getHA() returns it
      server.setHA(this);
```

This requires adding a `setHA` method to ArcadeDBServer (done in Task 5).

- [ ] **Step 5: Verify compilation of ha-raft module**

Run:
```bash
cd ha-raft && mvn compile -q
```
Expected: May fail until Task 5 adds `setHA()`. That's OK - we will verify after Task 5.

- [ ] **Step 6: Commit**

```bash
git add ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java
git commit -m "feat(ha-raft): implement HAServerPlugin interface in RaftHAPlugin

RaftHAPlugin now implements HAServerPlugin, providing all methods needed
by server HTTP handlers, MCP tools, and test utilities. Added missing
methods to RaftHAServer: getClusterName, getStats, getConfiguredServers,
getLeaderName, getReplicaAddresses. Added shutdownRemoteServer (via HTTP)
and disconnectCluster (closes Raft server).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Update ArcadeDBServer

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`

Replace `HAServer` field/getter with `HAServerPlugin`. Remove old HA creation logic. Add `setHA()` method.

- [ ] **Step 1: Update imports**

Remove:
```java
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
```

No new imports needed - `HAServerPlugin` is in the same package.

- [ ] **Step 2: Update the haServer field (line 92)**

Change:
```java
  private             HAServer                              haServer;
```
to:
```java
  private             HAServerPlugin                        haServer;
```

- [ ] **Step 3: Update getHA() method (lines 456-458)**

Change:
```java
  public HAServer getHA() {
    return haServer;
  }
```
to:
```java
  public HAServerPlugin getHA() {
    return haServer;
  }
```

- [ ] **Step 4: Add setHA() method**

After the `getHA()` method, add:
```java
  public void setHA(final HAServerPlugin ha) {
    this.haServer = ha;
  }
```

- [ ] **Step 5: Remove old HA creation logic (lines 201-209)**

Replace:
```java
    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      final String haImpl = configuration.getValueAsString(GlobalConfiguration.HA_IMPLEMENTATION);
      if ("raft".equalsIgnoreCase(haImpl))
        LogManager.instance().log(this, Level.INFO, "Using Raft HA implementation (loaded via plugin manager)");
      else {
        haServer = new HAServer(this, configuration);
        haServer.startService();
      }
    }
```
with:
```java
    // HA is started via the plugin manager (RaftHAPlugin discovered via ServiceLoader)
```

- [ ] **Step 6: Simplify database wrapping in createDatabase() (lines 417-422)**

Replace:
```java
      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
        if (databaseWrapper != null)
          embeddedDatabase = databaseWrapper.apply((LocalDatabase) embeddedDatabase);
        else if (!"raft".equalsIgnoreCase(configuration.getValueAsString(GlobalConfiguration.HA_IMPLEMENTATION)))
          embeddedDatabase = new ReplicatedDatabase(this, (LocalDatabase) embeddedDatabase);
      }
```
with:
```java
      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED) && databaseWrapper != null)
        embeddedDatabase = databaseWrapper.apply((LocalDatabase) embeddedDatabase);
```

- [ ] **Step 7: Simplify database wrapping in getDatabase() (lines 592-597)**

Replace:
```java
        if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
          if (databaseWrapper != null)
            embDatabase = databaseWrapper.apply((LocalDatabase) embDatabase);
          else if (!"raft".equalsIgnoreCase(configuration.getValueAsString(GlobalConfiguration.HA_IMPLEMENTATION)))
            embDatabase = new ReplicatedDatabase(this, (LocalDatabase) embDatabase);
        }
```
with:
```java
        if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED) && databaseWrapper != null)
          embDatabase = databaseWrapper.apply((LocalDatabase) embDatabase);
```

- [ ] **Step 8: Verify compilation**

Run:
```bash
cd server && mvn compile -q
```
Expected: BUILD SUCCESS

- [ ] **Step 9: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/ArcadeDBServer.java
git commit -m "refactor(server): replace HAServer with HAServerPlugin in ArcadeDBServer

Change haServer field type and getHA() return type to HAServerPlugin.
Add setHA() for plugin registration. Remove old HAServer construction
and legacy ReplicatedDatabase wrapping - HA is now entirely plugin-driven.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Update HTTP Handlers

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java`
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/GetServerHandler.java`

- [ ] **Step 1: Update PostServerCommandHandler imports**

Remove:
```java
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.Leader2ReplicaNetworkExecutor;
import com.arcadedb.server.ha.Replica2LeaderNetworkExecutor;
import com.arcadedb.server.ha.message.ServerShutdownRequest;
```

Add:
```java
import com.arcadedb.server.HAServerPlugin;
```

- [ ] **Step 2: Update PostServerCommandHandler.getHA() helper (around line 880)**

Change:
```java
  private HAServer getHA() {
    final HAServer ha = httpServer.getServer().getHA();
    if (ha == null)
      throw new CommandExecutionException(
          "ArcadeDB is not running with High Availability module enabled. Please add this setting at startup: -Darcadedb.ha.enabled=true");
    return ha;
```
to:
```java
  private HAServerPlugin getHA() {
    final HAServerPlugin ha = httpServer.getServer().getHA();
    if (ha == null)
      throw new CommandExecutionException(
          "ArcadeDB is not running with High Availability module enabled. Please add this setting at startup: -Darcadedb.ha.enabled=true");
    return ha;
```

- [ ] **Step 3: Update PostServerCommandHandler.shutdownServer() (lines 197-205)**

Replace the remote shutdown section:
```java
      final HAServer ha = getHA();
      final Leader2ReplicaNetworkExecutor replica = ha.getReplica(serverName);
      if (replica == null)
        throw new ServerException("Cannot contact server '" + serverName + "' from the current server");

      final Binary buffer = new Binary();
      ha.getMessageFactory().serializeCommand(new ServerShutdownRequest(), buffer, -1);
      replica.sendMessage(buffer);
```
with:
```java
      final HAServerPlugin ha = getHA();
      ha.shutdownRemoteServer(serverName);
```

Remove the `import com.arcadedb.database.Binary;` if it's no longer used elsewhere in the file (check first).

- [ ] **Step 4: Update PostServerCommandHandler.connectCluster() (lines 493-502)**

Replace:
```java
  private boolean connectCluster(final String serverAddress, final HttpServerExchange exchange) {
    final HAServer ha = getHA();

    Metrics.counter("http.connect-cluster").increment();

    return ha.connectToLeader(serverAddress, exception -> {
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send("{ \"error\" : \"" + exception.getMessage() + "\"}");
      return null;
    });
  }
```
with:
```java
  private boolean connectCluster(final String serverAddress, final HttpServerExchange exchange) {
    Metrics.counter("http.connect-cluster").increment();
    // With Raft, cluster membership is managed automatically - this is a no-op
    return true;
  }
```

- [ ] **Step 5: Update PostServerCommandHandler.disconnectCluster() (lines 505-515)**

Replace:
```java
  private void disconnectCluster() {
    Metrics.counter("http.server-disconnect").increment();

    final HAServer ha = getHA();

    final Replica2LeaderNetworkExecutor leader = ha.getLeader();
    if (leader != null)
      leader.close();
    else
      ha.disconnectAllReplicas();
  }
```
with:
```java
  private void disconnectCluster() {
    Metrics.counter("http.server-disconnect").increment();

    final HAServerPlugin ha = getHA();
    ha.disconnectCluster();
  }
```

- [ ] **Step 6: Update PostServerCommandHandler.checkServerIsLeaderIfInHA() (lines 873-878)**

Change:
```java
  private void checkServerIsLeaderIfInHA() {
    final HAServer ha = httpServer.getServer().getHA();
    if (ha != null && !ha.isLeader())
      // NOT THE LEADER
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", ha.getLeaderName());
  }
```
to:
```java
  private void checkServerIsLeaderIfInHA() {
    final HAServerPlugin ha = httpServer.getServer().getHA();
    if (ha != null && !ha.isLeader())
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", ha.getLeaderName());
  }
```

- [ ] **Step 7: Update GetServerHandler imports**

Remove:
```java
import com.arcadedb.server.ha.HAServer;
```

Add:
```java
import com.arcadedb.server.HAServerPlugin;
```

- [ ] **Step 8: Update GetServerHandler.exportCluster() (lines 83-148)**

Replace the entire method:
```java
  private void exportCluster(final HttpServerExchange exchange, final JSONObject response) {
    final HAServerPlugin ha = httpServer.getServer().getHA();
    if (ha != null) {
      final JSONObject haJSON = new JSONObject();
      response.put("ha", haJSON);

      haJSON.put("clusterName", ha.getClusterName());
      haJSON.put("leader", ha.getLeaderName());
      haJSON.put("electionStatus", ha.getElectionStatus().toString());
      haJSON.put("network", ha.getStats());

      if (!ha.isLeader()) {
        // ASK TO THE LEADER THE NETWORK COMPOSITION
        final String leaderAddr = ha.getLeaderAddress();
        if (leaderAddr != null) {
          HttpURLConnection connection;
          try {
            connection = (HttpURLConnection) new URL(
                "http://" + leaderAddr + "/api/v1/server?mode=cluster").openConnection();
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          try {
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Authorization", exchange.getRequestHeaders().get("Authorization").getFirst());
            connection.connect();

            JSONObject leaderResponse = new JSONObject(readResponse(connection));
            final JSONObject network = leaderResponse.getJSONObject("ha").getJSONObject("network");
            haJSON.getJSONObject("network").put("replicas", network.getJSONArray("replicas"));

          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          } finally {
            connection.disconnect();
          }
        }
      }

      final JSONArray databases = new JSONArray();

      for (String dbName : httpServer.getServer().getDatabaseNames()) {
        final ServerDatabase db = httpServer.getServer().getDatabase(dbName);
        final JSONObject databaseJSON = new JSONObject();
        databaseJSON.put("name", db.getName());
        if (db.getWrappedDatabaseInstance() instanceof HAReplicatedDatabase haDb)
          databaseJSON.put("quorum", haDb.getQuorum());
        databases.put(databaseJSON);
      }

      haJSON.put("databases", databases);

      final String leaderServer = ha.isLeader()
          ? httpServer.getServer().getHttpServer().getListeningAddress()
          : ha.getLeaderAddress();
      final String replicaServers = ha.getReplicaAddresses();

      haJSON.put("leaderAddress", leaderServer);
      haJSON.put("replicaAddresses", replicaServers);

      LogManager.instance()
          .log(this, Level.FINE, "Returning configuration leaderServer=%s replicaServers=[%s]", leaderServer, replicaServers);
    }
  }
```

- [ ] **Step 9: Verify compilation**

Run:
```bash
cd server && mvn compile -q
```
Expected: BUILD SUCCESS

- [ ] **Step 10: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/PostServerCommandHandler.java server/src/main/java/com/arcadedb/server/http/handler/GetServerHandler.java
git commit -m "refactor(server): update HTTP handlers to use HAServerPlugin

Replace HAServer, Leader2ReplicaNetworkExecutor, Replica2LeaderNetworkExecutor
references with HAServerPlugin interface methods. Simplify remote shutdown,
connect/disconnect cluster, and cluster status export.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Update BackupTask, ServerStatusTool, and PluginManager

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/backup/BackupTask.java`
- Modify: `server/src/main/java/com/arcadedb/server/mcp/tools/ServerStatusTool.java`
- Modify: `server/src/main/java/com/arcadedb/server/plugin/PluginManager.java`

- [ ] **Step 1: Update BackupTask.java**

Change import:
```java
import com.arcadedb.server.ha.HAServer;
```
to:
```java
import com.arcadedb.server.HAServerPlugin;
```

Update the usage (around line 126):
```java
      final HAServer ha = server.getHA();
```
to:
```java
      final HAServerPlugin ha = server.getHA();
```

- [ ] **Step 2: Update ServerStatusTool.java**

Change import:
```java
import com.arcadedb.server.ha.HAServer;
```
to:
```java
import com.arcadedb.server.HAServerPlugin;
```

Update the usage (around line 63):
```java
    final HAServer ha = server.getHA();
```
to:
```java
    final HAServerPlugin ha = server.getHA();
```

- [ ] **Step 3: Update PluginManager.java**

The `isRaftHAEnabled()` method checks `HA_IMPLEMENTATION`. Since we're removing that config entry and Raft is the only implementation, simplify the method.

Change:
```java
  private boolean isRaftHAEnabled() {
    return configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)
        && "raft".equalsIgnoreCase(configuration.getValueAsString(GlobalConfiguration.HA_IMPLEMENTATION));
  }
```
to:
```java
  private boolean isHAEnabled() {
    return configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED);
  }
```

Update the caller in `discoverPluginsOnMainClassLoader()`:
```java
    final boolean autoDiscoverRaft = isRaftHAEnabled();
```
to:
```java
    final boolean autoDiscoverRaft = isHAEnabled();
```

- [ ] **Step 4: Verify compilation**

Run:
```bash
cd server && mvn compile -q
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/backup/BackupTask.java server/src/main/java/com/arcadedb/server/mcp/tools/ServerStatusTool.java server/src/main/java/com/arcadedb/server/plugin/PluginManager.java
git commit -m "refactor(server): update BackupTask, MCP tool, and PluginManager for HAServerPlugin

Replace HAServer references with HAServerPlugin. Simplify PluginManager
to auto-discover HA plugin whenever HA_ENABLED=true (no HA_IMPLEMENTATION
check needed since Raft is the only implementation).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Update Test Utilities

**Files:**
- Modify: `test-utils/src/main/java/com/arcadedb/test/BaseGraphServerTest.java`
- Modify: `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java`
- Modify: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseRaftHATest.java`

Both `BaseGraphServerTest` files are near-identical copies. They use `HAServer.SERVER_ROLE` and set `HA_SERVER_ROLE` config. Since `HA_SERVER_ROLE` is being removed, we need to remove that config line and update the enum reference.

- [ ] **Step 1: Update test-utils BaseGraphServerTest**

In `test-utils/src/main/java/com/arcadedb/test/BaseGraphServerTest.java`:

Change import:
```java
import com.arcadedb.server.ha.HAServer;
```
to:
```java
import com.arcadedb.server.HAServerPlugin;
```

Change the `getServerRole` method:
```java
  protected HAServer.SERVER_ROLE getServerRole(final int serverIndex) {
    return serverIndex == 0 ? HAServer.SERVER_ROLE.ANY : HAServer.SERVER_ROLE.REPLICA;
  }
```
to:
```java
  protected HAServerPlugin.SERVER_ROLE getServerRole(final int serverIndex) {
    return serverIndex == 0 ? HAServerPlugin.SERVER_ROLE.ANY : HAServerPlugin.SERVER_ROLE.REPLICA;
  }
```

Update all `HAServer.SERVER_ROLE` references in the file to `HAServerPlugin.SERVER_ROLE`.

Remove the line that sets `HA_SERVER_ROLE` config (around line 313):
```java
      config.setValue(GlobalConfiguration.HA_SERVER_ROLE, getServerRole(i));
```
Delete this line entirely (Raft handles roles automatically).

- [ ] **Step 2: Update server BaseGraphServerTest**

In `server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java`:

Apply the exact same changes as Step 1.

- [ ] **Step 3: Update BaseRaftHATest**

In `ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseRaftHATest.java`:

Change import:
```java
import com.arcadedb.server.ha.HAServer;
```
to:
```java
import com.arcadedb.server.HAServerPlugin;
```

Change:
```java
  protected HAServer.SERVER_ROLE getServerRole(final int serverIndex) {
    // With Raft, leader election is automatic; all nodes start as ANY
    return HAServer.SERVER_ROLE.ANY;
  }
```
to:
```java
  protected HAServerPlugin.SERVER_ROLE getServerRole(final int serverIndex) {
    // With Raft, leader election is automatic; all nodes start as ANY
    return HAServerPlugin.SERVER_ROLE.ANY;
  }
```

- [ ] **Step 4: Verify compilation**

Run:
```bash
mvn compile -pl test-utils,server,ha-raft -q
```
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add test-utils/src/main/java/com/arcadedb/test/BaseGraphServerTest.java server/src/test/java/com/arcadedb/server/BaseGraphServerTest.java ha-raft/src/test/java/com/arcadedb/server/ha/raft/BaseRaftHATest.java
git commit -m "refactor(test): update test utilities to use HAServerPlugin

Replace HAServer.SERVER_ROLE with HAServerPlugin.SERVER_ROLE.
Remove HA_SERVER_ROLE config setting (Raft handles roles automatically).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Full Compilation Check Before Deletion

Before deleting the old code, verify the entire project compiles.

- [ ] **Step 1: Compile the full project**

Run:
```bash
mvn compile -DskipTests -q 2>&1 | tail -20
```
Expected: BUILD SUCCESS

If there are remaining references to `com.arcadedb.server.ha.HAServer` or other old HA classes outside the `server/src/main/java/com/arcadedb/server/ha/` directory, fix them before proceeding.

- [ ] **Step 2: Search for any remaining old HA imports outside the ha/ package**

Run a grep to confirm no files outside the old HA directory still import old HA classes:
```bash
grep -r "import com.arcadedb.server.ha\." --include="*.java" . | grep -v "server/src/main/java/com/arcadedb/server/ha/" | grep -v "server/src/test/java/com/arcadedb/server/ha/" | grep -v "/ha/raft/"
```
Expected: No output (all old HA imports have been updated)

- [ ] **Step 3: Commit if any fixes were needed**

Only commit if Step 1 or 2 revealed issues that needed fixing.

---

### Task 9: Delete Old HA Implementation

**Files:**
- Delete: all files in `server/src/main/java/com/arcadedb/server/ha/`
- Delete: all files in `server/src/test/java/com/arcadedb/server/ha/`

- [ ] **Step 1: Delete old HA main classes**

```bash
rm -rf server/src/main/java/com/arcadedb/server/ha/
```

- [ ] **Step 2: Delete old HA test classes**

```bash
rm -rf server/src/test/java/com/arcadedb/server/ha/
```

- [ ] **Step 3: Verify compilation**

Run:
```bash
mvn compile -DskipTests -q 2>&1 | tail -20
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(ha): remove legacy HA implementation

Delete 39 main classes and 27 test classes from the old custom HA stack
in server/src/main/java/com/arcadedb/server/ha/. The Raft-based
implementation in ha-raft/ is now the single HA stack.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Clean Up GlobalConfiguration

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`

Remove legacy-only config entries. Keep entries used by ha-raft or other modules.

- [ ] **Step 1: Remove legacy-only config entries**

Remove the following enum constants from `GlobalConfiguration.java`:

- `HA_IMPLEMENTATION` (around line 569)
- `HA_SERVER_ROLE` (around line 511)
- `HA_REPLICATION_QUEUE_SIZE` (around line 544)
- `HA_REPLICATION_FILE_MAXSIZE` (around line 548)
- `HA_REPLICATION_INCOMING_HOST` (around line 554)
- `HA_REPLICATION_INCOMING_PORTS` (around line 558)

**Keep** these (used by ha-raft or network module):
- `HA_ENABLED`, `HA_CLUSTER_NAME`, `HA_SERVER_LIST`
- `HA_QUORUM`, `HA_QUORUM_TIMEOUT`
- `HA_ERROR_RETRIES` (used by `network/RemoteHttpComponent`)
- `HA_ELECTION_TIMEOUT_MIN`, `HA_ELECTION_TIMEOUT_MAX` (used by ha-raft `RaftHAServer`)
- `HA_LOG_SEGMENT_SIZE`, `HA_APPEND_BUFFER_SIZE` (used by ha-raft `RaftHAServer`)
- `HA_REPLICATION_CHUNK_MAXSIZE` (used by `network/ChannelBinaryClient` and `ChannelBinaryServer`)
- `HA_K8S`, `HA_K8S_DNS_SUFFIX` (used by `ArcadeDBServer` for hostname resolution)
- All `HA_RAFT_*` entries, `HA_REPLICATION_LAG_WARNING`, `HA_LOG_VERBOSE`, `HA_CLUSTER_TOKEN`

- [ ] **Step 2: Remove RaftHAPlugin's HA_IMPLEMENTATION check**

In `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAPlugin.java`, update `isRaftEnabled()` since `HA_IMPLEMENTATION` no longer exists:

```java
  private boolean isRaftEnabled() {
    return configuration != null
        && configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED);
  }
```

Also update the javadoc/comment on the class to remove `HA_IMPLEMENTATION=raft` references.

- [ ] **Step 3: Verify compilation**

Run:
```bash
mvn compile -DskipTests -q 2>&1 | tail -20
```
Expected: BUILD SUCCESS

- [ ] **Step 4: Search for any remaining references to removed config entries**

```bash
grep -rn "HA_IMPLEMENTATION\|HA_SERVER_ROLE\|HA_REPLICATION_QUEUE_SIZE\|HA_REPLICATION_FILE_MAXSIZE\|HA_REPLICATION_INCOMING_HOST\|HA_REPLICATION_INCOMING_PORTS" --include="*.java" . | grep -v "/target/"
```
Expected: No output (all references removed)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore(config): remove legacy-only HA configuration entries

Remove HA_IMPLEMENTATION, HA_SERVER_ROLE, HA_REPLICATION_QUEUE_SIZE,
HA_REPLICATION_FILE_MAXSIZE, HA_REPLICATION_INCOMING_HOST, and
HA_REPLICATION_INCOMING_PORTS from GlobalConfiguration. These were
only used by the deleted legacy HA stack.

Keep entries used by ha-raft (election timeouts, log segment size,
append buffer) and network module (chunk max size, error retries, K8s).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Verify Build and Run Tests

- [ ] **Step 1: Full project compilation**

Run:
```bash
mvn clean install -DskipTests 2>&1 | tail -30
```
Expected: BUILD SUCCESS for all modules

- [ ] **Step 2: Run ha-raft unit tests**

Run:
```bash
cd ha-raft && mvn test -Dtest="*Test" -DfailIfNoTests=false 2>&1 | tail -30
```
Expected: All unit tests PASS

- [ ] **Step 3: Run server unit tests (non-HA)**

Run:
```bash
cd server && mvn test -Dtest="!com.arcadedb.server.ha.**" -DfailIfNoTests=false 2>&1 | tail -30
```
Expected: All non-HA server tests PASS

- [ ] **Step 4: Run a basic ha-raft integration test**

Run:
```bash
cd ha-raft && mvn test -Dtest="RaftReplication3NodesIT" -DfailIfNoTests=false 2>&1 | tail -30
```
Expected: PASS

- [ ] **Step 5: Commit if any fixes were needed**

Only commit if test failures revealed issues that needed fixing.
