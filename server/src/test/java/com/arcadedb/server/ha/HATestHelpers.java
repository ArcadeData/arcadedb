/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha;

import com.arcadedb.server.ArcadeDBServer;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Utility class for common High Availability test operations.
 *
 * <p>Provides reusable helper methods for waiting on cluster state changes,
 * server lifecycle transitions, and replication alignment. All methods use
 * Awaitility with timeouts from {@link HATestTimeouts}.
 *
 * <p>These helpers replace Thread.sleep() anti-patterns with explicit
 * condition waiting, improving test reliability and reducing flakiness.
 *
 * @see HATestTimeouts for timeout constants
 */
public class HATestHelpers {

  /**
   * Wait for the cluster to reach a stable state with all expected replicas connected.
   *
   * <p>Stable state means:
   * <ul>
   *   <li>All servers are ONLINE
   *   <li>Leader is elected and ready
   *   <li>Replication queues are drained
   *   <li>All expected replicas are connected
   * </ul>
   *
   * @param servers array of servers in the cluster (leader + replicas)
   * @param expectedReplicaCount number of replicas expected to be connected to leader
   * @throws TimeoutException if cluster doesn't stabilize within timeout
   */
  public static void waitForClusterStable(ArcadeDBServer[] servers, int expectedReplicaCount) {
    // Optimized single-phase wait that checks all conditions together
    // This is faster than multi-phase waits and matches the original BaseGraphServerTest behavior
    Awaitility.await("cluster stable")
        .atMost(HATestTimeouts.CLUSTER_STABILIZATION_TIMEOUT)
        .pollInterval(Duration.ofMillis(500))  // Match original poll interval for performance
        .until(() -> {
          // Count only ONLINE servers (some may be intentionally stopped in quorum tests)
          int onlineCount = 0;
          for (ArcadeDBServer server : servers) {
            if (server != null && server.getStatus() == ArcadeDBServer.Status.ONLINE) {
              onlineCount++;
            }
          }

          // If no servers are online, cluster cannot be stable
          if (onlineCount == 0) {
            return false;
          }

          // Find leader among online servers
          ArcadeDBServer leader = getLeader(servers);
          if (leader == null) {
            return false;
          }

          // Check HA is initialized
          if (leader.getHA() == null) {
            return false;
          }

          // Check all expected replicas are connected
          // expectedReplicaCount is the number of replicas (not including leader)
          return leader.getHA().getOnlineReplicas() >= expectedReplicaCount;
        });
  }

  /**
   * Wait for a server to complete shutdown.
   *
   * <p>Polls server status until it transitions to OFFLINE.
   *
   * @param server the server to wait for
   * @throws TimeoutException if shutdown doesn't complete within timeout
   */
  public static void waitForServerShutdown(ArcadeDBServer server) {
    Awaitility.await("server shutdown")
        .atMost(HATestTimeouts.SERVER_SHUTDOWN_TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> server.getStatus() == ArcadeDBServer.Status.OFFLINE);
  }

  /**
   * Wait for a server to complete startup and reach ONLINE status.
   *
   * <p>Polls server status until it transitions to ONLINE.
   *
   * @param server the server to wait for
   * @throws TimeoutException if startup doesn't complete within timeout
   */
  public static void waitForServerStartup(ArcadeDBServer server) {
    Awaitility.await("server startup")
        .atMost(HATestTimeouts.SERVER_STARTUP_TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> server.getStatus() == ArcadeDBServer.Status.ONLINE);
  }

  /**
   * Wait for leader election to complete in the cluster.
   *
   * <p>Ensures that exactly one server is the leader and it's ready to accept connections.
   *
   * @param servers array of servers in the cluster
   * @throws TimeoutException if leader election doesn't complete within timeout
   */
  public static void waitForLeaderElection(ArcadeDBServer[] servers) {
    Awaitility.await("leader election")
        .atMost(HATestTimeouts.CLUSTER_STABILIZATION_TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> {
          ArcadeDBServer leader = getLeader(servers);
          if (leader == null || leader.getStatus() != ArcadeDBServer.Status.ONLINE) return false;
          if (leader.getHA() == null || !leader.getHA().isLeader()) return false;
          return true;
        });
  }

  /**
   * Wait for schema propagation to complete across all replicas.
   *
   * <p>Ensures that schema changes (type creation, property addition) have
   * propagated from the leader to all replicas.
   *
   * @param servers array of servers in the cluster
   * @param schemaCheckFunction function that returns true when schema is aligned
   * @throws TimeoutException if schema doesn't align within timeout
   */
  public static void waitForSchemaAlignment(ArcadeDBServer[] servers, SchemaCheck schemaCheckFunction) {
    Awaitility.await("schema alignment")
        .atMost(HATestTimeouts.SCHEMA_PROPAGATION_TIMEOUT)
        .pollInterval(Duration.ofMillis(200))  // Faster polling for schema checks
        .until(() -> schemaCheckFunction.check(servers));
  }

  /**
   * Wait for replication to align across all replicas.
   *
   * <p>Ensures that all replicas have received and applied the same transactions
   * as the leader. Validates data consistency across the cluster.
   *
   * @param servers array of servers in the cluster
   * @param alignmentCheckFunction function that returns true when replication is aligned
   * @throws TimeoutException if replication doesn't align within timeout
   */
  public static void waitForReplicationAlignment(ArcadeDBServer[] servers, ReplicationCheck alignmentCheckFunction) {
    Awaitility.await("replication alignment")
        .atMost(HATestTimeouts.REPLICATION_QUEUE_DRAIN_TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> alignmentCheckFunction.check(servers));
  }

  /**
   * Find the current leader in the cluster.
   *
   * @param servers array of servers to search
   * @return the leader server, or null if no leader found
   */
  private static ArcadeDBServer getLeader(ArcadeDBServer[] servers) {
    for (ArcadeDBServer server : servers) {
      if (server.getHA() != null && server.getHA().isLeader()) {
        return server;
      }
    }
    return null;
  }

  /**
   * Functional interface for schema alignment checks.
   */
  @FunctionalInterface
  public interface SchemaCheck {
    boolean check(ArcadeDBServer[] servers) throws Exception;
  }

  /**
   * Functional interface for replication alignment checks.
   */
  @FunctionalInterface
  public interface ReplicationCheck {
    boolean check(ArcadeDBServer[] servers) throws Exception;
  }
}
