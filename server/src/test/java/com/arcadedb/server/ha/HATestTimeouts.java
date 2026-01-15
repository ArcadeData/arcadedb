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

import java.time.Duration;

/**
 * Common timeout constants for High Availability integration tests.
 *
 * <p>These timeouts are carefully chosen to balance test reliability with reasonable execution times.
 * They are based on typical cluster behavior under various conditions including network latency,
 * server restart times, and replication propagation delays.
 *
 * <p>Timeout rationale:
 * <ul>
 *   <li><b>Schema operations:</b> Typically complete within 100ms in a healthy cluster
 *   <li><b>Cluster consensus:</b> Includes leader election and replica consensus
 *   <li><b>Server lifecycle:</b> Accounts for graceful shutdown and startup procedures
 *   <li><b>Replication queue drain:</b> Ensures all pending operations are delivered
 *   <li><b>Chaos testing:</b> Extended timeouts for environments with server crashes
 * </ul>
 *
 * @see HARandomCrashIT for chaos testing patterns
 * @see HASplitBrainIT for split-brain simulation patterns
 * @see ReplicationChangeSchemaIT for schema propagation patterns
 */
public interface HATestTimeouts {
  /**
   * Timeout for schema propagation operations (type, property, bucket creation).
   *
   * <p>Schema operations are typically fast (&lt;100ms) in a healthy cluster. This timeout
   * allows for network latency and ensures all replicas have the schema change before proceeding.
   */
  Duration SCHEMA_PROPAGATION_TIMEOUT = Duration.ofSeconds(10);

  /**
   * Timeout for cluster-wide consensus operations (leader election, stabilization).
   *
   * <p>Cluster stabilization after a network partition or quorum loss requires time for:
   * <ul>
   *   <li>Servers to detect the partition (heartbeat timeout)
   *   <li>Leader election to occur
   *   <li>Replicas to commit new leader
   * </ul>
   *
   * <p>Increased to 120s to accommodate connection retry logic with exponential backoff
   * (Phase 3 Priority 1: Connection Resilience).
   */
  Duration CLUSTER_STABILIZATION_TIMEOUT = Duration.ofSeconds(120);

  /**
   * Timeout for server shutdown operations.
   *
   * <p>Includes graceful shutdown with connection draining and resource cleanup.
   * Typically completes within 5-10 seconds in normal conditions.
   */
  Duration SERVER_SHUTDOWN_TIMEOUT = Duration.ofSeconds(90);

  /**
   * Timeout for server startup operations.
   *
   * <p>Includes initialization of storage, index loading, and HA cluster joining.
   * Typically completes within 5-10 seconds depending on database size.
   */
  Duration SERVER_STARTUP_TIMEOUT = Duration.ofSeconds(90);

  /**
   * Timeout for replication queue draining.
   *
   * <p>Ensures all pending replication messages have been delivered and processed.
   * Includes network I/O and database persistence operations.
   */
  Duration REPLICATION_QUEUE_DRAIN_TIMEOUT = Duration.ofSeconds(30);

  /**
   * Timeout for replica reconnection after network partition or restart.
   *
   * <p>Includes detection of network availability and re-synchronization with leader.
   * Extended to account for potential backoff delays.
   *
   * <p>Increased to 60s to accommodate connection retry logic with exponential backoff
   * (Phase 3 Priority 1: Connection Resilience).
   */
  Duration REPLICA_RECONNECTION_TIMEOUT = Duration.ofSeconds(60);

  /**
   * Timeout for transaction execution during chaos testing.
   *
   * <p>Extended timeout to account for:
   * <ul>
   *   <li>Random server crashes during transaction execution
   *   <li>Quorum recovery delays
   *   <li>Replica reconnection and resync
   *   <li>Exponential backoff delays between retries
   * </ul>
   *
   * <p>This is significantly longer than normal transaction timeouts to allow for
   * graceful recovery from chaos scenarios.
   */
  Duration CHAOS_TRANSACTION_TIMEOUT = Duration.ofSeconds(300);

  /**
   * Poll interval for Awaitility conditions (how frequently to check).
   *
   * <p>Balances responsiveness with CPU usage. Higher values reduce polling overhead,
   * but may increase time to detect condition completion.
   */
  Duration AWAITILITY_POLL_INTERVAL = Duration.ofSeconds(1);

  /**
   * Poll interval for long-running operations.
   *
   * <p>Used for operations that typically take several seconds or more, where frequent
   * polling would be wasteful.
   */
  Duration AWAITILITY_POLL_INTERVAL_LONG = Duration.ofSeconds(1);
}
