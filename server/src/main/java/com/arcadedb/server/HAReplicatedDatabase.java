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

import com.arcadedb.database.Database;

/**
 * Common interface for replicated database wrappers. Allows server-layer code to interact
 * with the replicated database abstraction without knowing which concrete HA implementation
 * is active.
 */
public interface HAReplicatedDatabase {

  void createInReplicas();

  /**
   * Replicates a database drop operation. Raft-first: the leader does NOT drop
   * its own files until the state machine applies the drop entry across the cluster.
   * Non-HA implementations should throw {@link UnsupportedOperationException}.
   */
  void dropInReplicas();

  /**
   * Replicates a createDatabase operation with an optional forceSnapshot flag.
   * When {@code forceSnapshot} is true, replicas pull a fresh snapshot from the leader
   * even if the database already exists locally (used after a restore).
   */
  default void createInReplicas(final boolean forceSnapshot) {
    if (forceSnapshot)
      throw new UnsupportedOperationException("forceSnapshot variant not supported by this HA implementation");
    createInReplicas();
  }

  HAServerPlugin.QUORUM getQuorum();

  boolean isLeader();

  String getLeaderHttpAddress();

  /**
   * Sets the read consistency context for the current thread before a query.
   * Implementations use this to wait for replication to catch up before reading.
   */
  default void setReadConsistencyContext(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
    // No-op by default; overridden by HA implementations that support read consistency
  }

  /**
   * Clears the read consistency context after a query completes.
   */
  default void clearReadConsistencyContext() {
    // No-op by default
  }

  /**
   * Returns the last applied Raft index, or -1 if not available.
   * Used to set the commit index bookmark header in write responses.
   */
  default long getLastAppliedIndex() {
    return -1;
  }
}
