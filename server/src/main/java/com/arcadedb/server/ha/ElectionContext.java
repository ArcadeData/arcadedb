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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Captures a snapshot of the cluster state at the beginning of an election.
 *
 * <p>This immutable context prevents race conditions where cluster membership
 * might change during an election cycle. By capturing the state upfront,
 * the election proceeds with a consistent view of the cluster.</p>
 *
 * <p>Using this context ensures:</p>
 * <ul>
 *   <li>Consistent quorum calculations throughout the election</li>
 *   <li>Stable list of servers to contact for votes</li>
 *   <li>No surprises from concurrent cluster membership changes</li>
 * </ul>
 */
public record ElectionContext(
    /**
     * Snapshot of known servers at election start.
     * This set is unmodifiable to prevent accidental changes.
     */
    Set<HAServer.ServerInfo> servers,

    /**
     * Number of configured servers for quorum calculation.
     * Captured at election start to ensure consistent majority calculation.
     */
    int configuredServerCount,

    /**
     * Last replication message number at election start.
     * Used to determine which server has the most up-to-date data.
     */
    long lastReplicationMessageNumber,

    /**
     * Election turn number.
     * Monotonically increasing to distinguish election rounds.
     */
    long electionTurn,

    /**
     * Timestamp when this election context was created.
     * Useful for debugging and election timeout logic.
     */
    long createdAt
) {

  /**
   * Creates a new ElectionContext with the current timestamp.
   *
   * @param servers                     The set of known servers
   * @param configuredServerCount       The configured server count for quorum
   * @param lastReplicationMessageNumber The last replication message number
   * @param electionTurn                The election turn number
   * @return A new immutable ElectionContext
   */
  public static ElectionContext create(
      final Set<HAServer.ServerInfo> servers,
      final int configuredServerCount,
      final long lastReplicationMessageNumber,
      final long electionTurn) {
    // Create an unmodifiable copy to ensure immutability
    final Set<HAServer.ServerInfo> serversCopy = Collections.unmodifiableSet(new HashSet<>(servers));
    return new ElectionContext(
        serversCopy,
        configuredServerCount,
        lastReplicationMessageNumber,
        electionTurn,
        System.currentTimeMillis()
    );
  }

  /**
   * Calculates the majority required for this election.
   *
   * @return The number of votes needed for majority
   */
  public int getMajority() {
    return (configuredServerCount / 2) + 1;
  }

  /**
   * Checks if enough votes have been collected for majority.
   *
   * @param votes The number of votes collected
   * @return true if majority is reached
   */
  public boolean hasMajority(final int votes) {
    return votes >= getMajority();
  }

  /**
   * Gets the number of other servers (excluding self) that can be contacted.
   *
   * @return The count of servers minus one (for self)
   */
  public int getOtherServerCount() {
    return servers.size() - 1;
  }

  /**
   * Calculates how long this election has been running.
   *
   * @return Duration in milliseconds since election context was created
   */
  public long getElapsedTime() {
    return System.currentTimeMillis() - createdAt;
  }

  @Override
  public String toString() {
    return "ElectionContext{" +
        "turn=" + electionTurn +
        ", servers=" + servers.size() +
        ", configured=" + configuredServerCount +
        ", majority=" + getMajority() +
        ", lastMsg=" + lastReplicationMessageNumber +
        ", elapsed=" + getElapsedTime() + "ms" +
        '}';
  }
}
