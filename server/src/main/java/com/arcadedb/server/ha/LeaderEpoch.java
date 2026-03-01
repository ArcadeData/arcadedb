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

/**
 * Represents a leader epoch in the HA cluster. Used for leader fencing to prevent
 * split-brain scenarios where an old leader might continue accepting writes after
 * a new leader has been elected.
 *
 * <p>Each time a new leader is elected, the epoch is incremented. All replication
 * messages include the epoch, and replicas reject messages from leaders with
 * older epochs.</p>
 *
 * @param epoch      The epoch number, monotonically increasing with each election
 * @param leaderName The name/alias of the leader server for this epoch
 * @param electedAt  Timestamp when the leader was elected (milliseconds since epoch)
 */
public record LeaderEpoch(long epoch, String leaderName, long electedAt) {

  /**
   * Creates a new LeaderEpoch with the current timestamp.
   *
   * @param epoch      The epoch number
   * @param leaderName The name of the leader server
   * @return A new LeaderEpoch instance
   */
  public static LeaderEpoch create(final long epoch, final String leaderName) {
    return new LeaderEpoch(epoch, leaderName, System.currentTimeMillis());
  }

  /**
   * Creates the initial epoch (epoch 0) for a server that starts as leader
   * without going through an election (single-node cluster).
   *
   * @param leaderName The name of the leader server
   * @return A new LeaderEpoch with epoch 0
   */
  public static LeaderEpoch initial(final String leaderName) {
    return new LeaderEpoch(0, leaderName, System.currentTimeMillis());
  }

  /**
   * Checks if this epoch supersedes (is newer than) another epoch.
   *
   * @param other The epoch to compare against
   * @return true if this epoch is newer than the other
   */
  public boolean supersedes(final LeaderEpoch other) {
    if (other == null)
      return true;
    return this.epoch > other.epoch;
  }

  /**
   * Checks if this epoch is the same as or newer than another epoch.
   *
   * @param other The epoch to compare against
   * @return true if this epoch is the same or newer
   */
  public boolean isCurrentOrNewer(final LeaderEpoch other) {
    if (other == null)
      return true;
    return this.epoch >= other.epoch;
  }

  /**
   * Creates the next epoch after this one, typically used when a new leader is elected.
   *
   * @param newLeaderName The name of the new leader
   * @return A new LeaderEpoch with incremented epoch number
   */
  public LeaderEpoch next(final String newLeaderName) {
    return new LeaderEpoch(this.epoch + 1, newLeaderName, System.currentTimeMillis());
  }

  @Override
  public String toString() {
    return "LeaderEpoch{epoch=" + epoch + ", leader='" + leaderName + "', electedAt=" + electedAt + "}";
  }
}
