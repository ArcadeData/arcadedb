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

import com.arcadedb.log.LogManager;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

/**
 * Implements leader fencing to prevent split-brain scenarios in the HA cluster.
 *
 * <p>When a new leader is elected, old leaders must be "fenced" - prevented from
 * accepting any further writes. This prevents data corruption when an old leader
 * that was temporarily partitioned continues to accept writes after a new leader
 * has been elected.</p>
 *
 * <p>The fencing mechanism works as follows:</p>
 * <ol>
 *   <li>Each leader has an epoch number that increases with each election</li>
 *   <li>All replication messages include the leader's epoch</li>
 *   <li>When a node sees a higher epoch, it fences the current leader</li>
 *   <li>A fenced leader rejects all write operations with LeaderFencedException</li>
 * </ol>
 *
 * <p>Thread-safety: This class is thread-safe. The fenced flag uses volatile
 * semantics, and epoch updates are performed atomically.</p>
 */
public class LeaderFence {

  private final AtomicReference<LeaderEpoch> currentEpoch;
  private volatile boolean                   fenced;
  private volatile String                    fencedReason;
  private final String                       serverName;

  /**
   * Creates a new LeaderFence for a server.
   *
   * @param serverName The name of this server (for logging)
   */
  public LeaderFence(final String serverName) {
    this.serverName = serverName;
    this.currentEpoch = new AtomicReference<>(null);
    this.fenced = false;
    this.fencedReason = null;
  }

  /**
   * Creates a new LeaderFence with an initial epoch.
   *
   * @param serverName   The name of this server
   * @param initialEpoch The initial epoch (typically from election or startup)
   */
  public LeaderFence(final String serverName, final LeaderEpoch initialEpoch) {
    this.serverName = serverName;
    this.currentEpoch = new AtomicReference<>(initialEpoch);
    this.fenced = false;
    this.fencedReason = null;
  }

  /**
   * Checks if this leader is fenced and throws an exception if so.
   * This method should be called before any write operation.
   *
   * @throws LeaderFencedException if the leader has been fenced
   */
  public void checkFenced() throws LeaderFencedException {
    if (fenced) {
      throw new LeaderFencedException(
          "Leader '" + serverName + "' is fenced: " + (fencedReason != null ? fencedReason : "unknown reason"));
    }
  }

  /**
   * Checks if this leader is fenced.
   *
   * @return true if fenced, false otherwise
   */
  public boolean isFenced() {
    return fenced;
  }

  /**
   * Fences this leader, preventing any further write operations.
   *
   * @param reason The reason for fencing (for logging and diagnostics)
   */
  public synchronized void fence(final String reason) {
    if (!fenced) {
      fenced = true;
      fencedReason = reason;
      LogManager.instance().log(this, Level.WARNING,
          "Leader '%s' has been FENCED: %s", serverName, reason);
    }
  }

  /**
   * Removes the fence, allowing write operations again.
   * This is typically called when a server becomes leader again after an election.
   */
  public synchronized void unfence() {
    if (fenced) {
      fenced = false;
      fencedReason = null;
      LogManager.instance().log(this, Level.INFO,
          "Leader '%s' fence has been removed", serverName);
    }
  }

  /**
   * Gets the current epoch.
   *
   * @return The current LeaderEpoch, or null if not yet set
   */
  public LeaderEpoch getCurrentEpoch() {
    return currentEpoch.get();
  }

  /**
   * Gets the current epoch number, or -1 if no epoch is set.
   *
   * @return The epoch number or -1
   */
  public long getEpochNumber() {
    final LeaderEpoch epoch = currentEpoch.get();
    return epoch != null ? epoch.epoch() : -1;
  }

  /**
   * Accepts a new epoch from an election or leader announcement.
   * If the new epoch supersedes the current one, updates the epoch and returns true.
   * If the new epoch is older, returns false.
   *
   * @param newEpoch The new epoch to accept
   * @return true if the epoch was accepted (was newer), false if rejected (was older or same)
   */
  public boolean acceptEpoch(final LeaderEpoch newEpoch) {
    if (newEpoch == null)
      return false;

    while (true) {
      final LeaderEpoch current = currentEpoch.get();

      if (current != null && !newEpoch.supersedes(current)) {
        // New epoch is not newer than current - reject
        return false;
      }

      if (currentEpoch.compareAndSet(current, newEpoch)) {
        LogManager.instance().log(this, Level.INFO,
            "Server '%s' accepted new epoch: %s (previous: %s)",
            serverName, newEpoch, current);
        return true;
      }
      // CAS failed, another thread updated - retry
    }
  }

  /**
   * Checks if a message from a leader with the given epoch should be accepted.
   * Messages from leaders with older epochs are rejected.
   *
   * @param messageEpoch The epoch of the leader that sent the message
   * @return true if the message should be accepted, false if it should be rejected
   */
  public boolean shouldAcceptMessage(final LeaderEpoch messageEpoch) {
    if (messageEpoch == null)
      return false;

    final LeaderEpoch current = currentEpoch.get();
    if (current == null) {
      // No epoch set yet - accept any valid epoch
      return true;
    }

    return messageEpoch.isCurrentOrNewer(current);
  }

  /**
   * Checks if a message from a leader with the given epoch should be accepted,
   * and if not, fences the local leader (if we are one).
   *
   * @param messageEpoch The epoch from the incoming message
   * @param isLeader     Whether this server is currently a leader
   * @return true if the message should be accepted
   */
  public boolean validateAndMaybeFence(final LeaderEpoch messageEpoch, final boolean isLeader) {
    if (messageEpoch == null)
      return false;

    final LeaderEpoch current = currentEpoch.get();

    // If message has a newer epoch
    if (current != null && messageEpoch.supersedes(current)) {
      // Accept the new epoch
      acceptEpoch(messageEpoch);

      // If we thought we were the leader, we need to be fenced
      if (isLeader) {
        fence("Received message from newer leader epoch " + messageEpoch.epoch() +
            " (leader: " + messageEpoch.leaderName() + ")");
      }
      return true;
    }

    // If message has an older epoch, reject it
    if (current != null && !messageEpoch.isCurrentOrNewer(current)) {
      LogManager.instance().log(this, Level.WARNING,
          "Rejecting message from stale leader epoch %d (current: %d)",
          messageEpoch.epoch(), current.epoch());
      return false;
    }

    return true;
  }

  /**
   * Sets a new epoch when this server becomes leader.
   * This unfences the server and sets the new epoch.
   *
   * @param epoch The new epoch for this leader
   */
  public void becomeLeader(final LeaderEpoch epoch) {
    currentEpoch.set(epoch);
    unfence();
    LogManager.instance().log(this, Level.INFO,
        "Server '%s' became leader with epoch %s", serverName, epoch);
  }

  /**
   * Called when this server steps down from being leader.
   * The server is fenced to prevent any further writes.
   *
   * @param reason The reason for stepping down
   */
  public void stepDown(final String reason) {
    fence("Stepped down: " + reason);
  }

  /**
   * Resets the fence state. Used during testing or when rejoining cluster.
   */
  public synchronized void reset() {
    currentEpoch.set(null);
    fenced = false;
    fencedReason = null;
  }

  @Override
  public String toString() {
    return "LeaderFence{" +
        "server='" + serverName + '\'' +
        ", epoch=" + currentEpoch.get() +
        ", fenced=" + fenced +
        (fencedReason != null ? ", reason='" + fencedReason + '\'' : "") +
        '}';
  }
}
