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
package com.arcadedb.server.ha.raft;

import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * The windows in which a database accepts entries from this node only: a restore holds it from its claim to its release
 * (issue #10974), and an operation on the leader that must not interleave with a replica's write holds it for its
 * duration (issue #7438). Entries other nodes submit for it meanwhile are refused before they enter the log.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which consults the windows when it admits an entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ExclusiveDatabaseWindows {
  private final ArcadeStateMachine sm;

  /**
   * The databases on which this node, as leader, is running an operation that must not interleave with a replica's
   * transaction: a DDL (issue #7438), or the drop or reinstall of the database. Counted rather than flagged, since
   * operations on one database nest and overlap. See {@link #beginLeaderExclusive}.
   */
  private final ConcurrentHashMap<String, Integer> databasesExclusiveOnLeader = new ConcurrentHashMap<>();

  /**
   * The databases a restore on this server holds from its claim to its release (issue #10974): transaction entries of other
   * nodes for them are refused exactly as during a {@link #beginLeaderExclusive leader-exclusive} operation. Owned by the
   * {@link RaftHAServer}, which hands the same set to every state machine it creates ({@link #shareRestoreHolds}), so a hold
   * taken before an in-place Ratis restart still stands after it; a machine outside a Raft server keeps its own.
   */
  private volatile Set<String> databasesHeldForRestore = ConcurrentHashMap.newKeySet();

  ExclusiveDatabaseWindows(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  static NeedRetryException exclusiveOperationRefusal(final String databaseName) {
    return new NeedRetryException("Database '" + databaseName + "' is running a schema change, or being dropped, "
        + "reinstalled or restored, on the leader, so the leader cannot accept a transaction on it right now. Please retry");
  }

  /**
   * Whether a transaction entry of another node for {@code databaseName} must be refused because this leader runs an operation
   * on it that such an entry must not interleave with: a {@link #beginLeaderExclusive leader-exclusive} one, or a restore
   * holding it ({@link #holdForRestore}). Two lock-free map lookups.
   */
  boolean refusesEntriesOfOtherNodes(final String databaseName) {
    return databaseName != null && (databasesExclusiveOnLeader.containsKey(databaseName) || databasesHeldForRestore.contains(
        databaseName));
  }

  /**
   * Holds {@code databaseName} for a restore that has just claimed it (issue #10974), until {@link #releaseRestoreHold}: from
   * here on, transaction entries of other nodes for it are refused with a retryable error, and the call returns once the entries
   * this leader had already accepted for it - their page versions reserved, not applied yet - are applied. Without the wait such
   * an entry, accepted before the claim, would be appended and acknowledged to its client during the download, and destroyed
   * by the swap and the snapshot the restore then forces onto every node.
   * <p>
   * The same order as {@link #beginLeaderExclusive}, the registration before the wait, and the same bound. Not counted like
   * that registration: a restore claims a database once (the backup coordinator refuses a second restore of it), and a hold
   * taken again by the same restore must still end with one release.
   *
   * @return {@code true} when nothing was left in flight, {@code false} when the wait gave up; the hold stands either way
   */
  boolean holdForRestore(final String databaseName, final long drainTimeoutMs) {
    databasesHeldForRestore.add(databaseName);
    return awaitReservationsApplied(databaseName, drainTimeoutMs);
  }

  /** Ends the hold {@link #holdForRestore} took; a database not held is left alone. */
  void releaseRestoreHold(final String databaseName) {
    databasesHeldForRestore.remove(databaseName);
  }

  /** Whether a restore holds {@code databaseName}, see {@link #holdForRestore}. */
  boolean isHeldForRestore(final String databaseName) {
    return databasesHeldForRestore.contains(databaseName);
  }

  /**
   * Makes this machine read and write {@code holds} as its restore holds, see {@link #databasesHeldForRestore}. Called by the
   * {@link RaftHAServer} before the machine is handed to Ratis.
   */
  void shareRestoreHolds(final Set<String> holds) {
    this.databasesHeldForRestore = holds;
  }

  /**
   * Makes this leader exclusive on the database for the duration of a DDL, a drop or a reinstall (issue #7438): from
   * here until the matching {@link #endLeaderExclusive}, transaction entries of other nodes for it are refused with a
   * retryable error, and the call itself returns only once the entries the leader had already accepted are applied.
   * <p>
   * The order is what makes it work: the database is registered first, so nothing new is accepted, and only then does
   * the call wait for the reservations of the entries that were. Once it returns, every page the log has given a
   * version to is on this node, and nothing can be given one until the operation ends - so the DDL's own pages are
   * published against the versions the followers hold, and its {@code SCHEMA_ENTRY} takes the next place in the log
   * with no other entry claiming those versions.
   * <p>
   * The wait is bounded by {@code drainTimeoutMs}: an entry that cannot be applied must not hold the caller, which is
   * usually holding the database write lock, for ever. Applying an entry takes no database lock (the apply path in this
   * class never touches the write or read lock), so the caller's lock cannot be what the reservations wait on and a
   * healthy DDL does not spend the budget. The reservations counted are every entry's, the leader's own included: a
   * leader-local commit still between its append and its apply is waited for as well, which is what the DDL needs.
   * <p>
   * <b>Availability trade-off:</b> for the whole of the operation, entries of other nodes for this database are
   * refused with a retryable error, as the write lock already does to the leader's own writers. A long index build
   * therefore makes a replica's writers retry until it ends; a writer whose retry budget is smaller than the build sees
   * the {@link NeedRetryException} itself. The registration stays either way. Never throws; an
   * interruption ends the wait, keeps the registration and re-sets the interrupt flag.
   *
   * @return {@code true} when nothing was left in flight, {@code false} when the wait gave up
   */
  boolean beginLeaderExclusive(final String databaseName, final long drainTimeoutMs) {
    databasesExclusiveOnLeader.merge(databaseName, 1, Integer::sum);
    return awaitReservationsApplied(databaseName, drainTimeoutMs);
  }

  /**
   * Waits, at most {@code drainTimeoutMs}, for the page versions the log has reserved on {@code databaseName} to be applied
   * here. Never throws; an interruption ends the wait and re-sets the interrupt flag.
   *
   * @return {@code true} when nothing was left in flight, {@code false} when the wait gave up
   */
  private boolean awaitReservationsApplied(final String databaseName, final long drainTimeoutMs) {
    final long deadline = System.currentTimeMillis() + drainTimeoutMs;
    long pauseMs = 2L;
    int live;
    while ((live = sm.pageVersions.liveReservations(databaseName)) > 0) {
      if (System.currentTimeMillis() >= deadline) {
        LogManager.instance().log(sm, Level.WARNING,
            "Database '%s' still has %d page version(s) reserved by entries not applied yet after %d ms; the caller "
                + "gives up", databaseName, live, drainTimeoutMs);
        return false;
      }
      try {
        Thread.sleep(pauseMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      // Backed off (2, 4, 8, 16, 20 ms): each poll scans the database's ledger while the caller holds the write lock.
      pauseMs = Math.min(pauseMs * 2, 20L);
    }
    return true;
  }

  /** Ends one registration taken by {@link #beginLeaderExclusive}. */
  void endLeaderExclusive(final String databaseName) {
    final boolean[] wasOpen = new boolean[1];
    databasesExclusiveOnLeader.computeIfPresent(databaseName, (name, count) -> {
      wasOpen[0] = true;
      return count <= 1 ? null : count - 1;
    });
    if (!wasOpen[0])
      LogManager.instance().log(sm, Level.WARNING,
          "Ended a leader exclusive window on database '%s' that was not open: an unbalanced begin/end", databaseName);
  }
}
