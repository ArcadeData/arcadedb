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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.ReinstallOutcome;
import java.io.IOException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Re-verifies the closed local copies that could not be checked against the leader when this node joined (issue #8606):
 * on a back-off, it asks the current leader whether it still serves each one and reinstalls the copy the leader serves,
 * so a copy nobody vouched for does not stay in service indefinitely.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which schedules the re-verification on its lifecycle executor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class UnverifiedCopyReverifier {
  private final ArcadeStateMachine sm;

  // The leader the ladder above was climbed against: a different one may hold the database, so it starts over.
  private volatile RaftPeerId    unverifiedCopyReverifyLeader;

  // A round is queued or running: the ladder above is updated only when it ends, so without this a slow round (a slow
  // leader, a long download) would let every later tick queue another one behind it.
  private final    AtomicBoolean unverifiedCopyReverifyInFlight   = new AtomicBoolean();

  UnverifiedCopyReverifier(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Periodic re-verification of every closed copy this node keeps marked unverified (issue #8606), driven by the
   * {@link HealthMonitor} tick. The mark is written when a resync found the leader not holding the database (issue
   * #8589), and only an install or a drop removes it. Without this, the only things that tried an install again were a
   * later full resync, a Ratis-initiated install or a replicated entry for the database, so once the leader held the
   * database again - its 404 was transient, or an operator reopened it there - a database that is only read stayed
   * refused on this follower for good.
   * <p>
   * The marks are read from disk, so a copy marked by any path - the full and targeted resyncs and both reconciles - and
   * one restored after a restart are all found. For each, the leader is asked first whether its snapshot endpoint
   * would serve the database ({@link UnverifiedClosedCopyCheck#SERVES}: registered and not quarantined there): a leader
   * that still would not costs one small request, not a download retried with backoff.
   * When it does, the copy is reinstalled with the same targeted install a quarantine gets, which replaces the mark with
   * the copy; it stays marked when anything fails, so this never reopens a copy the leader did not send.
   * <p>
   * No-op on the leader (it reopens a marked copy on demand, after asking its peers - issue #8605), while a download is
   * running, and while no leader address is usable. Otherwise one round per
   * {@link #unverifiedClosedCopyReverifyIntervalMs(int)} after the previous one, on the {@code lifecycleExecutor}; with
   * nothing marked a round is one listing of the databases directory.
   */
  public void reverifyUnverifiedClosedCopies() {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null || sm.server == null || raftHA.isLeader())
      return;
    if (sm.snapshotDownloadInProgress.get())
      return;
    // The same precheck the other retries make, so a tick with no usable leader does not spend the slot.
    final RaftPeerId leaderId = raftHA.getLeaderId();
    if (leaderId == null)
      return;
    final String leaderHttpAddr = raftHA.getUnambiguousPeerHttpAddress(leaderId);
    if (leaderHttpAddr == null || raftHA.isOwnHttpAddress(leaderHttpAddr))
      return;
    if (!leaderId.equals(unverifiedCopyReverifyLeader)) {
      // A new leader may hold what the previous one did not: ask it now rather than at the end of the old ladder.
      unverifiedCopyReverifyLeader = leaderId;
      restartUnverifiedClosedCopyReverification();
      sm.lastUnverifiedCopyReverifyMs.set(0L);
    }
    if (unverifiedCopyReverifyInFlight.get())
      return;
    final long now = System.currentTimeMillis();
    final long previous = sm.lastUnverifiedCopyReverifyMs.get();
    if (previous != 0 && now - previous < unverifiedClosedCopyReverifyIntervalMs(sm.unverifiedCopyReverifyFailures.get()))
      return;
    if (!sm.lastUnverifiedCopyReverifyMs.compareAndSet(previous, now))
      return;
    if (!unverifiedCopyReverifyInFlight.compareAndSet(false, true))
      return;
    try {
      sm.lifecycleExecutor.submit(() -> {
        try {
          reverifyUnverifiedClosedCopiesNow();
        } catch (final RuntimeException e) {
          // The executor would swallow it: said here, and counted, so a round that keeps failing is neither silent nor
          // retried at the short end of the ladder.
          sm.unverifiedCopyReverifyFailures.incrementAndGet();
          LogManager.instance().log(sm, Level.WARNING,
              "Re-verification of unverified closed copies failed: %s. The next health check retries it", e,
              e.getMessage());
        } finally {
          unverifiedCopyReverifyInFlight.set(false);
        }
      });
    } catch (final RejectedExecutionException ree) {
      unverifiedCopyReverifyInFlight.set(false);
      LogManager.instance().log(sm, Level.FINE,
          "Cannot schedule the re-verification of unverified closed copies: executor is shut down");
    }
  }

  /**
   * Restarts the re-verification ladder of {@link #reverifyUnverifiedClosedCopies()}: the next health tick runs a round
   * once {@link ArcadeStateMachine#UNVERIFIED_COPY_REVERIFY_INTERVAL_MS} has passed since the previous one. Called when this node refused a
   * request for a marked copy (issue #8606): someone wants the database now, so the wait is no longer the long end of
   * the ladder. Cheap and bounded however often it is called: it never starts a round itself. Under steady demand for a
   * database the leader still does not serve, it deliberately holds the short interval: one listing and one small
   * request per marked copy every {@link ArcadeStateMachine#UNVERIFIED_COPY_REVERIFY_INTERVAL_MS}.
   */
  public void restartUnverifiedClosedCopyReverification() {
    sm.unverifiedCopyReverifyFailures.set(0);
  }

  /**
   * The wait after a round for the next one, given how many rounds in a row left a marked copy in place: doubling from
   * {@link ArcadeStateMachine#UNVERIFIED_COPY_REVERIFY_INTERVAL_MS} up to {@link ArcadeStateMachine#UNVERIFIED_COPY_REVERIFY_MAX_INTERVAL_MS}.
   */
  static long unverifiedClosedCopyReverifyIntervalMs(final int consecutiveFailures) {
    final int doublings = Math.min(Math.max(consecutiveFailures, 0), 16);
    return Math.min(ArcadeStateMachine.UNVERIFIED_COPY_REVERIFY_INTERVAL_MS << doublings, ArcadeStateMachine.UNVERIFIED_COPY_REVERIFY_MAX_INTERVAL_MS);
  }

  /** One round of {@link #reverifyUnverifiedClosedCopies()}, on the {@code lifecycleExecutor}. */
  private void reverifyUnverifiedClosedCopiesNow() {
    final ArcadeDBServer localServer = sm.server;
    final RaftHAServer raftHA = sm.raftHAServer;
    if (localServer == null || raftHA == null)
      return;
    final List<String> marked = new ArrayList<>();
    try {
      for (final String dbName : SnapshotInstaller.closedDatabaseNames(localServer))
        if (SnapshotInstaller.isUnverifiedClosedCopy(localServer, dbName))
          marked.add(dbName);
    } catch (final IOException e) {
      LogManager.instance().log(sm, Level.WARNING,
          "Cannot list the databases directory to re-verify unverified closed copies: %s", e.getMessage());
      sm.unverifiedCopyReverifyFailures.incrementAndGet();
      return;
    }
    if (marked.isEmpty()) {
      sm.unverifiedCopyReverifyFailures.set(0);
      return;
    }

    final RaftPeerId leaderId = raftHA.getLeaderId();
    final PeerDialAddress source = sm.resync.resolveSnapshotSource(leaderId);
    if (leaderId == null || source.refused()) {
      HALog.log(sm, HALog.BASIC, "Not re-verifying unverified closed copies %s: %s", marked,
          leaderId == null ? "no leader is known" : source.refusal());
      sm.unverifiedCopyReverifyFailures.incrementAndGet();
      return;
    }
    final String clusterToken = raftHA.getClusterToken();
    boolean anyLeft = false;
    boolean busy = false;
    for (final String dbName : marked) {
      final boolean leaderServesIt;
      try {
        leaderServesIt = leaderServesDatabase(localServer, raftHA, leaderId, source, dbName, clusterToken);
      } catch (final InterruptedException e) {
        // The executor is shutting down: every later question would fail the same way at once.
        Thread.currentThread().interrupt();
        anyLeft = true;
        break;
      } catch (final Exception e) {
        HALog.log(sm, HALog.BASIC, "Could not ask the leader about database '%s', closed and unverified here: %s",
            dbName, e.getMessage());
        anyLeft = true;
        continue;
      }
      if (!leaderServesIt) {
        HALog.log(sm, HALog.BASIC,
            "Database '%s' stays closed and unverified on this follower: the leader still does not serve it", dbName);
        anyLeft = true;
        continue;
      }
      final ReinstallOutcome outcome = reinstallUnverifiedClosedCopy(dbName, source, clusterToken);
      if (outcome == ReinstallOutcome.LEFT_MARKED)
        anyLeft = true;
      else if (outcome == ReinstallOutcome.BUSY)
        busy = true;
    }
    // A copy left only because another download held the single flight says nothing about the leader: the ladder
    // stays where it is, and the next round comes after the same wait.
    if (anyLeft)
      sm.unverifiedCopyReverifyFailures.incrementAndGet();
    else if (!busy)
      sm.unverifiedCopyReverifyFailures.set(0);
  }

  /**
   * Asks the leader whether it would serve {@code dbName}'s snapshot ({@link UnverifiedClosedCopyCheck#SERVES}),
   * through the {@code copyOf} form of the bootstrap-state RPC (issues #8605, #8606), answered from its registry alone:
   * nothing is opened or hashed there. Bounded by {@link UnverifiedClosedCopyCheck#ROUND_TIMEOUT_MS}, and refused unless
   * the leader itself answered.
   */
  private boolean leaderServesDatabase(final ArcadeDBServer localServer, final RaftHAServer raftHA,
      final RaftPeerId leaderId, final PeerDialAddress source, final String dbName, final String clusterToken)
      throws Exception {
    final boolean useSSL = localServer.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final String url = BootstrapElection.chooseUrl(source.httpAddress(), source.httpsAddress(), useSSL);
    if (url == null)
      throw new IOException("no address of the leader this node may dial");
    final HttpClient client =
        url.startsWith("https://") ? raftHA.getHttpsClients().clientFor(localServer) : BootstrapElection.HTTP;
    final CompletableFuture<Boolean> answer =
        UnverifiedClosedCopyCheck.askWhetherServed(client, leaderId.toString(), url, useSSL, dbName, clusterToken);
    try {
      return answer.get(UnverifiedClosedCopyCheck.ROUND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (final ExecutionException e) {
      throw e.getCause() instanceof Exception cause ? cause : e;
    } finally {
      answer.cancel(true);
    }
  }

  /**
   * Replaces the closed, unverified copy of {@code dbName} with the leader's, under the same single-flight protocol as
   * {@link ArcadeStateMachine#triggerDatabaseResync}, and settles it as that resync settles a success.
   *
   * @return how it ended; anything but {@link ReinstallOutcome#SETTLED} leaves the copy closed and marked
   */
  private ReinstallOutcome reinstallUnverifiedClosedCopy(final String dbName, final PeerDialAddress source,
      final String clusterToken) {
    if (!sm.snapshotDownloadInProgress.compareAndSet(false, true))
      return ReinstallOutcome.BUSY; // a download is running; the next round asks again
    try {
      if (!sm.snapshotDownloadLock.tryLock())
        return ReinstallOutcome.BUSY;
      try {
        // Re-checked under the single flight: an install, a drop or an operator may have settled it since the listing.
        if (sm.server.existsDatabase(dbName) || !SnapshotInstaller.isUnverifiedClosedCopy(sm.server, dbName))
          return ReinstallOutcome.SETTLED;
        try {
          sm.replacement.installLeaderCopy(dbName, source.httpAddress(), source.httpsAddress(), clusterToken);
        } catch (final LeaderDoesNotHoldDatabaseException e) {
          // The leader closed it again between its answer and the download: the copy keeps its mark.
          HALog.log(sm, HALog.BASIC, "Database '%s' stays closed and unverified on this follower: %s", dbName,
              e.getMessage());
          return ReinstallOutcome.LEFT_MARKED;
        }
        LogManager.instance().log(sm, Level.INFO,
            "Database '%s', closed on this follower and unverified since the leader did not hold it, has been "
                + "reinstalled from the leader, which holds it again: it is served again (issue #8606)", dbName);
        sm.reconciler.clearLeaderMissing(dbName);
        sm.quarantine.clearDivergedDatabase(dbName);
        sm.baselines.clearBootstrapUnreconciled(dbName);
        sm.bootstrapInstalls.settleBootstrapReplacement(dbName);
        return ReinstallOutcome.SETTLED;
      } finally {
        sm.snapshotDownloadLock.unlock();
      }
    } catch (final Exception e) {
      LogManager.instance().log(sm, Level.WARNING,
          "Re-installing database '%s', closed and unverified on this follower, from the leader failed: %s. It stays "
              + "closed and the next health check retries it", dbName, e.getMessage());
      return ReinstallOutcome.LEFT_MARKED;
    } finally {
      sm.snapshotDownloadInProgress.set(false);
    }
  }
}
