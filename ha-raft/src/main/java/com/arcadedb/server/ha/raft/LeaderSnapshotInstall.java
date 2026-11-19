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

import com.arcadedb.log.LogManager;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * Installs the snapshot the Raft leader tells this follower to take when its log has been compacted past what the
 * follower holds: downloads every database, and resolves the term and index the install leaves this node at, so the
 * applied position never moves to a point the files on disk do not match.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which receives the request from Ratis and runs the install on its own executor.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LeaderSnapshotInstall {
  private final ArcadeStateMachine sm;

  LeaderSnapshotInstall(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /** The body of {@link ArcadeStateMachine#notifyInstallSnapshotFromLeader}, run on {@link ArcadeStateMachine#snapshotInstallExecutor}. */
  TermIndex installSnapshotFromLeader(final RaftProtos.RoleInfoProto roleInfoProto,
      final TermIndex firstTermIndexInLog) {
    // Participate in the same single-flight protocol as triggerSnapshotDownload() so that
    // isSnapshotDownloadPending() returns true during this install and the HealthMonitor's
    // recoverFromPersistentLag() does not initiate a new concurrent triggerSnapshotDownload().
    // We use CAS (not unconditional set) to avoid clearing a flag owned by a concurrently
    // running triggerSnapshotDownload():
    //  - if we win (flag false->true): we own the flag and MUST clear it in finally.
    //  - if we lose (flag already true, another download in progress): we skip the flag but still perform the
    //    install, because standing down would report an install that never happened.
    // Losing the CAS used to mean the two downloads ran concurrently over one set of database directories -
    // benign only for as long as SnapshotInstaller keeps swapping atomically. snapshotDownloadLock states the
    // exclusion outright: this path waits for it (it owns its thread and may block), while the request-driven
    // paths fold into whatever holds it (issue #6202).
    final boolean acquiredSnapshotFlag = sm.snapshotDownloadInProgress.compareAndSet(false, true);
    try {
      // Interruptibly, so close()'s shutdownNow() can unwind a thread parked behind an in-flight resync instead
      // of holding the shutdown open for the length of somebody else's download.
      sm.snapshotDownloadLock.lockInterruptibly();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      if (acquiredSnapshotFlag)
        sm.snapshotDownloadInProgress.set(false);
      throw new RuntimeException("Interrupted while waiting for an in-flight resync to finish", e);
    }
    try {
      // Read the volatile ONCE for this whole install: resolveSnapshotSource() reads it into its own local so it
      // can refuse instead of throwing, and reading the field again below for the cluster token would reopen the
      // window that read is written to close - a teardown nulling it between the two turns a refusal into a
      // NullPointerException on the automatic resync path (issue #7253).
      final RaftHAServer raftHA = sm.raftHAServer;
      final RaftPeerId leaderId = RaftPeerId.valueOf(
          roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId());

      // The guards the manual resync path has always made, which this one had none of (issue #6202): a derived
      // address can name this node itself or the wrong peer, and reconcileDatabasesFromLeader would succeed,
      // the install would be recorded, the read floor dropped, and the node would return to the ready set
      // carrying whatever it copied. Refusing is the honest disposition - Ratis retries the install.
      final PeerDialAddress source = sm.resync.resolveSnapshotSource(leaderId);
      if (source.refused())
        throw new SnapshotRefusedException(source.refusal());

      final String leaderHttpAddr = source.httpAddress();
      // Both endpoints from the verdict: the reconciler prefers the encrypted one whenever it is non-null and
      // threads it into every branch, so a raw HTTPS address here would walk this path - the automatic one, the
      // one that had no checks at all before #6202 - straight back into the bug (issue #6221).
      final String leaderHttpsAddr = source.httpsAddress();
      final String clusterToken = raftHA != null ? raftHA.getClusterToken() : null;

      // A safe LOWER BOUND on the boundary each reconciled database's copy is about to cover (issue #8577),
      // known upfront and independent of the leader's snapshot marker: whatever resolveInstalledSnapshotBoundary
      // below finally registers - firstTermIndexInLog-1 in the common case, or firstTermIndexInLog itself when the
      // leader's marker is past its log start - is always >= this value, so recording it on every reconciled
      // database's install lock cannot skip an entry the final boundary would not also cover. Computed before the
      // reconcile so it is in place for every per-database install, not just the last one: entries this node is
      // still replaying from its own local log below the leader's compacted log start are stale once any of those
      // installs completes, whichever runs first.
      final long installedBoundaryIndex = Math.max(0L, firstTermIndexInLog.getIndex() - 1);

      // Databases the reconciler gave up on: it stopped failing the install for them, so they are NOT at the
      // snapshot index and must not be recorded as if they were (issue #6760). The reconciler also carries back
      // the leader's own latest Raft snapshot TermIndex (issue #8360), fetched over the very same bootstrap-state
      // RPC call, below.
      final DatabaseReconciler.ReconcileFromLeaderResult reconcileResult =
          sm.reconciler.reconcileDatabasesFromLeader(leaderId.toString(), leaderHttpAddr, leaderHttpsAddr, clusterToken,
              installedBoundaryIndex);
      final Set<String> notInstalled = reconcileResult.notInstalled();
      final Set<String> leaderMissing = Objects.requireNonNullElse(reconcileResult.leaderMissing(), Set.of());

      // Compute the installed snapshot TermIndex: normally firstTermIndexInLog - 1, the end of what the snapshot covers,
      // or firstTermIndexInLog itself when the leader's marker is past it (issue #8449, see
      // resolveInstalledSnapshotBoundary). Whatever is returned must also be what registerSnapshotMarker() records
      // below: StateMachineUpdater.reload() reads getLatestSnapshot() back and expects the two to match - returning a
      // TermIndex storage never recorded caused a NullPointerException (and before that, an IllegalStateException
      // from the PAUSED check). SnapshotInstallationHandler then purges this follower's log up to the returned index,
      // which the copied databases cover in both cases.
      final TermIndex leaderSnapshotTermIndex = reconcileResult.leaderSnapshotTermIndex();
      final TermIndex installedTermIndex = resolveInstalledSnapshotBoundary(firstTermIndexInLog, leaderSnapshotTermIndex);
      final long snapshotIndex = installedTermIndex.getIndex();
      final long snapshotTerm = installedTermIndex.getTerm();
      // Logged from the decision's result, so the message cannot drift from what was registered
      if (leaderSnapshotTermIndex == null)
        LogManager.instance().log(sm, Level.WARNING,
            "Leader %s reported no snapshot marker (a build that predates issue #8360); registering the approximate "
                + "term of the next log entry for install index %d. A term change exactly at that boundary would stall "
                + "this follower until the leader compacts past it; upgrading the leader closes the window (issue #8374).",
            leaderId, snapshotIndex);
      else if (installedTermIndex.equals(firstTermIndexInLog))
        // Segment-granularity purging left the leader's log start at or before its marker: the index before it is
        // neither in the leader's log nor its marker, so LogAppender.getPrevious would answer null and the leader
        // would re-notify this install forever (issue #8449).
        LogManager.instance().log(sm, Level.INFO,
            "Leader snapshot marker %s is past its log start %s; registering the log start as the install boundary "
                + "so the leader can resume replication (issue #8449).",
            leaderSnapshotTermIndex, firstTermIndexInLog);
      else if (!leaderSnapshotMatches(snapshotIndex, leaderSnapshotTermIndex))
        // Unexpected: Ratis purges only up to its snapshot, so the notifying leader's marker is at least S-1. A leader
        // change between the notification and the marker read could produce it; the approximation is kept
        // (issues #8360, #8449).
        LogManager.instance().log(sm, Level.FINE,
            "Leader-reported snapshot boundary %s is older than the install index %d; registering the approximate "
                + "term of the next log entry (issue #8360).",
            leaderSnapshotTermIndex, snapshotIndex);

      // Issue #8353: the entries this install skips may include the removal and the re-add of THIS node, which then
      // never reaches the runtime-join detector as a configuration change, so its previous membership's security
      // installs would keep counting toward readiness. Every install is a join boundary on an armed node instead.
      // BEFORE the snapshot marker and the applied index: a restart must not find the marker without the boundary,
      // and a catch-up match read concurrently must be judged against the boundary, not against the old join.
      sm.runtimeJoinDetector.onSnapshotInstalledFromLeader(snapshotIndex);

      // Advance the local applied-index to the snapshot point so that the StateMachineUpdater
      // knows which log entries have been consumed by this install. A full state-machine install
      // brings EVERY present database to the snapshot point, so record the snapshot index for each
      // of them too (not just the global position) - this keeps the per-database bootstrap
      // replay-skip honest after a full resync (issue #4824).
      // The boundary, the seed and the durable write under one lock (issue #8651): the apply thread may still be
      // replaying this node's pre-install log, and it checks the boundary and moves the applied positions under the
      // same lock, so a stale entry either lands entirely before the seed (which then moves the positions forward) or
      // sees the boundary and leaves them alone - never checks before and writes after.
      // BEFORE the snapshot marker (issue #11531): the marker authorises Ratis to purge the log up to snapshotIndex, so the
      // quarantine and the read floor of every database this install gave up on must already be on disk by then.
      final boolean durable;
      synchronized (sm.appliedPositionLock) {
        sm.recordInstalledRaftBoundary(snapshotIndex);
        sm.lastAppliedIndex.set(snapshotIndex);
        // An install skips the entries up to the snapshot: any schema change among them was never applied here one by one
        sm.schemaHistory.schemaHistoryIncompleteUpTo(snapshotIndex);
        sm.updateLastAppliedTermIndexAfterInstall(snapshotTerm, snapshotIndex);
        // The applied positions, the healed diverged marks and read floors of every database the install refreshed, and
        // the re-armed ones of those it did not reinstall, all in ONE durable write (issues #6760, #8137): a crash between
        // separate writes could leave a position at snapshotIndex with no quarantine for a database it gave up on
        // A database the reconcile reported LEADER_MISSING was kept, not refreshed, so its own applied position is not
        // advanced to the snapshot index either (issue #8588); it is not quarantined, unlike notInstalled.
        durable = sm.resync.completeSnapshotInstall(snapshotIndex, notInstalled, leaderMissing);
      }
      // A quarantine that lives in memory alone is gone after a restart, and once the marker below is registered Ratis
      // purges the log up to it: a database this install gave up on would come back ready, unclamped, with nothing left
      // to replay into it (issue #11531). So the install fails before the marker, Ratis neither records the boundary nor
      // purges, and it re-drives the install. The quarantine stays in force in memory meanwhile. An install that gave up
      // on nothing publishes no quarantine and is not failed: a position that did not reach disk leaves the marker ahead
      // of the persisted one, which the restart's snapshot-gap check answers with a resync (issue #6111).
      if (!durable && !notInstalled.isEmpty())
        throw new IOException("Snapshot install could not persist the quarantine of database(s) " + notInstalled
            + " it could not reinstall to " + sm.appliedIndexes.getAppliedIndexFile() + "; not registering the snapshot marker at index "
            + snapshotIndex + " (issue #11531)");

      // Register the snapshot in SimpleStateMachineStorage. StateMachineUpdater.reload() calls
      // getLatestSnapshot() immediately after reinitialize() and requires a non-null result.
      // registerSnapshotMarker() writes the empty marker file and updates the latest-snapshot
      // reference; see its javadoc for why a file-less, null-digest marker is safe for ArcadeDB.
      if (!sm.registerSnapshotMarker(snapshotTerm, snapshotIndex))
        throw new IOException("Failed to register snapshot marker at index " + snapshotIndex);

      // The install brought every database up to the snapshot point, so any read floor an earlier
      // stale marker published is now satisfied. Cleared BEFORE the notify below so a woken waiter
      // re-checks against the restored state instead of the floor (issue #6111).
      sm.resync.clearStaleSnapshotFloor();

      // Names the LEADER_MISSING databases: kept unrefreshed, they may keep the node out of the Service (issue #8702).
      LogManager.instance().log(sm, Level.INFO,
          "HA resync finished (mode=snapshot, result=%s): snapshotIndex=%d%s",
          notInstalled.isEmpty() ? "ok" : "partial", snapshotIndex,
          leaderMissing.isEmpty() ? "" :
              ", kept without refresh because the leader does not hold them: " + leaderMissing);
      // Only a reinstalled database loses its #6124 mark and its #8367 pending replacement: a given-up (#6760) or
      // LEADER_MISSING (#8588) one still holds the copy the bootstrap baseline rejected (issue #8702).
      final Set<String> notReplaced = BootstrapBaselines.notReplaced(notInstalled, leaderMissing);
      sm.baselines.clearBootstrapUnreconciledExcept(notReplaced);
      sm.bootstrapInstalls.settleBootstrapReplacementsExcept(notReplaced);

      // Wake any threads blocked in RaftHAServer.waitForAppliedIndex()/waitForLocalApply(): this
      // leader-driven snapshot install advances the applied index without going through
      // applyTransaction(), the only other notifyApplied() call site (issue #5846).
      //
      // LAST, after every floor and diverged mark of this install is in its final state. notifyApplied() holds
      // applyNotifier only long enough to notifyAll(), so a waiter can reacquire it and re-check
      // getTrustedAppliedIndex(db) immediately. Notifying any earlier than this leaves a window in which the
      // global floor is already cleared and the per-database one is not yet published - the first give-up never
      // had one - so the woken waiter sees the raw Ratis
      // index, which already equals snapshotIndex, and a LINEARIZABLE or read-your-writes read of a database
      // this install did NOT refresh passes its wait and is served from the stale copy. That is precisely the
      // outcome issue #6760 exists to prevent, so the notify has to come after the re-arm, not before it.
      if (raftHA != null)
        raftHA.notifyApplied();

      // Issue #7833: a snapshot install is the one catch-up path that provably skips the security entries - the
      // three documents live under <server-root>/config/, outside the database directory, and no snapshot
      // carries them. Ask the leader whether this node is still in step, AFTER the install is fully recorded so
      // the request cannot be answered against a half-installed node.
      //
      // A security entry this node had not made durable is below the install's boundary now, so it is not replayed any
      // more: the leader is asked to re-send the documents even when this node's copies in memory match (issue #7752).
      // Safe off the apply thread only because Ratis pauses the apply while it installs: the floor's retry pacing is
      // apply-thread state (see SecurityDurabilityFloor), so do not move this call anywhere the apply can run.
      final boolean undurableSecurityCovered = sm.securityDurabilityFloor.dropCoveredByInstall(snapshotIndex);
      if (undurableSecurityCovered)
        LogManager.instance().log(sm, Level.WARNING,
            "The snapshot install up to index %d covered a replicated security entry that never reached this node's disk, "
                + "so it will not be replayed: asking the leader to re-send the security documents. Until they commit, a "
                + "restart of this node reads its stale security files back (issue #7752)", snapshotIndex);
      if (raftHA != null)
        sm.securityCatchUp.afterSnapshotInstall(sm.server, raftHA, undurableSecurityCovered);

      return installedTermIndex;
    } catch (final SnapshotRefusedException e) {
      // A refusal is the expected, retried outcome this guard exists to produce, not a fault: Ratis re-drives
      // the install, so on a misconfigured cluster - or in the window right after an election, before the
      // leader-role flag catches up - it fires on every attempt. Logged at WARNING and without a stack trace,
      // like the same refusal on the two request-driven paths; a SEVERE per retry would trip log-based alerting
      // for a guard that is working as designed.
      LogManager.instance().log(sm, Level.WARNING, ArcadeStateMachine.SNAPSHOT_INSTALL_REFUSED + "%s", e.reason());
      throw new RuntimeException("Error during Raft snapshot installation", e);
    } catch (final Exception e) {
      LogManager.instance().log(sm, Level.SEVERE, "Error during snapshot installation from leader", e);
      throw new RuntimeException("Error during Raft snapshot installation", e);
    } finally {
      // Released in this order on purpose: the flag is the broader signal - isSnapshotDownloadPending() feeds the
      // HealthMonitor's decision not to start anything - so it must not read false while this install still holds
      // the lock. The converse window it leaves (lock free, flag still true) costs a concurrent request one folded
      // attempt, which the next retryUnfilledSnapshotGap() tick re-drives; swapping the two would only move the
      // window, not close it, and would move it to the side where something new can be started under a held lock.
      sm.snapshotDownloadLock.unlock();
      if (acquiredSnapshotFlag)
        sm.snapshotDownloadInProgress.set(false);
    }
  }

  /**
   * Picks the term to register for a leader-driven snapshot install at {@code snapshotIndex} (issue #8360).
   * <p>
   * The correct value is whatever term the LEADER will put in {@code previous} for its next {@code AppendEntries}
   * at {@code snapshotIndex + 1}. That index is no longer in the leader's log, so Ratis's
   * {@code LogAppender.getPreviousLog()} falls back to the leader's own snapshot marker
   * ({@code getLatestSnapshot().getTermIndex()}) - the value {@link ArcadeStateMachine#getLatestSnapshotTermIndex()} serves. Matching
   * that marker is what matters, even if the marker itself carries an inflated term (#575/#593): the follower must
   * agree with the leader, not with an abstract "true" term. {@code fallbackTerm} (the term of the NEXT log entry,
   * {@code snapshotIndex + 1}) is only an approximation: it is wrong whenever a term/leadership change lands exactly
   * on that boundary, which a rolling restart or a chaos-fault election makes routine rather than rare.
   * <p>
   * The mismatch matters because Ratis trusts whatever {@code TermIndex} this method's caller returns as gospel:
   * {@code ServerState.reloadStateMachine} stores it verbatim in {@code latestInstalledSnapshot}, and
   * {@code ServerState.containsTermIndex} - which answers the {@code AppendEntries} log-matching check the LEADER
   * runs for every subsequent replication attempt starting right after this boundary - accepts only an EXACT
   * {@code equals()} match. A wrong term therefore does not degrade gracefully: every future {@code AppendEntries}
   * whose {@code previous} is this boundary is rejected, the leader cannot walk further back (that is exactly why
   * it drove a snapshot install here in the first place), and its re-notification at the same
   * {@code firstAvailableLogIndex} is answered {@code ALREADY_INSTALLED} by Ratis without calling this state machine
   * again. The wrong term stays until the leader compacts past the boundary (issue #8374).
   * <p>
   * {@code leaderSnapshotTermIndex} is the leader's own answer, read over the bootstrap-state RPC
   * ({@link ArcadeStateMachine#getLatestSnapshotTermIndex()} on the leader). The reconciler fails the install when it cannot ask, so
   * {@code null} here means only a leader build that predates the field. It is trusted only when its index matches
   * {@code snapshotIndex}. A mismatch is not harmless: {@code LogAppender.getPrevious} then returns {@code null},
   * which makes the leader re-notify the install instead of sending entries. That is an INDEX problem, handled by
   * {@link #resolveInstalledSnapshotBoundary} before this method is consulted (issue #8449).
   * <p>
   * Package-private and static so the decision is unit-testable without a live Raft cluster.
   */
  static long resolveInstalledSnapshotTerm(final long snapshotIndex, final long fallbackTerm,
      final TermIndex leaderSnapshotTermIndex) {
    if (leaderSnapshotMatches(snapshotIndex, leaderSnapshotTermIndex))
      return leaderSnapshotTermIndex.getTerm();
    return fallbackTerm;
  }

  /** The one rule for trusting the leader's marker, shared by the decision and the fallback WARNING. */
  static boolean leaderSnapshotMatches(final long snapshotIndex, final TermIndex leaderSnapshotTermIndex) {
    return leaderSnapshotTermIndex != null && leaderSnapshotTermIndex.getIndex() == snapshotIndex;
  }

  /**
   * Picks the {@link TermIndex} to REGISTER for a leader-driven snapshot install - the boundary, not just the term
   * (issue #8449).
   * <p>
   * {@code computedSnapshotIndex} is {@code firstTermIndexInLog - 1}, where {@code firstTermIndexInLog} is the
   * leader's log start S with the term its log holds there ({@code LogAppender.shouldNotifyToInstallSnapshot}).
   * After the install the follower's next index is the registered index + 1, and the leader resumes replication only
   * if {@code LogAppender.getPrevious(next)} is non-null. That method reads the leader's LOG first, and falls back to
   * the leader's latest snapshot marker only when the marker ends exactly at {@code next - 1}.
   * <p>
   * Registering S-1 is therefore right only when the leader's marker M is S-1 itself. Ratis purges whole log
   * segments, so the log start routinely stays BEFORE the marker (S &lt;= M): S-1 is then neither in the log nor the
   * marker, {@code getPrevious(S)} is {@code null}, and {@code shouldInstallSnapshot} keeps answering {@code true} -
   * the leader re-notifies, the follower answers {@code ALREADY_INSTALLED}, forever. In that case this method
   * registers S itself, with its log term: S is in the leader's log, so {@code getPrevious(S + 1)} returns exactly
   * {@code firstTermIndexInLog} and the follower's log-matching check (an exact {@code equals()}) passes. The
   * marker's own TermIndex is NOT used for an index inside the log: {@code getPrevious} would answer with the log
   * term, and a marker term can differ from it ({@code takeSnapshot} pairs the applied index with the applied term,
   * which a configuration entry can advance on its own).
   * <p>
   * The follower's data covers S: M was fetched before the database copy on every
   * {@code DatabaseReconciler.reconcileDatabasesFromLeader} path, the copy is the leader's live state, and
   * S &lt;= M. Entries S+1..M are replayed on top of the copy, as S..M already were before this change.
   * <p>
   * When M is S-1, or older, or unknown, S-1 is kept with the term {@link #resolveInstalledSnapshotTerm} picks
   * (issues #8360, #8374).
   * <p>
   * Package-private and static so the decision is unit-testable without a live Raft cluster.
   */
  static TermIndex resolveInstalledSnapshotBoundary(final TermIndex firstTermIndexInLog,
      final TermIndex leaderSnapshotTermIndex) {
    final long computedSnapshotIndex = Math.max(0L, firstTermIndexInLog.getIndex() - 1);
    if (leaderSnapshotTermIndex != null && leaderSnapshotTermIndex.getIndex() > computedSnapshotIndex)
      return firstTermIndexInLog;
    return TermIndex.valueOf(
        resolveInstalledSnapshotTerm(computedSnapshotIndex, firstTermIndexInLog.getTerm(), leaderSnapshotTermIndex),
        computedSnapshotIndex);
  }

  /**
   * A snapshot resync that {@link SnapshotResync#resolveSnapshotSource} refused before it started. A distinct type only so the
   * install path can tell it apart from a genuine installation failure in its catch chain and log it at the
   * severity its disposition deserves - the two request-driven paths return rather than throw, and never had to
   * make the distinction.
   */
  private static final class SnapshotRefusedException extends IllegalStateException {
    private final String reason;

    private SnapshotRefusedException(final String reason) {
      super(ArcadeStateMachine.SNAPSHOT_INSTALL_REFUSED + reason);
      this.reason = reason;
    }

    /** The refusal on its own, so the log line can carry a literal prefix rather than a fully formatted message. */
    String reason() {
      return reason;
    }
  }
}
