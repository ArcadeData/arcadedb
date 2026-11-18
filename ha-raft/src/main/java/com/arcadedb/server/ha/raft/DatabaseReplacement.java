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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.InstallApplyGate;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * The replacement of a local database by the leader's copy, and the install-apply gate that serialises it with the apply
 * thread (issue #7958): an entry for a database is applied either entirely before the copy is installed or after it, never
 * against a directory that is being swapped. The databases being replaced are what the leader-service gap and the
 * client gates read.
 * <p>
 * Split out of {@link ArcadeStateMachine}, whose bootstrap, resync and snapshot paths all install a copy through it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class DatabaseReplacement {
  private final ArcadeStateMachine sm;

  /**
   * One lock per database that orders a snapshot install of that database against the Raft apply thread (issue
   * #7958). The apply thread holds it while it applies an entry for the database; every install of a database this
   * node holds takes it for the whole install, download included - see {@link #installLeaderCopy}.
   * <p>
   * Without it, an install driven from anywhere but the apply thread (the full and targeted resyncs on the
   * {@code lifecycleExecutor}, the bootstrap retries, the operator resync on an HTTP worker) downloaded the leader's
   * copy as it stood when the leader served it, while the apply thread went on applying committed entries to the
   * live copy the swap was about to discard. Every entry applied in between was lost: the applied index had already
   * moved past it, so nothing applied it again, and nothing logged anything. A type created in that window existed
   * on every other node and never on this one - the {@code BoltFollowerWrite} divergence of issue #7259.
   * <p>
   * Reentrant because the apply thread itself installs from two entry types (a forced-snapshot
   * {@code INSTALL_DATABASE_ENTRY} and a mismatched bootstrap baseline) while it already holds the lock. Never
   * removed: one small lock per database name this node has ever applied an entry for.
   */
  final ConcurrentHashMap<String, InstallApplyGate> installApplyGates = new ConcurrentHashMap<>();

  /**
   * Databases this node is replacing with the leader's copy right now (issue #8491), with the number of installs of
   * each in flight. Registered by {@link #runUnderInstallGate} for the whole install, download included, and released
   * in its {@code finally}.
   * <p>
   * Not the {@link #installApplyGates} locks: the apply thread takes those too, for every entry it applies, so a held
   * gate does not mean an install is running.
   * <p>
   * What it is for: a node can be ELECTED while it is replacing a database (Raft compares logs only, and an operator
   * or targeted resync leaves the log complete). A leader cannot install from itself, and every client request on
   * that database is refused while its copy is being replaced (issue #8363), so the cluster rejects every write to it
   * with a healthy majority. {@link ArcadeStateMachine#handOffLeadershipWhileReplacingDatabase()} reads this set to hand leadership to
   * a peer that holds the data, and {@code ClusterAlerts} reads it to report the condition. {@link ArcadeStateMachine#startTransaction}
   * reads it too, to refuse reserving page versions against a copy being replaced (issue #8022).
   */
  final ConcurrentHashMap<String, Integer> databasesBeingReplaced = new ConcurrentHashMap<>();

  DatabaseReplacement(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * The install lock of {@code dbName}, taken by the apply thread before it applies an entry for that database
   * (issue #7958), or {@code null} for an entry that targets no database. An install of the database holds it for
   * as long as the install runs, and the entry waits for the installed copy rather than going to the one being
   * replaced - logged once per wait, so a replication pause while a snapshot downloads has a line that explains it.
   */
  InstallApplyGate enterInstallApplyGate(final String dbName, final long index) {
    if (dbName == null)
      return null;
    final InstallApplyGate gate = installApplyGate(dbName);
    if (!gate.tryLock()) {
      LogManager.instance().log(sm, Level.INFO,
          "Applying entry %d for database '%s' waits for the snapshot install replacing this node's copy of it: the "
              + "copy the install replaces must not receive entries its replacement does not carry", index, dbName);
      final Consumer<String> waiting = ArcadeStateMachine.applyWaitsForInstallForTesting;
      if (waiting != null)
        waiting.accept(dbName);
      gate.lock();
    }
    return gate;
  }

  /**
   * Whether an entry of {@code type} at or below the applied index a leader served the installed copy at has nothing
   * left to do on this node (issue #8579). Only the entries whose whole effect is the database's own files qualify: a
   * transaction and a schema change. Every other type that names a database also acts on this node's own state (the
   * bootstrap baselines and the readiness holders they release, the install and drop bookkeeping), which a copy of the
   * database's files does not carry. An entry this node originated is always applied: its apply publishes a commit a
   * local caller is waiting on. So is every entry while ANY local commit is pending, whatever database it is for: a
   * coarse test, but one that can only fall back to applying the entry again, never skip one it should not. It is also
   * why skipping a transaction's whole apply - its local-commit claim and page-version release included - leaves
   * nothing behind: a claim needs a pending local commit, and the page-version reservations of this database were
   * cleared by the install that recorded the boundary ({@code runUnderInstallGate}).
   * <p>
   * SAFETY INVARIANT: leaving an entry out is correct only because the leader reads the applied index it reports
   * BEFORE it captures the copy it serves, and refuses to serve a database it has quarantined (issues #8454, #8468,
   * see {@code SnapshotHttpHandler}). A change there that let the reported index run ahead of the copy would turn this
   * skip into a silent loss of the entries in between.
   */
  boolean isCarriedByInstalledCopy(final RaftLogEntryType type, final boolean originatedLocally) {
    if (originatedLocally || sm.pendingLocalCommits() > 0)
      return false;
    // A database's configuration.json is a file of the database's own too, archived with the copy and read back when
    // the installed copy is opened (issue #9763)
    return type == RaftLogEntryType.TX_ENTRY || type == RaftLogEntryType.SCHEMA_ENTRY
        || type == RaftLogEntryType.DATABASE_CONFIGURATION_ENTRY;
  }

  /**
   * Whether the current thread holds the install lock of any database (issue #7958): it is then running an install
   * that may have the apply thread waiting on it, so it must not wait on the apply thread in turn.
   */
  boolean isHoldingInstallApplyGate() {
    for (final InstallApplyGate gate : installApplyGates.values())
      if (gate.isHeldByCurrentThread())
        return true;
    return false;
  }

  // Package-private for tests that hold the apply thread off a database WITHOUT the install's "being replaced"
  // registration, which a leader answers by refusing every transaction on it (Issue8454SnapshotSourceBehindFollowerIT).
  InstallApplyGate installApplyGate(final String dbName) {
    return installApplyGates.computeIfAbsent(dbName, name -> new InstallApplyGate());
  }

  /**
   * Replaces this node's copy of {@code dbName} with the leader's, with the apply thread held off the database for
   * the whole install (issue #7958). Every install of a database this node holds goes through here.
   * <p>
   * The lock is taken BEFORE the download starts, not around the swap: the leader serves the snapshot as of the
   * moment it receives the request, so what has to be excluded is every entry this node would otherwise apply
   * between that moment and the swap. Holding it from the start means everything this node applied to the old copy
   * was applied before it asked for the new one; the entries committed after that wait, and are applied to the
   * installed copy once the lock is released. An entry that is ALSO already in the leader's copy - at or below the
   * applied index the leader reported serving it at - is not applied a second time (issue #8579): it only advances
   * the applied position, see {@link InstallApplyGate#servedCopyIndex()}.
   * <p>
   * The cost is that replication of this database pauses for as long as the install runs, download retries
   * included, and every database behind it with it, since the apply thread is shared - the same cost the installs
   * that already ran ON the apply thread have always had. An install that fails leaves the old copy in place and the
   * waiting entries are applied to it, which is correct: nothing replaced it.
   */
  void installLeaderCopy(final String dbName, final Supplier<String> leaderHttpAddr,
      final Supplier<String> leaderHttpsAddr, final String clusterToken) throws IOException {
    installLeaderCopy(dbName, leaderHttpAddr, leaderHttpsAddr, clusterToken, null);
  }

  /**
   * As {@link #installLeaderCopy(String, Supplier, Supplier, String)}, first running {@code underGate} (when not
   * {@code null}) with the install lock held and before anything is downloaded, fed the highest index applied to this
   * database under that lock. It may throw to abandon the install with the local copy untouched (issue #8490).
   * <p>
   * Not tied to a Raft install boundary (issue #8577): a targeted single-database resync is not the leader's
   * compacted log catching this node up, so nothing moves the applied position here. The entries waiting behind the
   * gate that the installed copy already carries are recognised by the applied index the leader served it at instead
   * (issue #8579), exactly as {@link #installLeaderCopy} describes.
   */
  void installLeaderCopy(final String dbName, final Supplier<String> leaderHttpAddr,
      final Supplier<String> leaderHttpsAddr, final String clusterToken, final LongConsumer underGate) throws IOException {
    // Read once: the field is volatile, and the path and the install must be about the same server.
    final ArcadeDBServer localServer = sm.server;
    runUnderInstallGate(dbName, underGate, -1L, () -> SnapshotInstaller.install(dbName,
        SnapshotInstaller.resolveDatabasePath(localServer, dbName), leaderHttpAddr, leaderHttpsAddr, clusterToken,
        localServer));
  }

  /**
   * Runs {@code install} holding {@code dbName}'s install lock, as {@link #installLeaderCopy} describes. Also the
   * {@link DatabaseReconciler.InstallGate} of the Ratis-initiated installs: Ratis pauses the state machine only after
   * {@link ArcadeStateMachine#notifyInstallSnapshotFromLeader} has completed, so the apply thread can still be applying the entries
   * below the snapshot while the reconciler replaces the copy they target.
   */
  void runUnderInstallGate(final String dbName, final DatabaseReconciler.InstallAction install) throws IOException {
    runUnderInstallGate(dbName, null, -1L, install);
  }

  /**
   * As {@link #runUnderInstallGate(String, DatabaseReconciler.InstallAction)}, and - when {@code installedBoundaryIndex}
   * is {@code >= 0} - records it on the database's install lock before releasing it (issue #8577): the boundary a
   * leader-driven snapshot install just brought this database's copy to, safe to use even before the install's exact
   * registered {@link TermIndex} is known, because it is always {@code <=} that final value (see
   * {@link LeaderSnapshotInstall#resolveInstalledSnapshotBoundary}). {@link DatabaseReconciler.InstallGate}'s wiring for the
   * Ratis-initiated install path.
   */
  void runUnderInstallGate(final String dbName, final long installedBoundaryIndex,
      final DatabaseReconciler.InstallAction install) throws IOException {
    runUnderInstallGate(dbName, null, installedBoundaryIndex, install);
  }

  void runUnderInstallGate(final String dbName, final LongConsumer underGate, final long installedBoundaryIndex,
      final DatabaseReconciler.InstallAction install) throws IOException {
    // Registered for the whole install (issue #8491): from here until the finally below this node is replacing its copy
    // and cannot serve the database, so it must not stay leader - see databasesBeingReplaced.
    beginDatabaseReplacement(dbName);
    try {
      // The pre-install Raft log purge (issue #7037) asks Ratis for a snapshot, which the apply thread serves - and the
      // apply thread may be about to wait on the lock taken below. Purged here, before the lock, so it is not left
      // timing out against it; the install's own attempt then stands down (isHoldingInstallApplyGate()).
      final RaftHAServer raftHA = sm.raftHAServer;
      if (raftHA != null)
        raftHA.compactRaftLogBeforeSnapshotInstall(dbName);
      final InstallApplyGate gate = installApplyGate(dbName);
      gate.lock();
      try {
        if (underGate != null)
          underGate.accept(gate.appliedUnderGate());
        // The lock excludes the entries this node would apply AFTER it asked for the leader's copy; it says nothing
        // about the ones it applied BEFORE, which the copy has to carry because nothing will apply them again. The
        // leader publishes an entry's pages on its own apply thread, so it can serve a copy that is still behind an
        // entry this node has already applied (issue #8454). The install refuses such a copy and asks again: every
        // entry up to this floor went to the copy being replaced.
        final long floor = Math.max(gate.appliedUnderGate(), sm.lastAppliedIndex.get());
        final long servedAt = SnapshotInstaller.runRequiringSourceAppliedIndex(floor, install);
        // The install replaced the database's files, so no reservation taken against the previous copy can still hold
        // (issue #8022) - the rule applyDropDatabaseEntry already applies, here for every install path: the forced
        // reinstall of applyInstallDatabaseEntry (which clears nothing itself, issue #8598) and the targeted resync of
        // a quarantined database included. Success only and still under the lock:
        // a failed install leaves the old copy, and the entries behind the reservations are still to be applied to it.
        // Nothing in flight is dropped: reservations are taken only by a leader, a leader cannot install from itself,
        // so this node stepped down (clearing its ledger) before the download could succeed, and a node elected
        // mid-install takes no reservation on this database until the replacement ends (see startTransaction).
        sm.pageVersions.clear(dbName);
        // Recorded on success only, and still under the lock (issue #8577): a failed install leaves the old copy in
        // place, so the waiting entries must still be applied to it, not skipped. Recording here - before the
        // unlock below - is what makes the boundary visible to every entry that re-acquires this gate afterward,
        // whether it was already blocked behind it or arrives later.
        if (installedBoundaryIndex >= 0)
          gate.recordInstalled(installedBoundaryIndex);
        // Every install path, whatever started it (issue #8579): the operator and #8490 targeted resyncs, the full
        // resync, the bootstrap-mismatch retry and the Ratis-initiated one alike. Success only and before the unlock,
        // for the same reasons as the boundary above; -1 when the install downloaded nothing or the leader did not say.
        gate.recordServedCopy(servedAt);
      } finally {
        gate.unlock();
      }
    } finally {
      endDatabaseReplacement(dbName);
    }
  }

  /** Registers one more install of {@code dbName} in flight on this node (issue #8491). */
  private void beginDatabaseReplacement(final String dbName) {
    databasesBeingReplaced.merge(dbName, 1, Integer::sum);
  }

  /** Releases one registration taken by {@link #beginDatabaseReplacement}. */
  private void endDatabaseReplacement(final String dbName) {
    databasesBeingReplaced.computeIfPresent(dbName, (name, count) -> count <= 1 ? null : count - 1);
  }

  /**
   * The databases this node is replacing with the leader's copy right now (issue #8491), sorted. Allocation-free when
   * there are none, which is every call on a healthy node: {@code ClusterAlerts} reads it on every status poll.
   */
  public List<String> getDatabasesBeingReplaced() {
    if (databasesBeingReplaced.isEmpty())
      return Collections.emptyList();
    final List<String> names = new ArrayList<>(databasesBeingReplaced.keySet());
    Collections.sort(names);
    return names;
  }

  /** {@link #installLeaderCopy(String, Supplier, Supplier, String)} with addresses resolved once, by the caller. */
  void installLeaderCopy(final String dbName, final String leaderHttpAddr, final String leaderHttpsAddr,
      final String clusterToken) throws IOException {
    installLeaderCopy(dbName, () -> leaderHttpAddr, () -> leaderHttpsAddr, clusterToken);
  }
}
