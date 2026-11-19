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

import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.BootstrapBaseline;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Applies the entries that settle each database at cluster formation - BOOTSTRAP_FINGERPRINT and INSTALL_DATABASE - and
 * installs the leader's copy where the local one does not match the fingerprint the cluster agreed on. An install that
 * fails is retried, and a replacement still owed is remembered across restarts until the leader's copy is on disk.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which dispatches each entry to the class that applies it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BootstrapInstaller {
  private final ArcadeStateMachine sm;

  /**
   * Databases whose directory {@link #installFromLeaderForBootstrap} is replacing from the leader's snapshot
   * right now (issue #7519).
   * <p>
   * The window it names is the one the bootstrap protocol opens on every peer that did not source the baseline:
   * from the moment this node decides its copy is not the cluster's, until the leader's copy is on disk. For
   * most of it the local copy is still OPEN and SERVING - {@code SnapshotInstaller.install} downloads before it
   * touches the live files, deliberately, so a failed download costs no availability - and the node's own
   * {@code snapshotInstallInProgress} 503 window covers only the file swap at the end of it, and only on HTTP.
   * So a client reaching this node during the download is served from a copy the cluster has already decided
   * against, on every protocol, with nothing anywhere on the request path saying so.
   * <p>
   * Registered by {@link #installFromLeaderForBootstrap} around the install and read by
   * {@link ArcadeStateMachine#bootstrapWindowReason()}, which is what takes the node out of the Kubernetes Service for the
   * duration.
   * <p>
   * <b>A depth per database, not a set</b> (review of PR #7964), and for the same reason
   * {@link SnapshotInstaller}'s own {@code INSTALLS_IN_FLIGHT} is one: with a set, two overlapping installs of
   * the same database would have the FIRST {@code finally} to run drop the name while the second was still
   * moving files, and the node would report itself ready in the middle of a directory replacement - silently,
   * with no log line and nothing a test would catch. The four production callers of
   * {@code installFromLeaderForBootstrap} sit on two single-threaded executors (the Ratis apply thread for the
   * two {@code applyBootstrapFingerprintEntry} arms, the {@code lifecycleExecutor} for
   * {@code retryBootstrapInstall} and {@code retryMissingBootstrapDatabase}), so neither pair can race itself
   * and an apply/lifecycle overlap needs a replayed baseline to meet a live retry for the same database. That
   * is narrow rather than impossible, and proving it impossible across two pools is worth less than the six
   * lines that make it not matter.
   */
  final ConcurrentHashMap<String, Integer> bootstrapInstallsInFlight = new ConcurrentHashMap<>();

  // Wall-clock of the last pending-bootstrap-replacement retry claimed by retryPendingBootstrapReplacements(); 0 = none
  // yet. Throttles the HealthMonitor-driven retry to one attempt per snapshot watchdog window, like the other
  // snapshot backstops: each attempt is a full download of the database from the leader.
  private final AtomicLong lastBootstrapReplacementRetryMs = new AtomicLong();

  BootstrapInstaller(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  // @VisibleForTesting
  void applyInstallDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex) {
    final String databaseName = decoded.databaseName();
    final boolean forceSnapshot = decoded.forceSnapshot();
    // No ledger clear here: runUnderInstallGate clears it when the follower's reinstall actually replaces the files
    // (#8022). Clearing on the arms that replace nothing dropped live reservations of later entries (#8598).

    if (forceSnapshot) {
      // Replay guard (issue #7143). Ratis re-feeds every entry between the last snapshot marker and
      // shutdown, and unlike the normal-create branch below the force branch has no existence check to
      // stop it - existence is precisely what it ignores. So without this a restart re-downloaded and
      // re-installed the whole database on every replay: correct in outcome, but a node restarting
      // repeatedly re-pulls a multi-GB database on each attempt, lengthening every start and competing
      // for the bandwidth the cluster needs to recover. The in-place Ratis restart (restartRatisIfNeeded,
      // one per health-monitor tick) can repeat that without the process ever exiting.
      //
      // The same per-database evidence applyBootstrapFingerprintEntry uses: a persisted applied index at
      // or beyond this entry proves a previous session ran this install to completion and that Raft has
      // since replicated this database forward from there. It is deliberately PER-DATABASE - one state
      // machine multiplexes every database, so a co-located database that advanced the global index must
      // not suppress this one's reinstall (issue #4824) - and a legacy plain-number applied-index file
      // yields -1, which re-installs exactly as before.
      //
      // Both halves of that evidence are statements about a PREVIOUS session, and neither says the database
      // is here NOW (issue #7221). The index lives in <databaseDirectory>/.raft/applied-index, a sibling of
      // the per-database directories rather than a file inside them, so deleting one database's directory
      // leaves its entry in the map intact. The wipe-and-resync recovery an operator reaches for when a
      // follower's copy is bad - stop the node, delete the copy, start it again - then hit a guard that
      // skipped the reinstall and a log line claiming a reinstall the filesystem contradicted. So the skip
      // also requires the database to be registered here, the same question the normal-create arm below
      // asks; a node whose registry has no such database re-downloads, as it did before #7143.
      //
      // The registry, not the filesystem, is what is consulted - so the wording below says "registered"
      // rather than "present", which is the check that actually ran. A database dropped through Raft is
      // not the case this re-opens: applyTransaction routes a DROP_DATABASE_ENTRY through
      // writePersistedAppliedIndexDroppingDatabase, which evicts the per-database entry, so the read
      // below already returns -1 for a dropped database and the skip was never reachable for one.
      //
      // Both volatile collaborators are read once into locals and used from there for the rest of this
      // forceSnapshot branch (the normal-create arm below is untouched and keeps reading the field).
      // createStateMachine() (RaftHAServer:1416-1421) is the single production wiring point and sets the two on
      // consecutive lines, so "server is null" and "raftHAServer is null" are the same not-yet-wired state
      // rather than two independent ones - which is why they get the same treatment here instead of one being
      // captured and the other re-read.
      //
      // What keeps SnapshotInstaller.resolveDatabasePath below from seeing a null server is safe publication,
      // NOT a chain of volatile reads: this arm reads server BEFORE it reads raftHAServer, and observing the
      // later-written field non-null says nothing about a read that already happened, so that argument would
      // not hold. The one that does: createStateMachine() sets both fields on the machine before the reference
      // escapes it (RaftHAServer:1416-1421, assigned at :384 and :1478), so Ratis has no state machine to call
      // applyTransaction on until both writes are done. The precondition is now written down on
      // resolveDatabasePath itself, since six other call sites lean on it without saying so.
      final ArcadeDBServer localServer = sm.server;

      // Restore flow: replace files from the leader's snapshot even if the DB exists. The leader's own files are
      // already authoritative, so the leader skips the reinstall; replicas close their local copy and pull the
      // fresh snapshot from the leader.
      //
      // Checked FIRST, ahead of the replay guard below, because a leader never reinstalls from itself whatever the
      // guard decides, and the guard's own WARNING announces a reinstall from the leader. Logged before the skip,
      // that line recorded an action that never happened - on the node whose log an operator reads to find out what
      // the cluster did with the entry (issue #7302). The one case the guard's WARNING exists for - applied before,
      // absent now - is asserted inside the leader arm instead, below (issue #8067).
      //
      // The volatile field is read ONCE into a local. resolveSnapshotSource guards a null HA server and refuses
      // cleanly, but evaluating raftHAServer.getLeaderId() as its ARGUMENT dereferenced the field before that
      // guard could run, so the refusal it exists to produce arrived as a NullPointerException instead; the
      // same held for getClusterToken() below. A null local yields a null leader id, which
      // PeerDialAddress.resolve refuses as "the leader is unknown".
      //
      // Null is reachable, not hypothetical: no production caller nulls the field (grep for setRaftHAServer -
      // RaftHAServer:1419 is the only one), but it starts null and a state machine that has not been rewired
      // yet still carries null. Forgetting exactly that rewire on the recovery path is the regression
      // HaInPlaceRestartRecoveryIT.restartRewiresStateMachineRaftHAServer exists to catch.
      //
      // The skip ASSERTS its premise rather than assuming it (issue #8067, the sibling of #7901). "The leader's own
      // files are authoritative" says nothing about a leader that has no files: a node that applied this entry in a
      // previous session, lost the database directory and came back as the leader used to return here at TRACE,
      // ahead of the replay guard's "applied before but not registered now" arm - no install, no durable mark, no
      // alert. Throwing routes it into the per-database quarantine (handleUnexpectedApplyError), which is durable,
      // raises the alert, hands the leadership off, and reinstalls the database from the next leader through the
      // targeted resync.
      //
      // Both halves of the replay guard's evidence are required, not just the absence. "Present" is registered OR on
      // disk (isDatabasePresentLocally): a leader whose copy is merely closed still holds the authoritative files.
      // And the per-database applied index must show a previous session applied this entry: a DROP evicts that
      // index, so a missing database with no record is one the cluster dropped later in the log, and replaying the
      // older install on the leader stays the no-op it always was.
      final RaftHAServer raftHA = sm.raftHAServer;
      if (raftHA != null && raftHA.isLeader()) {
        if (!sm.isDatabasePresentLocally(databaseName)) {
          final long appliedBefore = sm.appliedIndexes.readPersistedAppliedIndex(databaseName);
          if (appliedBefore >= entryIndex && raftHA.isSoleVoter()) {
            // A quarantine is lifted by the leadership hand-off and the targeted resync from the next leader. A sole voter
            // has neither, so the quarantine would be permanent, and a permanent quarantine takes the whole node out of the
            // ready set and stops the Raft log from being checkpointed for a database only the operator can restore
            // (issue #8940). Alert in the log instead and keep serving.
            LogManager.instance().log(sm, Level.SEVERE,
                "Database '%s' was reinstalled by the forceSnapshot entry at index %d in a previous session but has no copy "
                    + "on this node now, and this node is the only voter, so there is no peer to reinstall it from. It is "
                    + "NOT quarantined (that could never be lifted here and would keep the node out of service): restore "
                    + "the database directory from a backup, or drop the database", databaseName, entryIndex);
          } else if (appliedBefore >= entryIndex)
            throw new IllegalStateException("Database '" + databaseName + "' was reinstalled by the forceSnapshot entry "
                + "at index " + entryIndex + " in a previous session (persistedAppliedIndex=" + appliedBefore
                + ") but has no copy on this node now, and this node is the Raft leader, so there is nowhere to "
                + "reinstall it from. Quarantining it and handing the leadership off so it is reinstalled from the next "
                + "leader. If the leadership stays here, transfer it (POST /api/v1/cluster/leader) to a node that "
                + "holds the database, then force the install here (POST /api/v1/cluster/resync/" + databaseName + ")");
        }
        HALog.log(sm, HALog.TRACE, "Leader skips forceSnapshot reinstall for '%s'", databaseName);
        return;
      }

      final long persistedApplied = sm.appliedIndexes.readPersistedAppliedIndex(databaseName);
      if (persistedApplied >= entryIndex) {
        if (localServer != null && localServer.existsDatabase(databaseName)) {
          LogManager.instance().log(sm, Level.INFO,
              "Database '%s' already reinstalled by this entry in a previous session (persistedAppliedIndex=%d >= "
                  + "entryIndex=%d) and is registered on this node; skipping the snapshot re-download",
              databaseName, persistedApplied, entryIndex);
          return;
        }
        LogManager.instance().log(sm, Level.WARNING,
            "Database '%s' was reinstalled by this entry in a previous session (persistedAppliedIndex=%d >= "
                + "entryIndex=%d) but is not registered on this node now; reinstalling it from the leader",
            databaseName, persistedApplied, entryIndex);
      }

      // Same refusals as every other path that pulls a snapshot, through the same helper (issue #6202): a
      // derived address that names this node would "restore" the local copy from itself and report success,
      // which is worse than the failure the caller already handles below.
      final PeerDialAddress source = sm.resync.resolveSnapshotSource(raftHA != null ? raftHA.getLeaderId() : null);
      if (source.refused())
        throw new RuntimeException("Cannot reinstall database '" + databaseName + "' from the leader: "
            + source.refusal());

      final String leaderHttpAddr = source.httpAddress();
      // The guard's own HTTPS endpoint rather than the raw resolver's: it is declared and derived independently
      // of the HTTP one, so the HTTP verdict does not cover it (issue #6221). Null falls back to plain HTTP.
      final String leaderHttpsAddr = source.httpsAddress();
      final String clusterToken = raftHA != null ? raftHA.getClusterToken() : null;
      try {
        // install() keeps the database open during the download and rolls back on failure, so a
        // failed restore never leaves it closed.
        sm.replacement.installLeaderCopy(databaseName, leaderHttpAddr, leaderHttpsAddr, clusterToken);
      } catch (final IOException e) {
        throw new RuntimeException("Failed to install snapshot for restored database '" + databaseName + "'", e);
      }
      // A copy of the leader's now, not the one the previous session left (issue #11946)
      sm.appliedIndexes.forgetPositionOfPreviousSession(databaseName);
      LogManager.instance().log(sm, Level.INFO, "Database '%s' reinstalled via forceSnapshot from leader", databaseName);
      sm.baselines.clearBootstrapUnreconciled(databaseName);
      settleBootstrapReplacement(databaseName);
      return;
    }

    // Normal create flow: skip if the database is already present locally.
    if (sm.server.existsDatabase(databaseName)) {
      HALog.log(sm, HALog.TRACE, "Database '%s' already present, skipping install-database entry", databaseName);
      return;
    }

    sm.server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
    // A new copy: the position the previous session reached on a database of this name was another copy's (issue #11946)
    sm.appliedIndexes.forgetPositionOfPreviousSession(databaseName);
    LogManager.instance().log(sm, Level.INFO, "Database '%s' created via Raft install-database entry", databaseName);
  }

  /**
   * Apply a {@link RaftLogEntryType#BOOTSTRAP_FINGERPRINT_ENTRY} on this peer (issue #4147 phase 5).
   * <p>
   * The committed entry names the peer chosen as the bootstrap source for {@code dbName} and
   * carries that source's {@code (fingerprint, lastTxId)}. Each peer compares its local state
   * against the committed baseline and decides:
   * <ul>
   *   <li><b>Match</b> (fingerprint and lastTxId both equal) - bootstrap locally, no bytes
   *       transfer, the database files on disk are already correct.</li>
   *   <li><b>Late newer joiner</b> (local lastTxId &gt; committed lastTxId) - this peer's data
   *       is fresher than the cluster's chosen baseline. We refuse to silently overwrite it and
   *       log a SEVERE pointing the operator at the recovery procedure.</li>
   *   <li><b>Mismatch</b> (any other case) - reinstall from the leader-shipped full snapshot.
   *       Subsequent transactions are picked up by native Ratis AppendEntries; no special
   *       transaction-delta path is needed because at first formation the Ratis log is empty.</li>
   * </ul>
   * The committed baseline is recorded in {@link BootstrapBaselines#bootstrapBaselines} for status export and tests.
   * <p>
   * Two rules keep the protocol honest on a cluster that starts taking writes while it is being born (issue
   * #7011: the formation-time election samples whatever local databases exist at collect time, and the
   * application typically creates and seeds its databases right after leader election):
   * <ul>
   *   <li><b>Superseded baseline.</b> A database that an application entry earlier in the log already
   *       created or mutated on this node has its whole history inside the Raft log: the baseline sampled for
   *       it is stale by construction and replication, not bootstrap, is what keeps the copies in step. The
   *       entry is ignored for that database. The decision keys on this database's persisted applied index
   *       being below the entry's index. That index is NOT guaranteed to be the same on every peer: it is also
   *       written outside log order, by {@link ArcadeStateMachine#writePersistedAppliedIndexForAllDatabases} after a snapshot
   *       install, and it reads as {@code -1} for every database on the first restart after upgrading from a
   *       legacy plain-number {@code applied-index} file. A peer reading a higher value takes the replay-skip
   *       below, and a peer reading {@code -1} re-runs the verification, whose "local is fresher" arm logs a
   *       SEVERE rather than diverging silently (both walked in issue #7958).</li>
   *   <li><b>Bootstrap source.</b> The peer that committed the entry sampled the baseline from its own copy,
   *       which is the copy the snapshot ships to everyone else. Its copy advancing past the sampled
   *       {@code lastTxId} between the sample and the local apply (18 ms in the report) is the expected
   *       outcome, not a fresher stray copy, so the overwrite refusal does not apply to it: the refusal is
   *       only meaningful on a peer that did not source the baseline.</li>
   * </ul>
   * Neither rule is reachable on a genuine first formation - no application entry precedes the baseline
   * there, and the source's copy equals the baseline - so the #4800 and #6124 guarantees are unchanged.
   * <p>
   * Package-private (not private) so ArcadeStateMachineBootstrapMismatchTest can exercise the
   * install-failure recovery path directly instead of via reflection.
   *
   * @param originatedLocally whether this node submitted the entry, i.e. is the elected bootstrap source
   *                          (see {@link ArcadeStateMachine#startTransaction}); {@code false} on replay, where the
   *                          per-database replay-skip below settles the question instead. The marker is set
   *                          only on the node whose own client submitted the request, which is always the
   *                          source: {@code BootstrapElection} never commits on behalf of a remote source -
   *                          it transfers leadership to it and that node commits the entry once it leads
   */
  // @VisibleForTesting
  void applyBootstrapFingerprintEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index,
      final boolean originatedLocally) {
    try {
      decideBootstrapFingerprintEntry(decoded, index, originatedLocally);
    } finally {
      // Issue #8368: the baseline has reached this node, so the pass has settled this database here, whatever the
      // decision was. Released only now, after the decision: a copy being replaced is held by the install holder
      // from inside the decision on, so the hold is handed over rather than dropped and retaken.
      if (decoded.databaseName() != null)
        sm.bootstrapPasses.bootstrapPassesPending.remove(decoded.databaseName());
    }
  }

  private void decideBootstrapFingerprintEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index,
      final boolean originatedLocally) {
    final String dbName = decoded.databaseName();
    final String chosenFingerprint = decoded.bootstrapFingerprint();
    final long chosenLastTxId = decoded.bootstrapLastTxId();
    if (dbName == null || chosenFingerprint == null) {
      LogManager.instance().log(sm, Level.WARNING,
          "BOOTSTRAP_FINGERPRINT_ENTRY missing required fields, skipping (db=%s, fp=%s)",
          dbName, chosenFingerprint);
      return;
    }

    final long persistedApplied = sm.appliedIndexes.readPersistedAppliedIndex(dbName);
    if (persistedApplied >= 0 && persistedApplied < index) {
      // Superseded (issue #7011): an application entry for this database was applied before this baseline was
      // committed, so the database was created or mutated through Raft on this cluster and its copies are
      // kept in step by replication. Honouring the baseline would either refuse the copy as "fresher" (it is
      // not: it is the replicated state) or reinstall it from a snapshot the following entries then re-apply.
      // The baseline is not recorded: it describes a state this cluster never adopted.
      LogManager.instance().log(sm, Level.INFO,
          "Bootstrap baseline for '%s' (lastTxId=%d) ignored: the database already has Raft history on this node "
              + "(applied index %d precedes the baseline at index %d), so replication keeps it in step",
          dbName, chosenLastTxId, persistedApplied, index);
      return;
    }

    sm.baselines.recordBootstrapBaseline(dbName, new BootstrapBaseline(chosenFingerprint, chosenLastTxId));

    // Re-application during log replay on restart: if we've persisted an applied index at or
    // beyond this entry's index, the verification ran in a prior session and the local database
    // has since been forward-replicated past the baseline by Ratis AppendEntries. Re-running
    // the install path here would race leader-discovery (the StateMachineUpdater thread is
    // inside applyTransaction and blocks Ratis leader-info notifications), exhaust the snapshot
    // retry budget with null leader addresses, and trip the critical-error halt.
    // This is a PER-DATABASE decision, so it must consult THIS database's applied index, not the
    // global one: one ArcadeStateMachine multiplexes every database, and a co-located database that
    // advanced the global index past this entry must not suppress this database's verification
    // (issue #4824). Absent positive per-database evidence the verification re-runs, which is
    // idempotent (a fingerprint match returns immediately without moving any bytes).
    //
    // Upgrade note: a legacy plain-number applied-index file carries no per-database breakdown, so on
    // the FIRST restart after upgrading, this read returns -1 for every database and verification
    // re-runs for any bootstrap entry still above the latest Ratis snapshot. That is a bounded,
    // one-time cost and is safe: a matching local fingerprint returns immediately; a locally-fresher
    // copy (local lastTxId > baseline) hits the "refusing to overwrite local data" guard below (no
    // data loss, just a SEVERE log line); a genuinely-behind copy re-installs from the leader, which
    // is the correct action anyway. From the first post-upgrade apply onwards the per-database map is
    // authoritative.
    // The premise of the skip below is asked BEFORE the skip, not one line after it (issue #7298). The sibling
    // guard in applyInstallDatabaseEntry had the identical defect and #7221 fixed it there only: a persisted
    // applied index says a PREVIOUS session applied this entry, and says nothing about whether its effect is
    // still on this node. The index lives in <databaseDirectory>/.raft/applied-index, a sibling of the
    // per-database directories rather than a file inside them, so the wipe-and-resync recovery this repo's own
    // runbook prescribes - stop the follower, delete the bad copy, start it again - leaves the entry intact
    // while the database it describes is gone.
    final boolean registeredLocally = sm.server.existsDatabase(dbName);

    if (persistedApplied >= index && registeredLocally) {
      HALog.log(sm, HALog.BASIC,
          "Bootstrap baseline for '%s' already applied (persistedAppliedIndex=%d >= entryIndex=%d) and the database is "
              + "registered on this node; skipping verification",
          dbName, persistedApplied, index);
      return;
    }

    // Issue #9568: a node shutting down does not verify the baseline against a database that is not open here. Every arm
    // below would act on the closed copy wrongly: reading the local state reopened it, a failed read fell back to a leader
    // install, and a database the shutdown already deregistered read as "not here" - either reinstalled from the leader
    // or recorded as a late joiner, which advances this database's applied index past the entry so the verification
    // never runs again. The entry is left for the replay on restart instead, like every entry #9550 covers. After the
    // replay-skip above, which touches no database, and asked in this order so a running node pays the shutdown probe
    // only for a database it does not have open. isDatabaseOpenHere() answers "open" when it cannot tell, which skips
    // this check; the catch around readLocalBootstrapState() below still classifies a failed read the same way.
    if (!sm.isDatabaseOpenHere(dbName) && sm.isNodeShuttingDown())
      throw sm.leaveForReplay(index, dbName, new DatabaseIsClosedException(
          "Database '" + dbName + "' is not open and this node is shutting down, so the bootstrap baseline is not "
              + "verified against it"));

    if (!registeredLocally) {
      if (persistedApplied >= index) {
        // The database WAS here - a previous session applied this very entry against it - and is not here now.
        // Falling through to the "late joiner" arm below would be wrong for a second reason beyond the log line:
        // that arm waits for a follow-on INSTALL_DATABASE_ENTRY, and a bootstrap-baselined database has none in
        // the log. It predates the cluster; the baseline entry is precisely the record of a database that was
        // never created through Raft. So nothing else in the log brings it back, and the node would keep running
        // permanently short of a database the cluster believes it has. Pull it from the leader, which is the
        // action #7221's own fix takes in the same situation on the install path.
        LogManager.instance().log(sm, Level.WARNING,
            "Bootstrap baseline for '%s' was applied in a previous session (persistedAppliedIndex=%d >= entryIndex=%d) "
                + "but the database is not registered on this node now; reinstalling it from the leader",
            dbName, persistedApplied, index);
        installFromLeaderForBootstrapWithRetry(dbName, false);
        return;
      }

      // Late joiner with no local copy of this database, and no evidence it ever had one. The follow-on
      // INSTALL_DATABASE_ENTRY (or natural Raft replay) will create the database and install the leader's
      // snapshot; we just record the baseline.
      LogManager.instance().log(sm, Level.INFO,
          """
          Bootstrap baseline recorded for '%s' (lastTxId=%d); database not yet present locally, \
          will be created via leader-shipped snapshot""",
          dbName, chosenLastTxId);
      return;
    }

    // Compute local state.
    final BootstrapBaseline local;
    try {
      local = sm.readLocalBootstrapState(dbName);
    } catch (final Exception e) {
      // Closed under the apply thread after the check above (issue #9568): a closed copy on a node going away is not a
      // reason to download the leader's one. Left for the replay on restart, which verifies it then.
      if (sm.isClosedUnderShutdown(dbName, e))
        throw sm.leaveForReplay(index, dbName, e);
      LogManager.instance().log(sm, Level.WARNING,
          "Could not read local bootstrap state for '%s': %s; falling back to leader-shipped full snapshot",
          dbName, e.getMessage());
      installFromLeaderForBootstrap(dbName);
      return;
    }
    if (local == null) {
      LogManager.instance().log(sm, Level.WARNING,
          "BOOTSTRAP_FINGERPRINT_ENTRY for '%s': embedded database is not a LocalDatabase, skipping",
          dbName);
      return;
    }
    final String localFingerprint = local.fingerprint();
    final long localLastTxId = local.lastTxId();

    // Match: bootstrap locally, no bytes move.
    if (localLastTxId == chosenLastTxId && chosenFingerprint.equals(localFingerprint)) {
      LogManager.instance().log(sm, Level.INFO,
          "Database '%s' bootstrapped locally (lastTxId=%d, fingerprint matches cluster baseline)",
          dbName, chosenLastTxId);
      return;
    }

    // The bootstrap source (issue #7011): this node committed the entry, so the baseline IS its own copy as it
    // stood at sampling time, and everyone else's snapshot comes from this copy. Writes accepted between the
    // sample and this apply legitimately move it past the baseline; refusing it here poisoned the leader of a
    // cluster that seeded its databases right after election, and the cluster never converged.
    if (originatedLocally) {
      if (localLastTxId >= chosenLastTxId)
        LogManager.instance().log(sm, Level.INFO,
            "Database '%s' is the bootstrap source on this node (local lastTxId=%d, baseline lastTxId=%d): the local copy "
                + "is the baseline, nothing to verify",
            dbName, localLastTxId, chosenLastTxId);
      else
        // A committed transaction id never moves backwards, so the source's copy cannot sit behind the baseline it
        // sampled from that same copy. Should it ever happen (a database replaced under the running server), the
        // copy is still the one every other peer installs from: keep it and say so, rather than fall through to
        // the mismatch branch and have this node reinstall from itself.
        LogManager.instance().log(sm, Level.WARNING,
            "Database '%s' is the bootstrap source on this node but its local lastTxId=%d is BELOW the baseline "
                + "lastTxId=%d it sampled; keeping the local copy, which is what the other peers install from",
            dbName, localLastTxId, chosenLastTxId);
      return;
    }

    // Late newer joiner: the operator's data is fresher than the cluster's chosen baseline.
    // We will not silently overwrite it. Surface a SEVERE with the recovery procedure and leave
    // the local files in place. The operator can stop the cluster, copy this peer's data to the
    // others, and restart. Without this guard, a misconfigured rolling deploy could erase newer
    // transactions on a single pod by re-bootstrapping from older peers.
    if (localLastTxId > chosenLastTxId) {
      // The refusal keeps this node's file-id space assigned by an independent history, and nothing
      // used to reconcile it afterwards (issue #6124). Record it durably so the condition survives the
      // restart that the per-database replay-skip stops this branch from re-evaluating, gets re-verified
      // against the leader by verifyBootstrapDivergence(), and is visible in the cluster status instead
      // of only in this one log line.
      sm.baselines.markBootstrapUnreconciled(dbName);
      LogManager.instance().log(sm, Level.SEVERE,
          """
          Database '%s': local lastTxId=%d is GREATER than cluster bootstrap lastTxId=%d. \
          This peer's data is fresher than the cluster's chosen baseline (committed \
          BOOTSTRAP_FINGERPRINT_ENTRY). Refusing to overwrite local data. To preserve it, \
          stop the cluster, copy this peer's database directory to every other peer, then \
          restart all peers. Until this node's copy is reconciled its file ids are out of step \
          with the rest of the cluster: to discard it and adopt the leader's copy instead, run \
          POST /api/v1/cluster/resync/%s on this node.""",
          dbName, localLastTxId, chosenLastTxId, dbName);
      return;
    }

    // Mismatch: install the leader-shipped full snapshot. Runtime delta catch-up of the gap
    // beyond the baseline is handled natively by Ratis AppendEntries once the snapshot is in
    // place; at bootstrap time the Ratis log is empty on every peer so a transaction-level
    // delta cannot be served from it.
    LogManager.instance().log(sm, Level.INFO,
        """
        Database '%s' bootstrap mismatch (local lastTxId=%d / fp=%s..., baseline lastTxId=%d / fp=%s...); \
        reinstalling from leader-shipped full snapshot""",
        dbName, localLastTxId, localFingerprint.substring(0, Math.min(8, localFingerprint.length())),
        chosenLastTxId, chosenFingerprint.substring(0, Math.min(8, chosenFingerprint.length())));
    installFromLeaderForBootstrapWithRetry(dbName, true);
  }

  /**
   * Runs {@link #installFromLeaderForBootstrap} for one of the bootstrap arms and converts a failure into an
   * asynchronous retry rather than letting it reach {@link ArcadeStateMachine#applyTransaction}'s critical-error halt.
   * <p>
   * Applied on the Raft {@code StateMachineUpdater} thread: letting a failure propagate shuts the server down
   * and leaves the database closed, and a transient leader unavailability during restart must not do that -
   * {@code install()} downloads before touching the live files, so a failed download leaves whatever was here
   * exactly as it was.
   * <p>
   * Extracted for issue #7298, which gave this recovery a SECOND caller: the replay-skip arm, when the database
   * the skip is about turns out not to be on this node any more. The two callers differ in one fact that decides
   * both the safety net and the retry, so it is a parameter rather than a re-derived guess -
   * {@code server.existsDatabase} inside the catch cannot tell "the install deregistered it" from "it was never
   * here".
   *
   * @param hadLocalCopy whether a local copy of {@code dbName} existed when the install was started. When it did,
   *                     a database left deregistered is reopened and the ordinary full-resync path carries the
   *                     retry. When it did not, neither applies: reopening would throw on a database that does
   *                     not exist, and {@code triggerSnapshotDownload} only reinstalls the databases this server
   *                     already has registered, so the retry has to be this same targeted install again
   */
  private void installFromLeaderForBootstrapWithRetry(final String dbName, final boolean hadLocalCopy) {
    // A holder for the WHOLE operation, the scheduled retry included, so the issue #7519 readiness gate does not
    // lapse in the gap between a failed install and the retry that replaces the copy (CodeRabbit on PR #7964).
    // The one installFromLeaderForBootstrap takes for itself is released by its own finally; this one outlives it.
    //
    // NOT the durable #6124 unreconciled mark, which an earlier revision of this fix borrowed for the same job
    // (review of PR #7964). That set means "this node kept a copy FRESHER than the baseline and needs an operator
    // to choose a side": ClusterAlerts raises it as CRITICAL, tells the operator their data diverged and
    // recommends stopping the cluster and copying a directory to every peer. A first download that found no
    // leader yet - the ordinary case at formation, since the entry is applied while election on this peer may
    // still be settling - is none of those things and retries itself. Holding readiness must not also raise a
    // false CRITICAL with a drastic remedy.
    beginBootstrapInstall(dbName);
    boolean holderHandedOver = false;
    try {
      installFromLeaderForBootstrap(dbName);
    } catch (final RuntimeException e) {
      LogManager.instance().log(sm, Level.SEVERE,
          "Failed to install snapshot during bootstrap for database '%s': %s. Scheduling an async retry once a "
              + "leader is reachable.", dbName, e.getMessage());

      if (hadLocalCopy) {
        // Safety net: install rolls back + reopens on failure; reopen here if left deregistered for any reason.
        // This branch should be unreachable on the normal failed-download case - install() is download-before-
        // close, so a download failure never touches the live files and leaves the DB open. It guards against
        // unexpected future changes (or a failure in a later install phase) that could leave it deregistered.
        if (!sm.server.existsDatabase(dbName)) {
          // Not reopened while the node is shutting down (issue #9568), for the reason databaseFor() gives: the apply
          // fails with a closed database instead, and applyWithRetry leaves the entry for the replay on restart.
          if (sm.isNodeShuttingDown())
            throw new DatabaseIsClosedException("Database '" + dbName + "' was left deregistered by a failed bootstrap "
                + "install and this node is shutting down, so it is not reopened; the entry is left for the replay on "
                + "restart");
          try {
            sm.server.getDatabaseForOperator(dbName);
          } catch (final Exception reopenEx) {
            // Deliberate last resort: the database is both unusable and unreopenable, so there is nothing
            // safe to serve. Unlike the transient leader-unavailable case above (local copy intact, retried
            // async), this is unrecoverable locally, so we intentionally DO let it reach applyTransaction's
            // critical-error halt rather than mask data loss behind a node that keeps running.
            throw new RuntimeException("Cannot reopen database '" + dbName + "' after a failed bootstrap install", reopenEx);
          }
        }
        // Flag the pending download and run it off-thread.
        sm.needsSnapshotDownload.set(true);
        // The holder goes to the pending-replacement set rather than to the retry (issue #8367): it is released
        // when this node's copy is actually replaced, not when one retry happens to end. A name already pending
        // (a replay of the same entry) already owns its holder, so this one is released in the finally below.
        holderHandedOver = markBootstrapReplacementPending(dbName);
      }

      // We are inside the catch on the Raft StateMachineUpdater thread: a RejectedExecutionException from
      // a shut-down executor (server stopping) must not escape, or it would reach applyTransaction's
      // critical-error halt - the very outcome this handler exists to prevent.
      try {
        sm.lifecycleExecutor.submit(() -> retryBootstrapInstall(dbName, hadLocalCopy));
        // Without a local copy the retry owns the holder and releases it in its own finally, whatever it decides
        // to do: the database is absent, so nothing is being served in the cluster's stead (issue #8045).
        if (!hadLocalCopy)
          holderHandedOver = true;
      } catch (final RejectedExecutionException ree) {
        // The remediation differs by branch, and naming the wrong one is the defect this whole change is about.
        // With a local copy the needsSnapshotDownload flag is set above, so the HealthMonitor backstop genuinely
        // picks the download up on the next start. WITHOUT one that flag was never set, and the backstop only
        // reinstalls databases the server has REGISTERED - which this one is not - so promising it here would
        // tell an operator a recovery path exists that does not cover them.
        if (hadLocalCopy)
          LogManager.instance().log(sm, Level.WARNING,
              "Cannot schedule bootstrap snapshot retry for '%s': executor is shut down; "
                  + "the HealthMonitor backstop will retry once the server is available", null, dbName);
        else
          LogManager.instance().log(sm, Level.SEVERE,
              "Cannot schedule the reinstall of database '%s', which was applied on this node in a previous session "
                  + "and is missing now: the executor is shut down. Nothing retries it automatically - the node is "
                  + "stopping, and on the next start this entry replays and reinstalls it. If it does not come back, "
                  + "run POST /api/v1/cluster/resync/%s on this node once a leader is reachable.",
              null, dbName, dbName);
      }
    } finally {
      // Released here on every path nothing else took ownership of: the install succeeded, or it failed without a
      // local copy and the executor was already shut down so nothing will retry. A holder nothing ever releases
      // would wedge the node out of the Service for good. The one handed to bootstrapReplacementsPending is not
      // such a holder: every path that replaces the copy releases it, and retryPendingBootstrapReplacements()
      // keeps driving one of those paths until it does.
      if (!holderHandedOver)
        endBootstrapInstall(dbName);
    }
  }

  /**
   * The off-thread half of {@link #installFromLeaderForBootstrapWithRetry}. Never throws: it runs on the
   * {@code lifecycleExecutor}, where an escaping exception is only logged by the executor and helps nobody.
   */
  private void retryBootstrapInstall(final String dbName, final boolean hadLocalCopy) {
    if (hadLocalCopy) {
      // No holder to release here: bootstrapReplacementsPending owns it until the copy is actually replaced
      // (issue #8367).
      retryBootstrapReplacement(dbName);
      return;
    }
    try {
      retryMissingBootstrapInstall(dbName);
    } finally {
      // Releases the holder installFromLeaderForBootstrapWithRetry handed over when it scheduled this retry, so
      // the node has been continuously out of the Service from the first install to the end of this one, however
      // this one ended (issue #7519, CodeRabbit on PR #7964).
      endBootstrapInstall(dbName);
    }
  }

  /**
   * The first retry of a failed replacement of a copy this node holds. Drives the ordinary full resync, which
   * settles the pending replacement when it reinstalls the database; when it does not - no leader yet, a second
   * failed download, another path holding the flag - the replacement stays pending and the node stays out of the
   * Service, and {@link #retryPendingBootstrapReplacements()} retries it on the next health tick (issue #8367).
   */
  private void retryBootstrapReplacement(final String dbName) {
    if (sm.needsSnapshotDownload.compareAndSet(true, false))
      sm.triggerSnapshotDownload();
    else
      // Another path (notifyLeaderChanged or the watchdog) already cleared the flag and is driving
      // the download; skip this retry. Logged so operators can trace why this submission did nothing.
      LogManager.instance().log(sm, Level.INFO,
          "Bootstrap snapshot retry skipped for '%s': download already triggered by another path", dbName);

    if (sm.bootstrapReplacementsPending.contains(dbName))
      LogManager.instance().log(sm, Level.WARNING,
          "Database '%s' has still not been replaced by the leader's copy the cluster's bootstrap baseline chose. This "
              + "node keeps it out of the Service and refuses client requests on it until it is; the periodic health "
              + "check retries the install once a leader is reachable. To force it, run "
              + "POST /api/v1/cluster/resync/%s on this node.", dbName, dbName);
  }

  /** The body of {@link #retryBootstrapInstall} for a database this node did not hold, which owns the readiness holder around it. */
  private void retryMissingBootstrapInstall(final String dbName) {

    // No local copy, so the full resync above has nothing to iterate over: it reinstalls the databases this
    // server has REGISTERED, and this one is exactly the one it does not have. Retry the targeted install.
    if (sm.server.existsDatabase(dbName)) {
      LogManager.instance().log(sm, Level.INFO,
          "Bootstrap snapshot retry skipped for '%s': the database is registered again, another path installed it", dbName);
      return;
    }
    try {
      installFromLeaderForBootstrap(dbName);
    } catch (final RuntimeException e) {
      // Recorded DURABLY before giving up, in the same set and the same file the #6124 overwrite guard uses
      // (CodeRabbit on PR #7756). Logging alone was not enough and the reason is this method's own premise:
      // applyTransaction persists this database's applied index whatever happens here, so the entry reads as
      // applied while the database is absent, and the replay that would retry it is not guaranteed to survive
      // the next Ratis snapshot. The mark is what outlives that - it is persisted in .raft/bootstrap-baselines,
      // published by ClusterAlerts, and re-verified on the HealthMonitor's bootstrap-divergence tick, which is
      // where the bounded retry now lives (reconcileBootstrapDivergence). installFromLeaderForBootstrap clears
      // it on the success path, so nothing has to remember to.
      sm.baselines.markBootstrapUnreconciled(dbName);
      // One retry HERE, then hand the problem to that tick: looping on the single-threaded lifecycleExecutor
      // would hold every other lifecycle task for as long as the leader stays unreachable.
      LogManager.instance().log(sm, Level.SEVERE,
          "Database '%s' was applied on this node in a previous session but is missing now, and reinstalling it from "
              + "the leader failed again: %s. The node is running WITHOUT it, and says so in the cluster status "
              + "until it is back (alert 'bootstrap-database-missing'). The periodic bootstrap-divergence check "
              + "retries the install once this node is a FOLLOWER - a node cannot install a database from itself, so "
              + "if this node is the leader, transfer leadership first (POST /api/v1/cluster/leader). To force it, "
              + "run POST /api/v1/cluster/resync/%s on this node once a leader that holds it is reachable.",
          e, dbName, e.getMessage(), dbName);
    }
  }

  /** Test convenience: applies the entry as a peer that did not source the baseline. */
  // @VisibleForTesting
  void applyBootstrapFingerprintEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    applyBootstrapFingerprintEntry(decoded, index, false);
  }

  /**
   * Close the local database and pull a full snapshot from the current leader. Same low-level
   * snapshot install machinery as {@code applyInstallDatabaseEntry(forceSnapshot=true)}.
   * <p>
   * The leader short-circuit below ASSERTS its premise rather than assuming it (issue #7901). It was written for
   * one caller - the fingerprint-mismatch arm, where this node has a local copy that differs from the cluster
   * baseline - and for that caller "the leader is the bootstrap source, so it already holds the chosen baseline"
   * is true by construction. Issue #7298 gave the method a second caller with the opposite premise: the database
   * is not on this node, and this node being the Raft leader does not make it appear. Returning normally there
   * meant no retry was scheduled, no durable mark recorded, and one TRACE line stood as the entire record of a
   * node permanently short of a database the cluster believes it has - the exact outcome #7298 existed to
   * prevent. Throwing routes that case into the handling already written for it.
   */
  void installFromLeaderForBootstrap(final String dbName) {
    // One read for both the leader check and the cluster token below (issue #7253).
    final RaftHAServer raft = sm.raftHAServer;
    if (raft != null && raft.isLeader()) {
      // "Present" means registered OR on disk, the same pair getBootstrapUnreconciled classifies on: a leader
      // whose copy is merely closed is still the source every peer installs from, and making it throw here would
      // mark it unreconciled and retry a download over files that are perfectly good.
      if (!sm.isDatabasePresentLocally(dbName))
        throw new IllegalStateException("Database '" + dbName + "' is missing on this node and this node is the Raft "
            + "leader, so there is nowhere to install it from. Transfer leadership (POST /api/v1/cluster/leader) to "
            + "a node that holds it, then force the install here (POST /api/v1/cluster/resync/" + dbName + ")");
      // The leader has the chosen baseline by definition (it's the source). No need to install.
      HALog.log(sm, HALog.TRACE, "Leader skips bootstrap snapshot install for '%s'", dbName);
      return;
    }

    // The node is out of the Service from here until the install terminates, one way or the other (issue
    // #7519). Registered BEFORE the install rather than inside it because the window this opens is the whole
    // install - the download most of all, which is where the local copy is still open and serving the very
    // bytes the cluster has decided against - and not the file swap at the end, which
    // SnapshotInstaller.install already guards with the node-wide snapshotInstallInProgress 503 (HTTP only).
    beginBootstrapInstall(dbName);
    try {
      // Resolve the leader address on each retry: the bootstrap-mismatch entry is applied
      // during Raft log replay on startup, which can race ahead of leader election on this peer.
      // install() keeps the local copy open during the download and rolls back on failure, so a
      // failed bootstrap install never leaves the database closed.
      final String clusterToken = raft != null ? raft.getClusterToken() : null;
      // Resolved through the same guard as every other snapshot pull: the supplier answers null - which
      // install() treats as "no leader to pull from" and retries - rather than handing back an address that
      // names this node or no single peer (issue #6202).
      sm.replacement.installLeaderCopy(dbName, sm.resync::guardedLeaderHttpAddress, sm.resync::guardedLeaderHttpsAddress, clusterToken);
      LogManager.instance().log(sm, Level.INFO,
          "Database '%s' reinstalled after bootstrap mismatch", dbName);
      sm.baselines.clearBootstrapUnreconciled(dbName);
      settleBootstrapReplacement(dbName);
    } catch (final IOException e) {
      throw new RuntimeException("Failed to install snapshot for bootstrap-mismatched database '" + dbName + "'", e);
    } finally {
      // In a finally, and deliberately also on the failure path: a node that could not reinstall is not
      // installing any more, and holding readiness on a condition nothing clears would wedge it out of the
      // Service for good. What it holds then is a copy the cluster did not adopt, which is what the
      // unreconciled mark records and what bootstrapWindowReason() reports in its own right.
      endBootstrapInstall(dbName);
    }
  }

  /**
   * Adds one holder to {@code dbName}'s bootstrap-install depth (issue #7519). Two callers take one, and they
   * nest: {@link #installFromLeaderForBootstrap} holds one for its own install, and
   * {@link #installFromLeaderForBootstrapWithRetry} holds one for the whole operation including the retry it may
   * schedule. Each pairs its own with {@link #endBootstrapInstall} in a {@code finally}, which is what the depth
   * is for - with a set, the inner release would drop the name while the outer operation was still running.
   */
  void beginBootstrapInstall(final String dbName) {
    bootstrapInstallsInFlight.merge(dbName, 1, Integer::sum);
  }

  /**
   * Removes one holder, and the entry itself once the last holder leaves - so the map does not keep the name of
   * every database this node ever reinstalled for the node's lifetime, the same rule the per-database applied
   * index and the bootstrap baselines already follow.
   */
  private void endBootstrapInstall(final String dbName) {
    bootstrapInstallsInFlight.computeIfPresent(dbName, (name, depth) -> depth > 1 ? depth - 1 : null);
  }

  /**
   * Arms a pending bootstrap replacement of {@code dbName} and durably records it (issue #8411), under
   * {@link BootstrapBaselines#bootstrapBaselinesFileLock} so the set and the file change together. Returns whether this call armed it,
   * i.e. whether the caller's readiness holder now belongs to the pending replacement: a name already pending (a
   * replay of the same entry, or one restored from the file) already owns its holder.
   */
  private boolean markBootstrapReplacementPending(final String dbName) {
    synchronized (sm.baselines.bootstrapBaselinesFileLock) {
      sm.baselines.ensureBootstrapBaselinesLoaded();
      if (!sm.bootstrapReplacementsPending.add(dbName))
        return false;
      sm.baselines.persistBootstrapBaselinesFile();
      return true;
    }
  }

  /**
   * Retires a pending bootstrap replacement of {@code dbName} and releases the readiness holder it owns (issue
   * #8367). Called from every path that actually replaces this node's copy with the leader's, and from the DROP of
   * the database. The removal from the set is the ownership test, so the holder is released exactly once however
   * many of those paths race; a database with nothing pending costs one read of an empty set.
   */
  void settleBootstrapReplacement(final String dbName) {
    // Loaded first: a replacement pending from before a restart is not in the set until the file is read, and a path
    // that replaced the copy before anything else read it would otherwise leave the flag on disk for the loader to
    // re-arm afterwards (issue #8411).
    sm.baselines.ensureBootstrapBaselinesLoaded();
    if (sm.bootstrapReplacementsPending.isEmpty())
      return;
    synchronized (sm.baselines.bootstrapBaselinesFileLock) {
      if (!sm.bootstrapReplacementsPending.remove(dbName))
        return;
      sm.baselines.persistBootstrapBaselinesFile();
    }
    endBootstrapInstall(dbName);
    LogManager.instance().log(sm, Level.INFO,
        "Database '%s' now carries the leader's copy: the pending bootstrap replacement is complete", dbName);
  }

  /** {@link #settleBootstrapReplacement} for every pending name not in {@code notReplaced}, for the full installs. */
  void settleBootstrapReplacementsExcept(final Set<String> notReplaced) {
    sm.baselines.ensureBootstrapBaselinesLoaded();
    if (sm.bootstrapReplacementsPending.isEmpty())
      return;
    for (final String dbName : new ArrayList<>(sm.bootstrapReplacementsPending))
      if (notReplaced == null || !notReplaced.contains(dbName))
        settleBootstrapReplacement(dbName);
  }

  /**
   * Databases whose bootstrap replacement failed and is still pending, sorted (issue #8367). Package-private for
   * tests: operators see the same names in the {@code bootstrap-install-in-progress} alert, since each one keeps its
   * holder in {@link #getBootstrapInstallsInFlight()}.
   */
  List<String> getPendingBootstrapReplacements() {
    sm.baselines.ensureBootstrapBaselinesLoaded();
    if (sm.bootstrapReplacementsPending.isEmpty())
      return Collections.emptyList();
    final List<String> names = new ArrayList<>(sm.bootstrapReplacementsPending);
    Collections.sort(names);
    return names;
  }

  /**
   * Periodic retry of every bootstrap replacement that failed and is still pending (issue #8367), driven by the
   * {@link HealthMonitor} tick. This is the re-arm the one-shot retry never had: that retry consumes
   * {@code needsSnapshotDownload}, and {@code triggerSnapshotDownload()} restores the flag only while a
   * stale-snapshot read floor is outstanding - which it is not at a first formation - so a second failed download
   * used to leave the replacement pending with nothing that would ever try it again.
   * <p>
   * Each attempt is the same targeted install the bootstrap itself runs ({@link #installFromLeaderForBootstrap}),
   * one database at a time rather than a full resync of every database on the node, submitted to the
   * {@code lifecycleExecutor} like every other leader-facing download. It settles the replacement on success; on
   * failure the replacement stays pending for the next window.
   * <p>
   * Zero cost in the normal case - the pending set is empty and this returns after one read. Otherwise throttled to
   * one attempt per {@link ArcadeStateMachine#computeSnapshotWatchdogTimeoutMs()}, and it stands down while a download is already
   * running (that one settles the replacement or leaves it for the next window) and while no leader address is
   * known yet, without spending the throttle slot, so the first tick after a leader appears is the one that tries.
   * <p>
   * <b>On the leader it installs nothing</b>: a node cannot install from itself. It keeps holding - the copy is still
   * the one the baseline rejected - and says so once per window, naming the leadership transfer that unblocks it.
   */
  public void retryPendingBootstrapReplacements() {
    sm.baselines.ensureBootstrapBaselinesLoaded();
    if (sm.bootstrapReplacementsPending.isEmpty())
      return;
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null || sm.server == null)
      return;
    if (sm.snapshotDownloadInProgress.get())
      return;

    final long retryIntervalMs = sm.computeSnapshotWatchdogTimeoutMs();
    if (raftHA.isLeader()) {
      if (claimBootstrapReplacementRetrySlot(System.currentTimeMillis(), retryIntervalMs))
        LogManager.instance().log(sm, Level.WARNING,
            "Database(s) %s still hold the copy the cluster's bootstrap baseline rejected, and this node is the leader, "
                + "so there is nowhere to install the replacement from. They stay out of service on this node until "
                + "leadership moves - the health check hands it to a peer that holds them (issue #8529), or run "
                + "POST /api/v1/cluster/leader - and the replacement is installed from the new leader",
            getPendingBootstrapReplacements());
      return;
    }

    // The same precheck retryUnfilledSnapshotGap() makes, so a tick with no usable leader does not burn the slot.
    final String leaderHttpAddr = raftHA.getUnambiguousPeerHttpAddress(raftHA.getLeaderId());
    if (leaderHttpAddr == null || raftHA.isOwnHttpAddress(leaderHttpAddr))
      return;
    if (!claimBootstrapReplacementRetrySlot(System.currentTimeMillis(), retryIntervalMs))
      return;

    final List<String> pending = getPendingBootstrapReplacements();
    LogManager.instance().log(sm, Level.WARNING,
        "Retrying the bootstrap replacement of %s from the leader: the copy on this node is still the one the "
            + "cluster's committed baseline rejected (issue #8367)", pending);
    try {
      sm.lifecycleExecutor.submit(() -> {
        for (final String dbName : pending) {
          if (!sm.bootstrapReplacementsPending.contains(dbName))
            continue; // settled by another path since the tick
          try {
            installFromLeaderForBootstrap(dbName);
          } catch (final RuntimeException e) {
            LogManager.instance().log(sm, Level.WARNING,
                "Replacing database '%s' with the leader's copy failed again: %s. It stays out of service on this node "
                    + "and the next health check retries it; to force it, run POST /api/v1/cluster/resync/%s on this "
                    + "node", dbName, e.getMessage(), dbName);
          }
        }
      });
    } catch (final RejectedExecutionException ree) {
      LogManager.instance().log(sm, Level.WARNING,
          "Cannot schedule the pending bootstrap replacement retry: executor is shut down", ree);
    }
  }

  /** Claims the one pending-replacement retry slot per {@code intervalMs}; a lost CAS means another tick has it. */
  private boolean claimBootstrapReplacementRetrySlot(final long now, final long intervalMs) {
    final long previous = lastBootstrapReplacementRetryMs.get();
    if (previous != 0 && now - previous < intervalMs)
      return false;
    return lastBootstrapReplacementRetryMs.compareAndSet(previous, now);
  }

  /**
   * Databases this node is installing from the leader's bootstrap snapshot right now, sorted for deterministic
   * output (issue #7519) - every one of them, whether it replaces a copy this node holds or reinstalls one it lost.
   * <p>
   * Package-private, and read by {@code ClusterAlerts.NodeStatus} so {@code GET /api/v1/cluster} publishes it as
   * the {@code bootstrap-install-in-progress} alert and the {@code bootstrapInstalls} member (issue #8044): until
   * then the readiness gate was its only consumer, and the status document the gate's 503 body points at said
   * nothing about it.
   */
  List<String> getBootstrapInstallsInFlight() {
    sm.baselines.ensureBootstrapBaselinesLoaded();
    // The overwhelmingly common answer, and this is on the readiness-probe path: allocate nothing for it.
    if (bootstrapInstallsInFlight.isEmpty())
      return Collections.emptyList();
    final List<String> names = new ArrayList<>(bootstrapInstallsInFlight.keySet());
    Collections.sort(names);
    return names;
  }
}
