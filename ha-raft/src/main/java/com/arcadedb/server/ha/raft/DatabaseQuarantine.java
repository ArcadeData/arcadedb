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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.DivergedAcceptance;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * The quarantine of the databases whose local copy can no longer be trusted: a WAL gap, an apply error, a damaged copy
 * found at open. A quarantined database refuses clients and is resynced from the leader; the quarantine is persisted in
 * the applied-index file so it survives a restart, until a resync or an operator clears it.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which quarantines a database when applying an entry to it fails.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class DatabaseQuarantine {
  private final ArcadeStateMachine sm;

  // The local-copy damages already reported on a sole voter (issue #11518), keyed "<database>\0<cause>": the recheck runs on
  // every reopen and every schema reload, and a sole voter cannot repair the copy, so the SEVERE report is made once
  private final Set<String> soleVoterDamageReported = ConcurrentHashMap.newKeySet();

  // Bounded escalation (issue #4740): a node that can never resync (no stable leader reachable)
  // must not stay in "swallow unexpected errors" mode forever, silently degrading. Each error
  // swallowed on a diverged database increments this; once it exceeds the threshold the next
  // unexpected error is allowed to propagate to the fatal halt path so a truly stuck node surfaces
  // loudly rather than quietly. Reset to 0 whenever a snapshot resync clears the diverged set.
  // Deliberately JVM-wide (not per-database): the threshold is a coarse "this node is stuck, halt
  // loudly" backstop, so a shared budget across all diverged databases is the intended behaviour -
  // one very noisy diverged database crossing the threshold should still halt the node.
  final AtomicInteger divergedSwallowedErrors      = new AtomicInteger(0);

  // Log-flood throttle for a diverged database's "snapshot resync in progress" notice. Once a WAL
  // version gap has quarantined a database, EVERY subsequent committed entry for it hits the same gap
  // until the snapshot download lands - potentially thousands of entries on a busy database. Logging a
  // SEVERE (with stack trace) per entry both floods the log and, on small nodes, steals the CPU/IO the
  // snapshot download needs to heal the node (observed in the field: ~30 SEVERE/s for 20s starving a
  // ~1 MB/s resync). This map records the last time the throttled notice was emitted per database so it
  // fires at most once per window. Entries are cleared when the database's divergence clears.
  final Map<String, Long> lastDivergedResyncLogByDb        = new ConcurrentHashMap<>();

  private static final long              DIVERGED_RESYNC_LOG_THROTTLE_MS   = 5_000L;

  DatabaseQuarantine(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Removes a single database from the diverged set after a targeted resync restored its state
   * (issue #4797). When the set becomes empty the bounded-escalation counter is reset, mirroring
   * {@link #clearDivergedState()} which clears everything after a full resync. The counter is shared
   * across databases, so it is only safe to reset once no database remains quarantined.
   */
  // @VisibleForTesting
  void clearDivergedDatabase(final String dbName) {
    // Under the applied-index lock and after the load, so the on-disk quarantine is dropped with the in-memory
    // one (issue #7735). Without this the resync would heal the database and the node would still come back
    // quarantined after the next restart - the inverse of the bug, and just as wrong.
    synchronized (sm.appliedIndexes.appliedIndexFileLock) {
      sm.appliedIndexes.ensureAppliedIndexLoaded();
      // The resync restored this database, so its read floor is satisfied (issue #6760). Dropped in the same write
      // as the quarantine, so the file never keeps one without the other (issue #8137)
      final boolean floorRemoved = sm.staleDatabaseAppliedFloors.remove(dbName) != null;
      if (sm.divergedDatabases.remove(dbName) != null || floorRemoved)
        sm.appliedIndexes.persistAppliedIndexFile();
    }
    lastDivergedResyncLogByDb.remove(dbName);
    // The resync brought the leader's copy, configuration.json included (issue #10086)
    sm.configurationEntries.releaseHeldConfiguration(dbName);
    if (sm.divergedDatabases.isEmpty())
      divergedSwallowedErrors.set(0);
  }

  /**
   * The operator's override of issue #9449: lifts the quarantine of {@code dbName} and its read floor WITHOUT a resync,
   * accepting this node's copy as it is, and persists the change in the same write the quarantine was persisted with.
   * <p>
   * Every other way out of a quarantine is a resync from a peer ({@link #clearDivergedDatabase}) or a DROP
   * ({@link DropDatabaseApplier#clearDroppedDatabaseQuarantine}). A sole voter has no peer, and since #9308 it no longer RAISES a quarantine,
   * but one restored from disk (#7735) or raised while the node still had peers keeps it not-ready and its log
   * un-checkpointed for good. Lifting it gives up the guarantee that the skipped entry stays replayable, which is why it
   * is an operator decision and is logged at WARNING with who made it, at which applied index, over which cause.
   * <p>
   * The persistence is NOT best-effort here, unlike the quarantine's own write: an override that does not survive a
   * restart would bring the quarantine back from disk, so on a failed write the in-memory state is put back and the
   * caller is told. Between the removal and that rollback a lock-free reader ({@link ArcadeStateMachine#isDatabaseDiverged}, the readiness
   * probe) can see the database as not quarantined for the duration of the failed write; only the file is authoritative
   * across a restart, and it never stops saying "quarantined".
   *
   * @return what was lifted, or {@code null} when {@code dbName} carried neither a quarantine nor a read floor
   *
   * @throws IOException when the change could not be written to the applied-index file; nothing is lifted then
   */
  DivergedAcceptance acceptDivergedDatabase(final String dbName, final String acceptedBy) throws IOException {
    if (dbName == null || dbName.isEmpty())
      return null;
    final DivergedAcceptance acceptance;
    synchronized (sm.appliedIndexes.appliedIndexFileLock) {
      sm.appliedIndexes.ensureAppliedIndexLoaded();
      final DivergenceCause cause = sm.divergedDatabases.remove(dbName);
      final Long floor = sm.staleDatabaseAppliedFloors.remove(dbName);
      if (cause == null && floor == null)
        return null;
      if (!sm.appliedIndexes.persistAppliedIndexFile(true)) {
        if (cause != null)
          sm.divergedDatabases.put(dbName, cause);
        if (floor != null)
          sm.staleDatabaseAppliedFloors.put(dbName, floor);
        throw new IOException("the change could not be written to " + sm.appliedIndexes.getAppliedIndexFile()
            + ", so the quarantine would come back at the next restart: check that the .raft directory is writable and "
            + "has free space");
      }
      final Long applied = sm.appliedIndexes.appliedIndexByDb.get(dbName);
      acceptance = new DivergedAcceptance(cause, floor != null ? floor : -1L, applied != null ? applied : -1L);
    }
    // Outside the lock, as clearDivergedDatabase does: the log throttle and the swallow budget are advisory counters, not
    // part of the persisted quarantine, and a racing quarantine on another database at worst keeps a budget it had.
    lastDivergedResyncLogByDb.remove(dbName);
    if (sm.divergedDatabases.isEmpty())
      divergedSwallowedErrors.set(0);

    LogManager.instance().log(sm, Level.WARNING,
        "Database '%s': %s lifted its quarantine WITHOUT a resync, accepting this node's copy as it is (applied index %d, "
            + "cause: %s, read floor: %s). The entry the quarantine skipped is not replayed: if the copy is missing it, it "
            + "stays missing. Check the database (CHECK DATABASE) and restore it from a backup if it is damaged "
            + "(issue #9449)",
        dbName, acceptedBy, acceptance.appliedIndex(),
        acceptance.cause() != null ? acceptance.cause().getDescription() : "none, only a read floor stood",
        acceptance.readFloor() >= 0 ? String.valueOf(acceptance.readFloor()) : "none");
    return acceptance;
  }

  /**
   * Returns {@code true} at most once per {@link #DIVERGED_RESYNC_LOG_THROTTLE_MS} window per database.
   * Used to rate-limit the "snapshot resync in progress" notice that would otherwise be emitted once per
   * committed entry while a database is quarantined after a WAL version gap, flooding the log and
   * starving the in-flight snapshot download on small nodes. Package-private for unit testing.
   */
  // @VisibleForTesting
  boolean shouldLogDivergedResync(final String dbName) {
    final long now = System.currentTimeMillis();
    final Long last = lastDivergedResyncLogByDb.get(dbName);
    if (last != null && now - last < DIVERGED_RESYNC_LOG_THROTTLE_MS)
      return false;
    lastDivergedResyncLogByDb.put(dbName, now);
    return true;
  }

  boolean quarantineUnpublishedSchemaChange(final String dbName, final String session, final Throwable refusal) {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA != null && !sm.isDatabaseDiverged(dbName) && raftHA.isSoleVoter()) {
      LogManager.instance().log(sm, Level.SEVERE,
          "The %s on database '%s' was applied locally but the entry publishing it was refused or could not be built "
              + "(%s). This node is the only voter, so there is no peer to resync it from and it is NOT quarantined: the "
              + "database now holds a change the Raft log does not (issues #9555, #9558)",
          session, dbName, ArcadeStateMachine.describe(refusal));
      return false;
    }

    if (!quarantineDatabase(dbName, DivergenceCause.UNPUBLISHED_SCHEMA_CHANGE))
      return false;

    LogManager.instance().log(sm, Level.SEVERE,
        "The %s on database '%s' was applied locally but the entry publishing it was refused or could not be built "
            + "(%s), so no other node holds it: quarantining the database and resyncing it from the leader now rather "
            + "than waiting for a later entry to collide with it (issues #9555, #9558)",
        session, dbName, ArcadeStateMachine.describe(refusal));
    sm.triggerDatabaseResync(dbName);
    sm.resync.handOffLeadershipIfLeader(dbName);
    return true;
  }

  /**
   * Marks {@code dbName} as diverged from the committed Raft log (issue #4740). While a database is
   * diverged, unexpected Throwables raised while applying its entries are treated as recoverable
   * resync conditions in {@link ArcadeStateMachine#applyWithRetry} rather than fatal halts.
   */
  // @VisibleForTesting
  void markStateDiverged(final String dbName) {
    markStateDiverged(dbName, DivergenceCause.APPLY_ERROR);
  }

  /**
   * {@link #markStateDiverged(String)} naming why, which is what the cluster status document reports
   * (issue #7741).
   * <p>
   * The FIRST cause a quarantine is recorded with is the one it keeps: {@code putIfAbsent} deliberately, which is
   * the behaviour the {@code Set.add()} this replaced already had. A quarantined database goes on failing - every
   * later committed entry for it hits the same wall - so the last cause would be noise from a database that is
   * already waiting for a resync, while the first is the one that describes what went wrong. The map is cleared
   * when the resync lands, so the next quarantine records afresh (code review on PR #7747).
   */
  // @VisibleForTesting
  void markStateDiverged(final String dbName, final DivergenceCause cause) {
    quarantineDatabase(dbName, cause);
  }

  /**
   * The single writer of a quarantine: records {@code dbName} as diverged under {@code cause} and durably
   * persists the quarantine, returning {@code true} only for the call that established it (issue #7735).
   * <p>
   * Every path that quarantines a single database goes through here - the WAL version gap in
   * {@link ArcadeStateMachine#applyReplicatedTransaction}, the apply error and the undecodable entry in
   * {@link EntryApplyRetry#handleUnexpectedApplyError} - so there is one place the durability can be missing from rather than three.
   * The only other writer is {@code settleDivergedStateAfterInstall}, which a snapshot install needs to REPLACE the
   * whole diverged state (quarantines, read floors and applied positions) in one write rather than add to it, which is
   * why it cannot be a loop over this method: both hold {@link AppliedIndexFile#appliedIndexFileLock} across their mutation and write
   * the same file through {@link AppliedIndexFile#persistAppliedIndexFile()}.
   * <p>
   * Written under {@link AppliedIndexFile#appliedIndexFileLock}, the same lock {@link AppliedIndexFile#ensureAppliedIndexLoaded()} and the
   * applied-index writers take, so the mark and the applied position it qualifies land in one atomic rename.
   * The load-before-mutate keeps the other databases' applied positions intact when the file is rewritten.
   * <p>
   * The file write is best-effort, exactly like the applied-index write it shares, but a failure is reported at
   * WARNING here rather than at the FINE the shared writer logs: this is the one write the whole fix depends on,
   * and an operator whose disk refused it needs to know the quarantine will not survive the next restart. The
   * backstop is the snapshot refusal in {@link ArcadeStateMachine#takeSnapshot()}, which reads the in-memory map and so still
   * holds: with the log unpurged past the skipped entry, a restart replays into the same quarantine instead of
   * into a silent gap.
   *
   * @return {@code true} when this call established the quarantine, {@code false} when one was already recorded
   */
  // @VisibleForTesting
  boolean quarantineDatabase(final String dbName, final DivergenceCause cause) {
    if (dbName == null || dbName.isEmpty())
      return false;
    synchronized (sm.appliedIndexes.appliedIndexFileLock) {
      sm.appliedIndexes.ensureAppliedIndexLoaded();
      if (sm.divergedDatabases.putIfAbsent(dbName, cause) != null)
        return false;
      if (!sm.appliedIndexes.persistAppliedIndexFile() && !sm.closed)
        LogManager.instance().log(sm, Level.WARNING,
            "Database '%s' is quarantined (%s) but the quarantine could NOT be written to %s: a restart before "
                + "this is fixed comes back without it. The log is not checkpointed while a database is "
                + "quarantined, so the skipped entry stays replayable, but check that the .raft directory is "
                + "writable and has free space (issue #7735)",
            dbName, cause, sm.appliedIndexes.getAppliedIndexFile());
      return true;
    }
  }

  /**
   * Quarantines every open database whose crash recovery, run when the server opened it, did not replay every transaction
   * (issue #11250), so the node does not serve it, does not lead with it, and re-installs it from the cluster.
   * <p>
   * Called once the Raft server is up, which is after the server opened the databases: the recovery the check reads has
   * already run. Before this, a node whose recovery had aborted on a WAL gap still rejoined the cluster, won an election
   * and answered queries from pages a power loss had left behind.
   * <p>
   * A sole voter is left alone, for the reason {@link EntryApplyRetry#handleUnexpectedApplyError} spells out: there is no copy to resync
   * from, so the quarantine could never lift. It is reported instead.
   *
   * @return the databases quarantined by this call
   */
  List<String> quarantineDatabasesWithIncompleteRecovery() {
    final ArcadeDBServer srv = sm.server;
    final RaftHAServer raftHA = sm.raftHAServer;
    if (srv == null || raftHA == null)
      return List.of();

    final List<String> quarantined = new ArrayList<>();
    for (final String dbName : srv.getDatabaseNames()) {
      if (ArcadeDBServer.isReservedDatabaseName(dbName))
        continue;
      final ServerDatabase db;
      try {
        db = srv.getDatabase(dbName, false, false);
      } catch (final RuntimeException e) {
        LogManager.instance().log(sm, Level.WARNING, "Cannot read the recovery outcome of database '%s': %s", dbName,
            e.getMessage());
        continue;
      }
      if (quarantineIfLocalCopyDamaged(dbName, db))
        quarantined.add(dbName);
    }
    return quarantined;
  }

  /**
   * Quarantines {@code dbName} when its local copy carries one of the damages {@link LocalCopyDamage} reads (issues #11250,
   * #11258), through the same decisions as the startup scan: the three {@code quarantineAfter*} methods below, so the
   * quarantine, the resync, the leadership hand-off and the sole-voter exception are the ones startup takes.
   * <p>
   * The startup scan is not the only time a copy can turn up damaged (issue #11518): a database closed for idleness and reopened,
   * one opened lazily after the scan, and a replicated schema entry's reload all read the files again, and the scan never saw
   * what they found. {@link #recheckOpenedDatabase} and {@link ArcadeStateMachine#applySchemaEntry} call this for those.
   * <p>
   * A database already quarantined is left alone: the resync that will lift the quarantine replaces the copy anyway. On a
   * sole voter the damage is reported once per database and cause rather than on every reload, since nothing can repair it
   * there and a schema entry reloads the schema each time; a clean read forgets the report, so a damage that comes back is
   * reported again.
   *
   * @return {@code true} when this call quarantined the database
   */
  boolean quarantineIfLocalCopyDamaged(final String dbName, final DatabaseInternal db) {
    if (dbName == null || db == null || sm.isDatabaseDiverged(dbName))
      return false;
    final LocalCopyDamage damage;
    try {
      damage = LocalCopyDamage.of(db);
    } catch (final RuntimeException e) {
      LogManager.instance().log(sm, Level.WARNING, "Cannot read the recovery outcome of database '%s': %s", dbName,
          e.getMessage());
      return false;
    }
    if (damage == null) {
      if (!soleVoterDamageReported.isEmpty())
        soleVoterDamageReported.removeIf(key -> key.startsWith(dbName + '\0'));
      return false;
    }
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null)
      return false;
    if (raftHA.isSoleVoter() && !soleVoterDamageReported.add(dbName + '\0' + damage.cause()))
      return false;
    return switch (damage.cause()) {
      case LOCAL_RECOVERY_INCOMPLETE -> quarantineAfterIncompleteRecovery(dbName);
      case EMPTY_DICTIONARY -> quarantineAfterEmptiedDictionary(dbName);
      default -> quarantineAfterUnavailableUniqueIndex(dbName, damage.uniqueIndexNames());
    };
  }

  /**
   * The open-time half of issue #11518: {@code RaftHAServer} calls this for every database the server opens once the Raft
   * server is running - a database closed for idleness and reopened by a request, one opened lazily after the startup scan,
   * one {@link ArcadeStateMachine#databaseFor} reopens for an entry. Before, only the startup scan read the damage, so a copy found damaged by
   * any later open served queries and stayed electable.
   * <p>
   * Not for a database being replaced by a snapshot install ({@link DatabaseReplacement#databasesBeingReplaced}): the install validates the
   * copy it reopens itself ({@code SnapshotInstaller.swapAndReopen}) and rolls a damaged one back, and every install path
   * lifts the quarantine on success, which would erase one raised here.
   *
   * @return {@code true} when this call quarantined the database
   */
  boolean recheckOpenedDatabase(final String dbName, final DatabaseInternal db) {
    if (dbName == null || ArcadeDBServer.isReservedDatabaseName(dbName) || sm.replacement.databasesBeingReplaced.containsKey(dbName))
      return false;
    return quarantineIfLocalCopyDamaged(dbName, db);
  }

  /**
   * The decision for one database whose crash recovery did not replay every transaction (issue #11250): quarantine it, start
   * the targeted resync and hand leadership off, unless this node is the only voter (nothing to resync from, so the
   * quarantine could never lift: reported at SEVERE instead).
   *
   * @return {@code true} when this call quarantined the database
   */
  boolean quarantineAfterIncompleteRecovery(final String dbName) {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null)
      return false;

    if (raftHA.isSoleVoter()) {
      LogManager.instance().log(sm, Level.SEVERE,
          "The crash recovery of database '%s' did not complete and this node is the only voter, so there is no peer to "
              + "re-install it from. It is NOT quarantined; it may be missing committed data: run CHECK DATABASE and "
              + "restore it from a backup if it is damaged (issue #11250)", dbName);
      return false;
    }
    if (!quarantineDatabase(dbName, DivergenceCause.LOCAL_RECOVERY_INCOMPLETE))
      return false;

    LogManager.instance().log(sm, Level.SEVERE,
        "The crash recovery of database '%s' did not complete, so its local copy may be missing committed data: "
            + "quarantining it and re-installing it from the leader (issue #11250)", dbName);
    sm.triggerDatabaseResync(dbName);
    sm.resync.handOffLeadershipIfLeader(dbName);
    return true;
  }

  /**
   * The decision for one database that opened with a unique index it could not load, for a file with no header page (issue
   * #11258): the index enforces nothing on this node, and the load cannot tell a half-arrived compaction file from one a power
   * loss emptied. Like {@link #quarantineAfterIncompleteRecovery}, it quarantines the database and re-installs it from the
   * leader, unless this node is the only voter.
   *
   * @return {@code true} when this call quarantined the database
   */
  boolean quarantineAfterUnavailableUniqueIndex(final String dbName, final List<String> indexNames) {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null)
      return false;

    if (raftHA.isSoleVoter()) {
      LogManager.instance().log(sm, Level.SEVERE,
          "Database '%s' opened without its unique indexes %s (no header page) and this node is the only voter, so there is "
              + "no peer to re-install it from. It is NOT quarantined; the constraint is not enforced: run REBUILD INDEX "
              + "(issue #11258)", null, dbName, indexNames);
      return false;
    }
    if (!quarantineDatabase(dbName, DivergenceCause.UNAVAILABLE_UNIQUE_INDEX))
      return false;

    LogManager.instance().log(sm, Level.SEVERE,
        "Database '%s' opened without its unique indexes %s (no header page), so its constraint is not enforced here: "
            + "quarantining it and re-installing it from the leader (issue #11258)", null, dbName, indexNames);
    sm.triggerDatabaseResync(dbName);
    sm.resync.handOffLeadershipIfLeader(dbName);
    return true;
  }

  /**
   * The decision for one database whose schema dictionary a power loss emptied (issue #11258): the files kept their names but
   * not their content, the load wrote a fresh empty dictionary page, and crash recovery had no WAL left to repair it from, so
   * it reported completed while every property name was lost. Like {@link #quarantineAfterIncompleteRecovery}, it quarantines
   * the database and re-installs it from the leader, so the node neither serves the copy, nor leads with it, nor ships it as a
   * snapshot, unless this node is the only voter.
   *
   * @return {@code true} when this call quarantined the database
   */
  boolean quarantineAfterEmptiedDictionary(final String dbName) {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null)
      return false;

    if (raftHA.isSoleVoter()) {
      LogManager.instance().log(sm, Level.SEVERE,
          "The schema dictionary of database '%s' was found empty although the schema declares types, and this node is the "
              + "only voter, so there is no peer to re-install it from. It is NOT quarantined; property names are lost: "
              + "restore it from a backup (issue #11258)", dbName);
      return false;
    }
    if (!quarantineDatabase(dbName, DivergenceCause.EMPTY_DICTIONARY))
      return false;

    LogManager.instance().log(sm, Level.SEVERE,
        "The schema dictionary of database '%s' was found empty although the schema declares types, so its local copy lost "
            + "its property names: quarantining it and re-installing it from the leader (issue #11258)", dbName);
    sm.triggerDatabaseResync(dbName);
    sm.resync.handOffLeadershipIfLeader(dbName);
    return true;
  }

  /**
   * Clears the diverged-database set and the bounded-escalation counter after a snapshot resync has
   * restored consistent state across all databases. A resync always reinstalls every database from
   * the leader, so clearing the whole set (rather than a single database) matches what the resync
   * actually did.
   */
  // @VisibleForTesting
  void clearDivergedState() {
    // Same reasoning as clearDivergedDatabase: the persisted copy has to go with the in-memory one (issue #7735).
    synchronized (sm.appliedIndexes.appliedIndexFileLock) {
      sm.appliedIndexes.ensureAppliedIndexLoaded();
      // A resync reinstalls every database, so every per-database read floor is satisfied too (issue #6760). The
      // snapshot-install path re-publishes the floors of the databases it could NOT reinstall right after calling
      // this, so clearing wholesale here stays correct. Same write as the quarantine (issue #8137)
      if (!sm.divergedDatabases.isEmpty() || !sm.staleDatabaseAppliedFloors.isEmpty()) {
        sm.divergedDatabases.clear();
        sm.staleDatabaseAppliedFloors.clear();
        sm.appliedIndexes.persistAppliedIndexFile();
      }
    }
    lastDivergedResyncLogByDb.clear();
    divergedSwallowedErrors.set(0);
    // Every database was reinstalled from the leader, configuration.json included (issue #10086)
    sm.configurationEntries.heldConfigurations.clear();
  }

  /**
   * Why {@code dbName} is quarantined on this node, or {@code null} when it is not (issue #8468). A database a snapshot
   * install gave up on carries its read floor beside the quarantine, so this covers it too
   * ({@link DivergenceCause#SNAPSHOT_INSTALL_INCOMPLETE}).
   */
  DivergenceCause quarantineCause(final String dbName) {
    if (dbName == null)
      return null;
    // A quarantine restored from disk must be visible to the first caller after a restart (issue #7735)
    sm.appliedIndexes.ensureAppliedIndexLoaded();
    return sm.divergedDatabases.get(dbName);
  }

  /**
   * The databases this node holds quarantined, as a snapshot (issue #9553): what this node advertises to its peers'
   * capability poll so each of them can tell whether any voter still holds a copy a resync could be served from.
   * Allocation-free when nothing is quarantined, which is every poll of a healthy node.
   */
  Set<String> getQuarantinedDatabaseNames() {
    sm.appliedIndexes.ensureAppliedIndexLoaded();
    return sm.divergedDatabases.isEmpty() ? Set.of() : Set.copyOf(sm.divergedDatabases.keySet());
  }

  /**
   * The quarantined databases a targeted resync from the current leader could actually restore (issue #9553): those the
   * leader's last fresh capability answer does NOT report quarantined too. A resync of one it does report can only fail
   * - the leader refuses to serve a quarantined database as a snapshot (issue #8468) - so retrying it on every tick
   * reads as recovery in progress while nothing can progress. Unknown is not "quarantined": a leader with no fresh
   * answer, or on a build that predates the field, leaves every database in, which is the behaviour before this.
   * <p>
   * Package-private and static so the filter can be tested without a cluster.
   */
  // @VisibleForTesting
  static Set<String> resyncableFromLeader(final Set<String> quarantined, final String leaderId,
      final PeerCapabilityRegistry registry) {
    if (quarantined.isEmpty() || leaderId == null || registry == null)
      return quarantined;
    Set<String> resyncable = null;
    for (final String dbName : quarantined)
      if (registry.reportsQuarantined(leaderId, dbName)) {
        if (resyncable == null)
          resyncable = new HashSet<>(quarantined);
        resyncable.remove(dbName);
      }
    return resyncable == null ? quarantined : resyncable;
  }

  // @VisibleForTesting
  int divergedSwallowedErrorCount() {
    return divergedSwallowedErrors.get();
  }
}
