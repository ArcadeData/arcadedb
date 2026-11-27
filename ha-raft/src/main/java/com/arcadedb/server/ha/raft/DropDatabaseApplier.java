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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.InstallApplyGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Applies the DROP_DATABASE entries of the Raft log: closes and unregisters the database, deletes its directory (now, or
 * once the files are released), and forgets every piece of per-database state the state machine kept for it, so a
 * database later created with the same name starts clean.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which dispatches each entry to the class that applies it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class DropDatabaseApplier {
  private final ArcadeStateMachine sm;

  /**
   * Removes dropped database directories away from the apply loop. Deliberately not the lifecycleExecutor: a
   * deletion is unbounded in the size of the database and would delay the snapshot-download triggers that
   * executor carries.
   */
  volatile DeferredDatabaseDeleter deferredDatabaseDeleter = new DeferredDatabaseDeleter();

  /** The drops this node's own verb is waiting on (issue #8035); see {@link #applyDropDatabaseEntry}. */
  private volatile LocalDropVerbs localDropVerbs = new LocalDropVerbs();

  /** What the entry of a drop whose files could not be removed is held for, followed by the database name (issue #12611). */
  private static final String UNDELETED_DROP_KEY_PREFIX = "drop:";

  // Apply thread only: when retryUndeletedDropsIfDue runs next, and the clock it reads
  private long         nextUndeletedDropRetryNanos;
  private LongSupplier undeletedDropRetryClock = System::nanoTime;

  DropDatabaseApplier(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Applies the DROP_DATABASE entry at {@code index}.
   *
   * @throws EntryLeftForReplayException when the files of the database could not be removed (issue #12611): the entry is
   *                                     held for the replay, see {@link #holdUndeletedDrop}
   */
  // @VisibleForTesting
  void applyDropDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long index) {
    final String databaseName = decoded.databaseName();
    // Whatever the database's pages were reserved at, the pages are going away: evict its ledger here so the
    // per-database map does not keep the names of dropped databases for the node's lifetime (same rule as the
    // persisted applied index above), and a database recreated under the same name starts from a clean ledger.
    sm.pageVersions.clear(databaseName);
    sm.schemaHistory.lastSchemaChangeIndex.remove(databaseName);
    sm.schemaHistory.recentSchemaChanges.remove(databaseName);
    // Nor the position the previous session reached on it (issue #11946): a database recreated under the same name is a new copy
    sm.appliedIndexes.forgetPositionOfPreviousSession(databaseName);
    // Nor a configuration held for the replay (issue #10086): the file it was to be written to is going away
    sm.configurationEntries.releaseHeldConfiguration(databaseName);
    // Nor a compaction hold-off (issue #9558): it described a payload of the database being dropped
    sm.clearCompactionHoldOff(databaseName);
    // Nor a compaction whose swap could not be persisted (issue #11240): there is nothing left to replay it into
    sm.applyRetry.releaseHeldEntry(ArcadeStateMachine.unpersistedPublicationKey(databaseName));
    // Nor the term of its last materialized view recovery (issue #11555): a database recreated under the same name in this
    // leadership gets the full recovery at its open, not the partial one the dropped database had earned
    sm.viewRecovery.materializedViewsRecoveredInTerm.remove(databaseName);
    // The copy the served index described is going away with it (issue #8579): a database recreated under the same
    // name must not inherit it.
    final InstallApplyGate installGate = sm.replacement.installApplyGates.get(databaseName);
    if (installGate != null)
      installGate.recordServedCopy(-1L);

    // Checked before the maintenance slot below, so replaying the entry of a database that is already gone never
    // waits on anything. "Gone" is the registry AND the disk (issue #8451): `close database` deregisters a database
    // and leaves its files in place, it is not leader-forwarded, and the next request on this node would reopen that
    // directory - so a closed database still has something to drop.
    if (!sm.server.existsDatabase(databaseName) && closedDatabaseDirectory(databaseName) == null) {
      retireAbsentDatabase(databaseName);
      return;
    }

    // This node's per-database maintenance slot, as DROP, before the database is closed and its directory renamed
    // away (issue #8035). A drop is issued on the leader, whose verb takes DROP there (#7641), but it is APPLIED on
    // every peer, and without this the apply on the others took nothing: a backup, export or import running on a
    // follower - the scheduled backup runs on every node by default, and none of those verbs is leader-forwarded -
    // had the instance it was working through closed and its directory deleted underneath it. Operation.CLOSE was
    // enrolled for exactly that reason (#7469); the replicated drop is the same close, and worse.
    //
    // On the node whose own verb submitted this entry the slot is already held, on the request thread that is
    // waiting for this very apply, and the reservation is not reentrant: taking it here would wait on itself.
    // LocalDropVerbs says whether that is the case, and holds the verb's registration across the drop so a verb
    // whose wait timed out cannot release its slot in the middle of it. Only trusted while the slot is actually
    // held by an exclusive operation: a registration without one takes the slot like any other peer.
    final BackupCoordinator coordinator = sm.server.getBackupCoordinator();
    if (coordinator != null
        && (coordinator.isInProgress(databaseName, BackupCoordinator.Operation.DROP)
        || coordinator.isInProgress(databaseName, BackupCoordinator.Operation.RESTORE))
        && localDropVerbs.runUnderLocalVerb(databaseName, () -> dropHoldingMaintenanceSlot(databaseName, index)))
      return;

    // Bounded, and proceeding when the bound expires, for the reason SnapshotInstaller.install gives for the
    // replicated restore: this applies a committed Raft entry, and a peer that declined it would diverge from the
    // cluster. The wait is the same knob, "how long a committed destructive entry waits for local maintenance".
    // A null coordinator is not a production state; the unit tests that drive this method hand it a bare server.
    final BackupCoordinator.Operation refusedBy = coordinator == null ? null
        : coordinator.begin(databaseName, BackupCoordinator.Operation.DROP,
            sm.server.getConfiguration().getValueAsLong(GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS));
    final boolean slotHeld = coordinator != null && refusedBy == null;
    if (refusedBy != null)
      LogManager.instance().log(sm, Level.WARNING,
          "Dropping database '%s' on this node while %s of it is still running: the drop applies a committed Raft "
              + "entry and cannot be declined, so it proceeds and that operation will fail or produce an incomplete "
              + "result. Raise '%s' to give it longer to finish", null, databaseName, refusedBy.phrase(),
          GlobalConfiguration.HA_SNAPSHOT_INSTALL_BACKUP_WAIT_MS.getKey());

    try {
      dropHoldingMaintenanceSlot(databaseName, index);
    } finally {
      // Only what was actually reserved: a wait that expired took nothing, and releasing then would drop the
      // reservation the operation still in flight is holding.
      if (slotHeld)
        coordinator.end(databaseName, BackupCoordinator.Operation.DROP);
    }
  }

  /**
   * The drop itself, with this node's maintenance slot already held (or deliberately given up on) by
   * {@link #applyDropDatabaseEntry}.
   */
  private void dropHoldingMaintenanceSlot(final String databaseName, final long index) {
    // Only the rename below runs on the apply thread: the recursive delete costs one unlink per file and is
    // unbounded in the size of the database, and this loop is sequential and shared by every database
    // multiplexed on the state machine. Close, deregister and rename hold the databases lock as one unit -
    // mirroring the snapshot installer's swap - so no concurrent open can reopen the directory in between.
    Path staged = null;
    UncheckedIOException undeleted = null;
    final boolean absent;
    synchronized (sm.server.getDatabasesLock()) {
      final Path databaseDirectory;
      if (sm.server.existsDatabase(databaseName)) {
        // Resolved inside the lock: getDatabase reopens a database that is registered-but-closed, so resolving
        // it outside would let another holder of this lock deregister it between the lookup and the close, and
        // this thread would reopen the directory from disk only to close it again. Through databaseFor, which does not
        // reopen it while the node is shutting down (issue #9550): the drop is then left for the replay on restart.
        final DatabaseInternal embedded = sm.databaseFor(databaseName).getEmbedded();
        databaseDirectory = Path.of(embedded.getDatabasePath());
        embedded.closeForDrop();
        sm.server.removeDatabase(databaseName);
      } else {
        // Deregistered, either before this entry arrived or while the apply waited for the slot: a `close
        // database` leaves the files on disk, and getDatabase would reopen them on the next request (issue
        // #8451). Deleted from the configured directory WITHOUT opening it - opening only to drop would replay
        // its WAL and take its file locks for nothing.
        databaseDirectory = closedDatabaseDirectory(databaseName);
        if (databaseDirectory != null)
          closeUnregisteredInstance(databaseDirectory);
      }
      // stageForDeletion falls back to deleting inline when the rename is impossible, and that fallback
      // belongs inside the lock even though it is slow: the directory still carries its live name, so
      // releasing the lock first would let a concurrent create of the same name meet a half-deleted one.
      if (databaseDirectory != null)
        try {
          staged = deferredDatabaseDeleter.stageForDeletion(databaseDirectory);
        } catch (final UncheckedIOException e) {
          undeleted = e;
        }
      absent = databaseDirectory == null;
      // A drop ends the close that `close database` asked for, and only removeDatabase ends it (issue #11739): without it GET
      // /exists kept answering true for a database the cluster had dropped. Idempotent, and it lists the directories again after
      // the rename. Not when the inline delete of stageForDeletion left the directory behind (issue #12611): the database is
      // then marked closed, whether a `close database` had marked it or it was open until this drop closed it, so no request
      // reopens the copy the cluster dropped. Under the lock, like the deregistration it goes with.
      if (undeleted == null)
        sm.server.removeDatabase(databaseName);
      else
        sm.server.markClosedByCommand(databaseName);
    }
    if (undeleted != null)
      // Before the baseline is evicted and the quarantine retired below: this database was not dropped here
      throw holdUndeletedDrop(databaseName, index, undeleted);
    if (absent) {
      // Neither registered nor on disk any more: the removal this entry asks for already happened elsewhere.
      retireAbsentDatabase(databaseName);
      return;
    }
    // Queued outside the lock: a saturated deletion queue runs the delete on this thread, and that must not
    // extend to holding the databases lock for the length of a recursive delete.
    if (staged != null)
      deferredDatabaseDeleter.deleteInBackground(staged);

    // Evict AFTER the drop succeeded, mirroring the applied-index drop eviction which runs only once
    // apply completes: if drop() had thrown and quarantined this database, the baseline must stay so a
    // restart can still recover it (evicting first would lose the baseline of a database that was not
    // actually dropped - the #5100 failure mode).
    sm.baselines.evictBootstrapBaseline(databaseName);
    clearDroppedDatabaseQuarantine(databaseName);
    // A drop of the name completed: an earlier one whose files could not be removed has nothing left to replay
    sm.applyRetry.releaseHeldEntry(undeletedDropKey(databaseName));

    LogManager.instance().log(sm, Level.INFO, "Database '%s' dropped via Raft drop-database entry%s", databaseName,
        staged != null ? " (files staged as '" + staged.getFileName() + "' for background deletion)" : "");
  }

  /**
   * Holds the DROP entry at {@code index} for the replay because the files of {@code databaseName} are still on disk under
   * its name (issue #12611): the rename aside was not possible and the inline deletion was refused (a read-only or faulty
   * filesystem, a permission problem). Reported as applied, the entry left nothing {@code takeSnapshot()} consults, so the
   * next checkpoint let Ratis purge it, and after a restart, or once the filesystem recovered, this node served a database
   * the rest of the cluster had dropped.
   * <p>
   * Held, not quarantined, by the rule of {@code ha-raft/CLAUDE.md}: a quarantine is lifted by a resync, and the leader has
   * no copy of a dropped database to resync from. Its targeted resync would end in "the leader does not hold it", lift the
   * quarantine and leave the files and a purgeable entry, having held the whole node out of the ready set meanwhile. The
   * hold lasts until a drop of the name completes: {@link #retryUndeletedDropsIfDue} tries again, a restart replays the
   * entry, or a Raft install covers it.
   */
  private EntryLeftForReplayException holdUndeletedDrop(final String databaseName, final long index,
      final UncheckedIOException undeleted) {
    // Not on the very next entry: the filesystem that has just refused the deletion gets an interval to recover
    nextUndeletedDropRetryNanos = undeletedDropRetryClock.getAsLong()
        + TimeUnit.MILLISECONDS.toNanos(SecurityDurabilityFloor.RETRY_INTERVAL_MS);
    return sm.applyRetry.holdEntryForReplay(undeletedDropKey(databaseName), index, "Database '" + databaseName
        + "' was dropped by the cluster but its files could not be removed from this node (" + undeleted.getCause().getMessage()
        + "): it stays closed here and the drop is tried again", undeleted);
  }

  static String undeletedDropKey(final String databaseName) {
    return UNDELETED_DROP_KEY_PREFIX + databaseName;
  }

  /**
   * Tries again, at most once per {@link SecurityDurabilityFloor#RETRY_INTERVAL_MS}, to remove the files of every database
   * whose drop is held because they could not be removed (issue #12611), so a filesystem that takes the deletion again stops
   * holding the Raft log without waiting for a restart to replay the entry. Costs one map check while nothing is held.
   * <p>
   * On the apply thread and driven by applied entries, as {@link ConfigurationEntryApplier#retryHeldConfigurationsIfDue} is,
   * so no apply of the same database can interleave with it; blocking included, like that retry.
   */
  void retryUndeletedDropsIfDue() {
    final Map<String, Long> held = sm.applyRetry.heldEntryFloors;
    if (held.isEmpty())
      return;

    final long now = undeletedDropRetryClock.getAsLong();
    if (now - nextUndeletedDropRetryNanos < 0)
      return;
    nextUndeletedDropRetryNanos = now + TimeUnit.MILLISECONDS.toNanos(SecurityDurabilityFloor.RETRY_INTERVAL_MS);

    for (final Map.Entry<String, Long> entry : held.entrySet()) {
      if (!entry.getKey().startsWith(UNDELETED_DROP_KEY_PREFIX))
        continue;
      final String databaseName = entry.getKey().substring(UNDELETED_DROP_KEY_PREFIX.length());
      try {
        retryUndeletedDrop(databaseName, entry.getValue());
      } catch (final RuntimeException e) {
        // Runs inside applyTransaction, whose catch-all halts the node: a retry that cannot run is a retry for later
        LogManager.instance().log(sm, Level.FINE, "The files of dropped database '%s' still cannot be removed: %s",
            databaseName, e.getMessage());
      }
    }
  }

  /** One retry of {@link #retryUndeletedDropsIfDue}; throws when the files are still there. */
  private void retryUndeletedDrop(final String databaseName, final long index) {
    if (sm.appliedIndexes.isOfEarlierIncarnation(databaseName, index)) {
      // A copy created or acquired after the held drop stands under the name: a replay would skip the entry (issue #11919)
      sm.applyRetry.releaseHeldEntry(undeletedDropKey(databaseName));
      return;
    }

    final Path staged;
    synchronized (sm.server.getDatabasesLock()) {
      // Registered again, by an operator's `open database`: not closed under them here, the replay on restart drops it
      if (sm.server.existsDatabase(databaseName))
        return;
      final Path databaseDirectory = closedDatabaseDirectory(databaseName);
      if (databaseDirectory == null)
        staged = null;
      else {
        closeUnregisteredInstance(databaseDirectory);
        staged = deferredDatabaseDeleter.stageForDeletion(databaseDirectory);
      }
      // Ends the close mark the failed drop left
      sm.server.removeDatabase(databaseName);
    }
    if (staged != null)
      deferredDatabaseDeleter.deleteInBackground(staged);

    // What the apply of the entry would have done had the deletion succeeded then; the global position moved on since
    sm.appliedIndexes.writePersistedDatabaseAppliedIndex(index, databaseName, true);
    sm.baselines.evictBootstrapBaseline(databaseName);
    clearDroppedDatabaseQuarantine(databaseName);
    sm.applyRetry.releaseHeldEntry(undeletedDropKey(databaseName));
    LogManager.instance().log(sm, Level.INFO,
        "The files of database '%s', dropped by the Raft entry at index %d, are removed from this node now; snapshot "
            + "checkpoints may cover the entry (issue #12611)", databaseName, index);
  }

  /** Paces {@link #retryUndeletedDropsIfDue} on {@code clock} instead of {@link System#nanoTime()}, for a test. */
  // @VisibleForTesting
  void setUndeletedDropRetryClock(final LongSupplier clock) {
    this.undeletedDropRetryClock = clock;
  }

  /**
   * Idempotent on replay: if the database is already gone, nothing to do beyond evicting any persisted baseline.
   * applyBootstrapFingerprintEntry records a baseline by name even when the database is not present locally (the
   * late-joiner path), so a node can hold a persisted baseline for a database it never had locally; evict it here so
   * it does not linger in the file for the node lifetime. This branch never calls drop(), so the eviction cannot
   * precede a failed drop.
   */
  private void retireAbsentDatabase(final String databaseName) {
    sm.baselines.evictBootstrapBaseline(databaseName);
    clearDroppedDatabaseQuarantine(databaseName);
    // Nor a drop held because its files could not be removed (issue #12611): they are gone now
    sm.applyRetry.releaseHeldEntry(undeletedDropKey(databaseName));
    HALog.log(sm, HALog.TRACE, "Database '%s' already absent, skipping drop-database entry", databaseName);
  }

  /**
   * The on-disk directory of a database that is NOT in the server registry, or {@code null} when there is none
   * (issue #8451). It is the directory {@code ArcadeDBServer.getDatabase} would load the name from, so it is exactly
   * what a later request on this node would reopen.
   * <p>
   * The name arrives in a committed log entry rather than in a request this node validated, and what is found here is
   * deleted, so a name the server would refuse to open - one that resolves outside the databases directory - is
   * reported as having no directory rather than trusted.
   * <p>
   * Only a path that is confirmed missing counts as "no directory". A stat that fails for any other reason (permission,
   * I/O error) fails the apply instead: reading it as absent would retire the database's bookkeeping and advance the
   * applied index past the drop while its files are still there, to be reopened once the filesystem recovers.
   *
   * @throws UncheckedIOException when the filesystem cannot say whether the directory exists
   */
  private Path closedDatabaseDirectory(final String databaseName) {
    try {
      sm.server.checkDatabaseNameIsValid(databaseName);
    } catch (final IllegalArgumentException e) {
      LogManager.instance().log(sm, Level.WARNING,
          "Drop-database entry names '%s', which is not a valid database name on this node: nothing on disk is "
              + "removed for it (%s)", databaseName, e.getMessage());
      return null;
    }
    final Path directory = Path.of(sm.server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY),
        databaseName);
    final BasicFileAttributes attributes;
    try {
      attributes = Files.readAttributes(directory, BasicFileAttributes.class);
    } catch (final NoSuchFileException e) {
      return null;
    } catch (final IOException e) {
      // Deliberately NOT a ReplicationException: applyWithRetry rethrows that one unchanged, so the database would be
      // neither quarantined nor resynced, and the next entry to apply would checkpoint straight past this drop. A plain
      // runtime exception takes the handleUnexpectedApplyError path: quarantine, targeted resync, and no snapshot while
      // the quarantine holds, so the entry stays replayable.
      throw new UncheckedIOException(
          "Cannot tell whether database '" + databaseName + "' still has files at '" + directory + "' to drop", e);
    }
    return attributes.isDirectory() ? directory : null;
  }

  /**
   * Closes an instance of {@code databaseDirectory} that is still open in this JVM although the server no longer
   * registers it - a close that failed half way, or an embedded open outside the server - so its files are not
   * renamed from under open file handles. A no-op in the ordinary case, where {@code close database} closed it.
   */
  private static void closeUnregisteredInstance(final Path databaseDirectory) {
    final Database active = DatabaseFactory.getActiveDatabaseInstance(databaseDirectory.toString());
    if (active == null || !active.isOpen())
      return;
    try {
      ((DatabaseInternal) active).getEmbedded().closeForDrop();
    } catch (final DatabaseIsClosedException e) {
      // Its holder is not serialized by the databases lock - it is not registered - so it can close it between the
      // isOpen() above and closeForDrop(). Closed is exactly what this method wanted; anything else still fails.
      if (active.isOpen())
        throw e;
    }
  }

  /**
   * Retires the quarantine bookkeeping of a database a DROP entry has just removed: the per-database read floor
   * (issue #6760) and the diverged marker that goes with it.
   * <p>
   * Both exist to hold back readers of a database a snapshot install could NOT bring up to date, until a targeted
   * resync restores it. A dropped database has no such obligation left - there is nothing to resync and nothing to
   * read - and the floor is only ever cleared by {@link ArcadeStateMachine#clearDivergedDatabase}, which runs when a resync succeeds.
   * So without this, the floor of a database that was quarantined when it was dropped outlives it for the node's
   * lifetime, with two consequences (review of PR #7649):
   * <ul>
   *   <li>{@link RaftHAServer#getTrustedAppliedIndex(String)} clamps to that floor forever, so the local-apply wait
   *   {@code RaftReplicatedDatabase.dropInReplicas} takes could never be satisfied - the drop would report failure
   *   through its full quorum timeout even though the directory is gone;</li>
   *   <li>a database later recreated under the same name would inherit the dead floor and have its LINEARIZABLE
   *   reads pinned behind it.</li>
   * </ul>
   * The same reasoning the eviction of {@code pageVersions} and the bootstrap baseline in this method already
   * applies - do not keep per-database state for a name that no longer has a database.
   */
  private void clearDroppedDatabaseQuarantine(final String databaseName) {
    if (sm.getDatabaseAppliedFloor(databaseName) < 0 && !sm.isDatabaseDiverged(databaseName))
      return;

    HALog.log(sm, HALog.BASIC,
        "Database '%s' was quarantined when it was dropped: retiring its read floor and diverged marker, "
            + "since a dropped database has no resync left to wait for (issue #6760)", databaseName);
    sm.clearDivergedDatabase(databaseName);
  }

  /**
   * Shares the owning {@link RaftHAServer}'s registry of local drop verbs, which outlives this state machine across a
   * Ratis restart. Left at its own empty registry by the unit tests that build a state machine without one.
   */
  void setLocalDropVerbs(final LocalDropVerbs localDropVerbs) {
    this.localDropVerbs = localDropVerbs;
  }

  // @VisibleForTesting
  void setDeferredDatabaseDeleter(final DeferredDatabaseDeleter deleter) {
    final DeferredDatabaseDeleter previous = this.deferredDatabaseDeleter;
    this.deferredDatabaseDeleter = deleter;
    if (previous != null && previous != deleter)
      previous.close();
  }
}
