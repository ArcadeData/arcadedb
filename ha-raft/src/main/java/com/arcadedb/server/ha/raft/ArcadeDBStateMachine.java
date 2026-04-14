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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ReplicationCallback;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Ratis state machine for ArcadeDB replication. Each committed Raft log entry contains a serialized
 * database transaction (WAL page diffs) that is applied identically on all follower nodes.
 *
 * <p>On the originating leader, {@code applyTransaction()} fires (step 4 in the sequence below)
 * BEFORE {@code commit2ndPhase()} runs (step 7). The origin-skip optimization skips the state
 * machine apply on the leader to prevent double-application once {@code commit2ndPhase()} runs.
 * For ALL quorum, if the watch fails after MAJORITY ack, {@link ReplicatedDatabase} catches
 * {@link MajorityCommittedAllFailedException} and still calls {@code commit2ndPhase()} to keep
 * the leader database consistent with its own Raft log.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeDBStateMachine extends BaseStateMachine implements org.apache.ratis.statemachine.StateMachine.EventApi {

  /**
   * Tolerance for the snapshot gap check in {@link #reinitialize()}. During normal shutdown the
   * persisted applied index may lag a few entries behind the snapshot index because
   * {@link #takeSnapshot()} and {@link #persistAppliedIndex(long)} are not atomic with respect
   * to each other. A small gap (within this tolerance) is harmless and does not indicate missing
   * data, so we avoid an unnecessary full snapshot download on restart.
   */
  private static final long SNAPSHOT_GAP_TOLERANCE = 10;

  private final ArcadeDBServer            server;
  private final RaftHAServer              raftHA;
  private final SimpleStateMachineStorage storage          = new SimpleStateMachineStorage();
  private final AtomicLong                lastAppliedIndex = new AtomicLong(-1);
  private final AtomicLong                electionCount    = new AtomicLong(0);
  private volatile long                   lastElectionTime = 0;
  private volatile long                   startTime        = System.currentTimeMillis();
  /** True when this follower is replaying log entries to catch up after being behind. */
  private final AtomicBoolean             catchingUp       = new AtomicBoolean(false);
  /** Set by reinitialize() when a snapshot gap is detected, cleared by notifyLeaderChanged() via compareAndSet. */
  private final AtomicBoolean             needsSnapshotDownload  = new AtomicBoolean(false);
  /** Cached leader flag, updated by notifyLeaderChanged(). Used for non-critical checks (e.g. catch-up detection).
   *  The correctness-critical origin-skip in applyTransactionEntry() uses raftHA.isLeader() directly to avoid stale reads. */
  private volatile boolean                currentNodeIsLeader    = false;
  /** Executor for async lifecycle tasks (snapshot download, Ratis restart) so they can be awaited on close. */
  private final ExecutorService           lifecycleExecutor      = Executors.newSingleThreadExecutor(
      r -> { final Thread t = new Thread(r, "arcadedb-sm-lifecycle"); t.setDaemon(true); return t; });
  private final SnapshotInstaller         snapshotInstaller;

  public ArcadeDBStateMachine(final ArcadeDBServer server, final RaftHAServer raftHA) {
    this.server = server;
    this.raftHA = raftHA;
    this.snapshotInstaller = (server != null && raftHA != null) ? new SnapshotInstaller(server, raftHA) : null;
  }

  /** Returns the lifecycle executor for scheduling async tasks (leader catch-up, snapshot download). */
  public ExecutorService getLifecycleExecutor() {
    return lifecycleExecutor;
  }

  @Override
  public void initialize(final RaftServer raftServer, final RaftGroupId groupId, final RaftStorage raftStorage) throws IOException {
    super.initialize(raftServer, groupId, raftStorage);
    storage.init(raftStorage);
    reinitialize();
    LogManager.instance().log(this, Level.INFO, "ArcadeDB Raft state machine initialized (groupId=%s)", groupId);
  }

  @Override
  public void close() throws IOException {
    lifecycleExecutor.shutdownNow();
    try {
      if (!lifecycleExecutor.awaitTermination(10, TimeUnit.SECONDS))
        LogManager.instance().log(this, Level.WARNING, "Lifecycle executor did not terminate within 10 seconds");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    super.close();
  }

  /**
   * Called by Ratis before snapshot installation. Intentionally a no-op: we do not need to block
   * applyTransaction() here because the actual snapshot install (HTTP download + directory swap)
   * is deferred to notifyLeaderChanged() and uses a crash-safe marker file, not in-memory state.
   * The gap detection in {@link #reinitialize()} handles the trigger.
   */
  @Override
  public void pause() {
    LogManager.instance().log(this, Level.INFO, "State machine paused by Ratis for snapshot installation");
  }

  @Override
  public void reinitialize() throws IOException {
    final var snapshotInfo = storage.getLatestSnapshot();
    if (snapshotInfo != null) {
      final long snapshotIndex = snapshotInfo.getIndex();

      // Check if the snapshot index is ahead of what we last persisted as applied.
      // This detects chunk-based snapshot installation: Ratis installs a marker file (updating
      // the snapshot index) but the database doesn't have the actual data. We must download
      // the database from the leader to bridge the gap.
      // The download is deferred to notifyLeaderChanged() because during reinitialize()
      // the Ratis server hasn't joined the cluster yet and the leader is unknown.
      final long persistedApplied = readPersistedAppliedIndex();
      if (persistedApplied >= 0 && snapshotIndex > persistedApplied + SNAPSHOT_GAP_TOLERANCE) {
        LogManager.instance().log(this, Level.INFO,
            "Snapshot index %d is ahead of persisted applied index %d, will download from leader when available",
            snapshotIndex, persistedApplied);
        needsSnapshotDownload.set(true);

        // Watchdog: if notifyLeaderChanged() doesn't fire within 30 seconds (e.g., stable
        // leader, no election), trigger the download directly. This prevents a follower from
        // remaining permanently stale when the leader is stable.
        // Submitted to lifecycleExecutor so close() can interrupt it via shutdownNow().
        lifecycleExecutor.submit(() -> {
          try {
            Thread.sleep(30_000);
            if (needsSnapshotDownload.compareAndSet(true, false)) {
              LogManager.instance().log(this, Level.WARNING,
                  "Snapshot download watchdog: no leader change after 30s, triggering download directly");
              if (snapshotInstaller != null)
                snapshotInstaller.installDatabasesFromLeader();
            }
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE,
                "Snapshot download watchdog failed", e);
          }
        });
      }

      lastAppliedIndex.set(snapshotIndex);
      updateLastAppliedTermIndex(snapshotInfo.getTerm(), snapshotIndex);
    } else
      lastAppliedIndex.set(-1);
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  // -- Transaction Application --

  @Override
  public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
    final var logEntry = trx.getLogEntry();
    final long index = logEntry.getIndex();

    try {
      final ByteBuffer entryData = logEntry.getStateMachineLogEntry().getLogData().asReadOnlyByteBuffer();
      final byte[] data = new byte[entryData.remaining()];
      entryData.get(data);

      final RaftLogEntryType type = RaftLogEntryCodec.readType(ByteBuffer.wrap(data));

      if (type == null) {
        // Unrecognized entry type - likely from a newer node during a rolling upgrade.
        // Skip it so the state machine stays healthy; the entry will be applied by nodes
        // that understand this type.
        LogManager.instance().log(this, Level.WARNING,
            "Skipping unrecognized Raft log entry at index %d (type byte: %d). "
                + "This node may be running an older version than the entry's author",
            index, data.length > 0 ? data[0] : -1);
        lastAppliedIndex.set(index);
        updateLastAppliedTermIndex(logEntry.getTerm(), index);
        return CompletableFuture.completedFuture(Message.EMPTY);
      }

      HALog.log(this, HALog.TRACE, "applyTransaction: index=%d, type=%s, server=%s", index, type, server.getServerName());

      switch (type) {
        case CREATE_DATABASE -> applyCreateDatabase(data);
        case DROP_DATABASE -> applyDropDatabase(data);
        case TRANSACTION -> applyTransactionEntry(data);
        case CREATE_USER -> applyCreateUser(data);
        case UPDATE_USER -> applyUpdateUser(data);
        case DROP_USER -> applyDropUser(data);
      }

      final long previousApplied = lastAppliedIndex.getAndSet(index);
      updateLastAppliedTermIndex(logEntry.getTerm(), index);

      // Wake up any threads waiting for this index (READ_YOUR_WRITES, waitForLocalApply)
      final RaftHAServer raftHA = this.raftHA;
      if (raftHA != null) {
        raftHA.notifyApplied();

        // Detect hot resync (log replay catch-up) on followers.
        // When a follower applies multiple entries in rapid succession (gap > 1 between
        // consecutive applies), it's catching up. When it reaches the commit index, it's done.
        if (!isCurrentNodeLeader()) {
          final long gap = index - previousApplied;
          if (gap > 1 && catchingUp.compareAndSet(false, true))
            HALog.log(this, HALog.BASIC, "Follower catching up: gap=%d (previous=%d, current=%d)", gap, previousApplied, index);
          if (catchingUp.get()) {
            final long commitIndex = raftHA.getCommitIndex();
            if (commitIndex > 0 && index >= commitIndex) {
              catchingUp.set(false);
              HALog.log(this, HALog.BASIC, "Hot resync complete: applied=%d >= commit=%d", index, commitIndex);
              fireCallback(ReplicationCallback.TYPE.REPLICA_HOT_RESYNC, server.getServerName());
            }
          }
        }
      }

      return CompletableFuture.completedFuture(Message.EMPTY);

    } catch (final ReplicationException | IllegalArgumentException | IllegalStateException e) {
      // Expected errors (database unavailable, corrupted entry, unknown value type).
      // Advance applied index: Ratis has committed this entry and won't re-apply it.
      // The snapshot resync will bring the correct state from the leader.
      LogManager.instance().log(this, Level.SEVERE,
          "Error applying Raft log entry at index %d. Triggering snapshot resync to recover state.", e, index);
      lastAppliedIndex.set(index);
      updateLastAppliedTermIndex(logEntry.getTerm(), index);

      // On followers, trigger snapshot resync. The needsSnapshotDownload flag debounces multiple
      // failures: only the first successful compareAndSet triggers a download.
      //
      // Note: compareAndSet(true, false) clears the flag before the download runs. If the
      // download itself fails, the flag stays false until the next apply failure re-arms it
      // (line below sets it to true). On a quiet cluster with no new writes, no new log entry
      // will arrive to trigger re-arming, so the follower remains diverged until either:
      //   1. The leader sends new entries (which will also fail to apply, re-arming the flag), or
      //   2. The HealthMonitor detects the follower lag and triggers corrective action.
      if (!isCurrentNodeLeader()) {
        needsSnapshotDownload.set(true);
        lifecycleExecutor.submit(() -> {
          if (needsSnapshotDownload.compareAndSet(true, false)) {
            try {
              if (snapshotInstaller != null)
                snapshotInstaller.installDatabasesFromLeader();
            } catch (final Exception ex) {
              LogManager.instance().log(this, Level.SEVERE,
                  "Snapshot resync after failed apply failed", ex);
            }
          }
        });
      }

      // Return a completed future: Ratis has committed the entry and the applied index has been
      // advanced. Returning a failed future would violate the BaseStateMachine contract and may
      // cause Ratis to enter an unexpected state. Recovery happens out-of-band via snapshot.
      return CompletableFuture.completedFuture(Message.EMPTY);
    } catch (final Throwable e) {
      // Unexpected errors (NPE, ClassCastException, OOM, etc.) indicate a bug that could cause
      // state divergence if silently swallowed. Crash the state machine so the node recovers
      // via snapshot rather than continuing with potentially inconsistent state.
      LogManager.instance().log(this, Level.SEVERE,
          "CRITICAL: Unexpected error applying Raft log entry at index %d. "
              + "Shutting down to prevent state divergence.", e, index);
      // Schedule stop on a separate thread to avoid deadlock: server.stop() closes the
      // Ratis server, which may try to acquire locks held by the current applyTransaction
      // callback thread.
      final Thread stopThread = new Thread(() -> {
        try {
          server.stop();
        } catch (final Throwable t) {
          LogManager.instance().log(this, Level.SEVERE, "Emergency stop failed", t);
        }
      }, "arcadedb-emergency-stop");
      stopThread.setDaemon(true);
      stopThread.start();
      return CompletableFuture.failedFuture(e instanceof Exception ex ? ex : new RuntimeException(e));
    }
  }

  /**
   * Applies a replicated transaction entry on a follower (or on a restarted leader replaying its log).
   *
   * <p>Execution proceeds in three phases:
   * <ol>
   *   <li><strong>Phase 1:</strong> Create physical files referenced by the WAL (idempotent)</li>
   *   <li><strong>Phase 2:</strong> Apply WAL page changes (idempotent via page-version guard)</li>
   *   <li><strong>Phase 3:</strong> Update schema metadata and remove dropped files</li>
   * </ol>
   *
   * <p><strong>Follower isolation caveat:</strong> between Phase 2 and the deferred schema reload,
   * concurrent readers may see committed page data but stale schema metadata. This window is
   * sub-millisecond and consistent with follower eventual-consistency guarantees. Schema-dependent
   * queries that observe this window will succeed on retry.
   */
  private void applyTransactionEntry(final byte[] data) {
    final RaftLogEntryCodec.TransactionEntry entry = RaftLogEntryCodec.deserializeTransaction(data);

    final DatabaseInternal db = server.getDatabase(entry.databaseName());
    if (db == null || !db.isOpen())
      throw new ReplicationException("Database '" + entry.databaseName() + "' is not available");

    // The originating leader will apply changes locally via commit2ndPhase() after replicateTransaction()
    // returns. Skip the state machine apply here to avoid double-applying page changes.
    //
    // Execution order on the leader (important for understanding the skip logic):
    //   1. commit1stPhase() - WAL prepared, pages not yet written
    //   2. replicateTransaction() blocks waiting for Raft commit
    //   3. Raft gets MAJORITY ack -> commits the entry
    //   4. applyTransaction() fires on this state machine (HERE) - skip fires if isLeader()
    //   5. Ratis sends client reply (completing the future in replicateTransaction)
    //   6. replicateTransaction() returns to the caller
    //   7. commit2ndPhase() writes pages locally
    //
    // So applyTransaction() runs BEFORE commit2ndPhase(), not after.
    //
    // The skip only fires when BOTH conditions are true:
    //   1. This node's peer ID matches the originPeerId embedded in the log entry
    //   2. This node is currently the leader
    //
    // Condition 2 is critical for restart safety: if the leader crashes between Raft commit and
    // commit2ndPhase() (step 3-7 above), the entry is in the Raft log but pages were never written.
    // On restart, lastAppliedIndex is restored from the snapshot (not from in-memory state at crash
    // time), so Ratis replays the entry from snapshot+1. Because isLeader() returns false on restart,
    // the skip does NOT fire and the entry is applied via the normal follower path.
    //
    // ALL quorum TOCTOU: when ALL quorum is configured, step 3 fires applyTransaction() (skip)
    // but step 5 may fail if the ALL watch times out. ReplicatedDatabase catches
    // MajorityCommittedAllFailedException and still calls commit2ndPhase() to prevent divergence.
    //
    // We call raftHA.isLeader() (which queries Ratis's internal role state directly) rather than
    // the cached currentNodeIsLeader field. The cached field is updated by notifyLeaderChanged()
    // on a separate thread and could be stale during a concurrent leadership transfer.
    if (isOriginNode(entry.originPeerId())) {
      if (raftHA != null && raftHA.isLeader()) {
        HALog.log(this, HALog.TRACE, "Skipping WAL apply on origin node (commit2ndPhase handles it): db=%s", entry.databaseName());
        return;
      }
      if (raftHA == null)
        LogManager.instance().log(this, Level.WARNING,
            "Origin node match but raftHA is null - cannot determine leadership, applying entry defensively (db=%s)",
            entry.databaseName());
    }
    HALog.log(this, HALog.DETAILED, "Applying WAL on follower: db=%s, walSize=%d, deltaSize=%d, hasSchema=%s",
        entry.databaseName(), entry.walBuffer() != null ? entry.walBuffer().size() : 0,
        entry.bucketRecordDelta().size(), entry.schemaJson() != null);

    boolean needsSchemaReload = false;

    // Phase 1: Create physical files first - WAL pages may reference new file IDs.
    // This must happen before WAL apply so that page writes find the target files.
    // Schema reload is deferred to after all phases to avoid a window where schema
    // is ahead of WAL data for concurrent readers.
    if (entry.filesToAdd() != null && !entry.filesToAdd().isEmpty()) {
      try {
        createNewFiles(db, entry.filesToAdd());
        needsSchemaReload = true;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error creating files from Raft log", e);
        throw new ReplicationException("Error creating files from Raft log", e);
      }
    }

    // Phase 2: Apply WAL page changes (if any - schema-only entries have empty WAL buffer).
    // New file IDs are already registered in FileManager (Phase 1) so page writes succeed.
    // Schema component lookups (for page count / vector index updates) gracefully handle
    // missing entries via getFileByIdIfExists(), and will be correct after the final reload.
    if (entry.walBuffer() != null && entry.walBuffer().size() > 0) {
      final WALFile.WALTransaction walTx = RaftLogEntryCodec.parseWalTransaction(entry.walBuffer());

      LogManager.instance().log(this, Level.FINE, "Applying Raft tx %d (modifiedPages=%d, db=%s)...", walTx.txId,
          walTx.pages.length, entry.databaseName());

      try {
        db.getTransactionManager().applyChanges(walTx, entry.bucketRecordDelta(), false);
      } catch (final com.arcadedb.exception.WALVersionGapException e) {
        // Version gap: WAL page version > DB page version + 1 - an intermediate transaction
        // was never applied on this node. State has diverged; trigger snapshot resync via the
        // outer ReplicationException catch rather than continuing with inconsistent page data.
        LogManager.instance().log(this, Level.SEVERE,
            "WAL version gap on follower - state divergence detected, triggering snapshot resync (db=%s, txId=%d): %s",
            entry.databaseName(), walTx.txId, e.getMessage());
        throw new ReplicationException("WAL version gap detected - snapshot resync required (db=" + entry.databaseName() + ")", e);
      } catch (final com.arcadedb.exception.ConcurrentModificationException e) {
        // Benign replay: WAL page version <= DB page version - the entry was already applied
        // (via WAL recovery or a prior commit). Expected after cold restart or snapshot install.
        LogManager.instance().log(this, Level.WARNING,
            "Skipping already-applied WAL entry on follower (db=%s, txId=%d): %s",
            entry.databaseName(), walTx.txId, e.getMessage());
      }
    }

    // Phase 3: Finalize schema - update metadata and remove dropped files AFTER WAL is safely applied.
    //
    // Crash-safety analysis for both failure directions:
    //
    // (a) Crash BEFORE Phase 2 (WAL not yet applied, schema not yet updated):
    //     Ratis replays the entry on restart. All phases run normally. No inconsistency.
    //
    // (b) Crash AFTER Phase 2 but BEFORE Phase 3 (pages written, schema.json not updated):
    //     The local WAL recovery (TransactionManager.checkIntegrity) does NOT cover this case:
    //     applyChanges() writes directly to page files without going through the local WAL, so
    //     there is no .wal file to replay. Recovery is handled exclusively by Ratis log replay:
    //     - lastAppliedIndex is updated only AFTER all phases complete (see the caller), so the
    //       snapshot never marks this entry as applied if Phase 3 did not finish.
    //     - On restart, Ratis replays from the snapshot's last applied index, which excludes this entry.
    //     - Phase 1 is idempotent (file-existence guard). Phase 2 is idempotent (page-version guard
    //       in applyChanges() throws ConcurrentModificationException for already-applied pages).
    //     - Phase 3 runs and writes the missing schema update. Inconsistency is resolved.
    //
    // (c) Crash AFTER Phase 3 but Phase 1 file not flushed (schema-ahead-of-data):
    //     Same Ratis replay path. Phase 1's existence guard skips already-created files.
    //     Phase 2's version guard skips already-applied pages. Phase 3 is idempotent (overwrites
    //     schema.json with the same content). No harm.
    //
    // In all cases the recovery path is Ratis log replay, not WAL recovery.
    if (entry.schemaJson() != null) {
      try {
        removeDroppedFiles(db, entry.filesToRemove());
        updateSchemaMetadata(db, entry.schemaJson());
        needsSchemaReload = true;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error applying schema changes from Raft log", e);
        throw new ReplicationException("Error applying schema changes from Raft log", e);
      }
    }

    // Single schema reload after all phases complete. This avoids:
    // 1. Double reload when a transaction has both new files and schema changes
    // 2. A window where schema is ahead of WAL data for concurrent readers
    //
    // Follower isolation note: between Phase 2 (WAL apply) and this schema reload, concurrent
    // readers on the follower may observe newly written page data (e.g., a new bucket's pages)
    // but the schema does not yet reflect the new type or property. Reads that go through the
    // schema layer (type.getProperty(), bucket lookup by type) may return stale metadata during
    // this brief window. Direct page/record reads by RID are unaffected since they bypass the
    // schema. This is acceptable because:
    //   - Followers provide eventual consistency, not snapshot isolation
    //   - The window is bounded by a single schema reload (typically sub-millisecond)
    //   - Schema-dependent queries that fail during this window will succeed on retry
    if (needsSchemaReload) {
      try {
        db.getSchema().getEmbedded().load(ComponentFile.MODE.READ_WRITE, false);
        db.getSchema().getEmbedded().initComponents();
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error reloading schema after Raft log apply", e);
        throw new ReplicationException("Error reloading schema after Raft log apply", e);
      }
    }

    // Fire REPLICA_MSG_RECEIVED callback for test infrastructure
    fireCallback(ReplicationCallback.TYPE.REPLICA_MSG_RECEIVED, entry.databaseName());
  }

  private void applyCreateDatabase(final byte[] data) {
    final RaftLogEntryCodec.CreateDatabaseEntry entry = RaftLogEntryCodec.deserializeCreateDatabase(data);

    // The originating leader already created the database locally before submitting the Ratis entry.
    // We also require isLeader() for the same reason as applyTransactionEntry: if the leader crashes
    // before or after local creation and later rejoins as a follower, isOriginNode() would still be
    // true but the database may never have been created. The existence check below handles idempotency.
    if (isOriginNode(entry.originPeerId()) && raftHA != null && raftHA.isLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping CREATE_DATABASE on origin leader (already created): db=%s", entry.databaseName());
      return;
    }

    if (server.existsDatabase(entry.databaseName())) {
      HALog.log(this, HALog.BASIC, "Database '%s' already exists on this follower, skipping create", entry.databaseName());
      return;
    }

    HALog.log(this, HALog.BASIC, "Creating database '%s' on follower (replicated from leader)", entry.databaseName());
    server.createDatabase(entry.databaseName(), ComponentFile.MODE.READ_WRITE);
  }

  private void applyDropDatabase(final byte[] data) {
    final RaftLogEntryCodec.DropDatabaseEntry entry = RaftLogEntryCodec.deserializeDropDatabase(data);

    // The originating leader already dropped the database locally before submitting the Ratis entry.
    // We also require isLeader() for the same reason as applyTransactionEntry: if the leader crashes
    // before or after local drop and later rejoins as a follower, isOriginNode() would still be true
    // but the database may still exist locally. The existence check below handles idempotency.
    if (isOriginNode(entry.originPeerId()) && raftHA != null && raftHA.isLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping DROP_DATABASE on origin leader (already dropped): db=%s", entry.databaseName());
      return;
    }

    if (!server.existsDatabase(entry.databaseName())) {
      HALog.log(this, HALog.BASIC, "Database '%s' does not exist on this follower, skipping drop", entry.databaseName());
      return;
    }

    HALog.log(this, HALog.BASIC, "Dropping database '%s' on follower (replicated from leader)", entry.databaseName());
    try {
      server.getDatabase(entry.databaseName()).getEmbedded().drop();
    } catch (final Exception e) {
      // Database was removed concurrently between existsDatabase() and getDatabase().
      // This is harmless - the drop was the intended outcome.
      HALog.log(this, HALog.BASIC, "Database '%s' was removed concurrently, drop is a no-op: %s",
          entry.databaseName(), e.getMessage());
    }
    server.removeDatabase(entry.databaseName());
  }

  private void applyCreateUser(final byte[] data) {
    final RaftLogEntryCodec.UserEntry entry = RaftLogEntryCodec.deserializeUserEntry(data);

    if (isOriginNode(entry.originPeerId()) && raftHA != null && raftHA.isLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping CREATE_USER on origin leader (already created)");
      return;
    }

    final JSONObject userConfig = new JSONObject(entry.userJson());
    final String userName = userConfig.getString("name");

    if (server.getSecurity().getUser(userName) != null) {
      HALog.log(this, HALog.BASIC, "User '%s' already exists on this follower, skipping create", userName);
      return;
    }

    HALog.log(this, HALog.BASIC, "Creating user '%s' on follower (replicated from leader)", userName);
    server.getSecurity().createUser(userConfig);
  }

  private void applyUpdateUser(final byte[] data) {
    final RaftLogEntryCodec.UserEntry entry = RaftLogEntryCodec.deserializeUserEntry(data);

    if (isOriginNode(entry.originPeerId()) && raftHA != null && raftHA.isLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping UPDATE_USER on origin leader (already updated)");
      return;
    }

    final JSONObject userConfig = new JSONObject(entry.userJson());
    final String userName = userConfig.getString("name");

    if (server.getSecurity().getUser(userName) == null) {
      HALog.log(this, HALog.BASIC, "User '%s' does not exist on this follower, skipping update", userName);
      return;
    }

    HALog.log(this, HALog.BASIC, "Updating user '%s' on follower (replicated from leader)", userName);
    server.getSecurity().updateUser(userConfig);
  }

  private void applyDropUser(final byte[] data) {
    final RaftLogEntryCodec.DropUserEntry entry = RaftLogEntryCodec.deserializeDropUser(data);

    if (isOriginNode(entry.originPeerId()) && raftHA != null && raftHA.isLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping DROP_USER on origin leader (already dropped): user=%s", entry.userName());
      return;
    }

    if (server.getSecurity().getUser(entry.userName()) == null) {
      HALog.log(this, HALog.BASIC, "User '%s' does not exist on this follower, skipping drop", entry.userName());
      return;
    }

    HALog.log(this, HALog.BASIC, "Dropping user '%s' on follower (replicated from leader)", entry.userName());
    server.getSecurity().dropUser(entry.userName());
  }

  /**
   * Checks whether this node originated the given log entry by comparing the peer ID
   * embedded in the entry against the local peer ID. This avoids a TOCTOU race that would
   * occur if we queried live leadership state (which can change between commit and apply).
   */
  private boolean isOriginNode(final String originPeerId) {
    final RaftHAServer raftHA = this.raftHA;
    return raftHA != null && raftHA.getLocalPeerId().toString().equals(originPeerId);
  }

  private boolean isCurrentNodeLeader() {
    return currentNodeIsLeader;
  }

  // -- Schema Changes --

  private void createNewFiles(final DatabaseInternal db, final Map<Integer, String> filesToAdd) throws IOException {
    final String databasePath = db.getDatabasePath();
    DatabaseContext.INSTANCE.init(db);
    for (final Map.Entry<Integer, String> entry : filesToAdd.entrySet()) {
      // Idempotency guard: during log replay after a cold restart or snapshot installation,
      // the file may already exist on disk from a prior commit. Skip creation to avoid
      // disturbing a file that already has valid data.
      if (db.getFileManager().existsFile(entry.getKey())) {
        LogManager.instance().log(this, Level.FINE, "Skipping file creation for fileId=%d (%s), already registered",
            entry.getKey(), entry.getValue());
        continue;
      }
      final File osFile = new File(databasePath + File.separator + entry.getValue());
      if (osFile.exists() && osFile.length() > 0) {
        LogManager.instance().log(this, Level.WARNING,
            "Skipping file creation for fileId=%d (%s), file already exists on disk with size %d",
            entry.getKey(), entry.getValue(), osFile.length());
        continue;
      }
      db.getFileManager().getOrCreateFile(entry.getKey(), databasePath + File.separator + entry.getValue());
    }
  }

  private void removeDroppedFiles(final DatabaseInternal db, final Map<Integer, String> filesToRemove) throws IOException {
    if (filesToRemove == null || filesToRemove.isEmpty())
      return;
    DatabaseContext.INSTANCE.init(db);
    for (final Map.Entry<Integer, String> entry : filesToRemove.entrySet()) {
      db.getPageManager().deleteFile(db, entry.getKey());
      db.getFileManager().dropFile(entry.getKey());
      db.getSchema().getEmbedded().removeFile(entry.getKey());
    }
  }

  private void updateSchemaMetadata(final DatabaseInternal db, final String schemaJson) throws IOException {
    if (schemaJson != null && !schemaJson.isEmpty())
      db.getSchema().getEmbedded().update(new JSONObject(schemaJson));
  }

  // -- Snapshots --

  /**
   * Persists a snapshot marker file so that Ratis can purge old log entries and restore
   * lastAppliedIndex on restart.
   * <p>
   * ArcadeDB state lives in the database files on disk, not in this marker file. The marker
   * records the term and index so that:
   * <ol>
   *   <li><b>Log compaction:</b> Ratis uses the returned index as the purge boundary. Without this,
   *       the Raft log would grow unboundedly. Auto-triggered every {@code arcadedb.ha.snapshotThreshold}
   *       entries (see {@link com.arcadedb.GlobalConfiguration#HA_SNAPSHOT_THRESHOLD}).</li>
   *   <li><b>Restart recovery:</b> reinitialize() reads the snapshot index so Ratis skips
   *       already-applied entries on cold start.</li>
   *   <li><b>Follower catch-up:</b> when a follower is too far behind for log replay,
   *       notifyInstallSnapshotFromLeader() handles full resync via HTTP.</li>
   * </ol>
   */
  @Override
  public long takeSnapshot() throws IOException {
    final TermIndex termIndex = getLastAppliedTermIndex();
    if (termIndex == null || termIndex.getIndex() <= 0) {
      LogManager.instance().log(this, Level.FINE, "Skipping snapshot: no entries applied yet");
      return lastAppliedIndex.get();
    }

    final File snapshotFile = storage.getSnapshotFile(termIndex.getTerm(), termIndex.getIndex());
    Files.writeString(snapshotFile.toPath(), "arcadedb-snapshot-marker");

    final MD5Hash digest = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    storage.updateLatestSnapshot(
        new SingleFileSnapshotInfo(new FileInfo(snapshotFile.toPath(), digest), termIndex));

    // Persist the applied index so that reinitialize() can detect snapshot gaps
    writePersistedAppliedIndex(termIndex.getIndex());

    LogManager.instance().log(this, Level.INFO, "Raft snapshot taken at term=%d, index=%d",
        termIndex.getTerm(), termIndex.getIndex());
    return termIndex.getIndex();
  }

  // -- Persisted applied index (for snapshot gap detection) --

  private Path getAppliedIndexFile() {
    // Store under the peer-specific subdirectory so that multiple server instances sharing the same
    // root path (e.g. in-JVM tests) do not overwrite each other's applied index files.
    final String peerId = raftHA != null ? raftHA.getLocalPeerId().toString() : "default";
    return Path.of(server.getRootPath(), "ratis-storage", peerId, "applied-index");
  }

  private long readPersistedAppliedIndex() {
    try {
      final Path file = getAppliedIndexFile();
      if (Files.exists(file))
        return Long.parseLong(Files.readString(file).trim());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not read persisted applied index: %s", e.getMessage());
    }
    return -1;
  }

  private void writePersistedAppliedIndex(final long index) {
    try {
      final Path file = getAppliedIndexFile();
      Files.createDirectories(file.getParent());
      // Write via a temp file + atomic rename to avoid a corrupt/truncated file on crash mid-write.
      // A corrupt file would cause readPersistedAppliedIndex to return -1, preventing the gap
      // detection from triggering a snapshot download when one is actually needed.
      final Path tmp = file.resolveSibling("applied-index.tmp");
      Files.writeString(tmp, Long.toString(index));
      Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not write persisted applied index: %s", e.getMessage());
    }
  }

  // -- Follower event: notification-mode snapshot installation --

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(final RaftProtos.RoleInfoProto roleInfoProto,
      final TermIndex firstTermIndexInLog) {
    LogManager.instance().log(this, Level.INFO,
        "Raft snapshot installation requested from leader (firstLogIndex=%s). Triggering full resync...", firstTermIndexInLog);

    // Threading safety: Ratis calls this method inside synchronized(RaftServerImpl) in
    // SnapshotInstallationHandler.notifyStateMachineToInstallSnapshot(), but only attaches a
    // whenComplete() callback to the returned future - it does NOT block (.join()) on it before
    // releasing the monitor. The heavy work (HTTP download, db.close(), server.getDatabase()) runs
    // on the ForkJoinPool and only synchronizes on ArcadeDBServer.databases, never on RaftServerImpl,
    // so there is no deadlock risk.
    return CompletableFuture.supplyAsync(() -> {
      try {
        if (snapshotInstaller == null)
          throw new ReplicationException("Snapshot installer not available (server not fully initialized)");
        final RaftPeerId leaderId = RaftPeerId.valueOf(
            roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId());
        final String leaderHttpAddr = raftHA.getPeerHTTPAddress(leaderId);
        if (leaderHttpAddr == null)
          throw new ReplicationException("Cannot determine leader HTTP address for snapshot download");
        snapshotInstaller.installFromLeaderNotification(leaderHttpAddr);
        LogManager.instance().log(this, Level.INFO, "Full resync from leader %s completed", leaderId);
        return firstTermIndexInLog;
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error during snapshot installation from leader", e);
        throw new ReplicationException("Error during Raft snapshot installation", e);
      }
    });
  }

  // -- Event notifications --

  @Override
  public void notifyLeaderChanged(final RaftGroupMemberId groupMemberId, final RaftPeerId newLeaderId) {
    HALog.log(this, HALog.BASIC, "Leader changed to %s (group: %s)", newLeaderId, groupMemberId);
    electionCount.incrementAndGet();
    lastElectionTime = System.currentTimeMillis();

    // If a snapshot gap was detected in reinitialize(), download now that we know the leader.
    // Threading safety: Ratis calls notifyLeaderChanged() from ServerState.setLeader() which
    // uses AtomicReference (no synchronized block). The download runs on a dedicated thread
    // and only synchronizes on ArcadeDBServer.databases, never on Ratis internals.
    if (needsSnapshotDownload.compareAndSet(true, false)) {
      lifecycleExecutor.submit(() -> {
        try {
          // No artificial delay needed: SnapshotInstaller.downloadSnapshotWithRetry() handles
          // leader unavailability with retries and exponential backoff (5s/10s/20s).
          if (snapshotInstaller != null)
            snapshotInstaller.installDatabasesFromLeader();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to download databases after leader discovery: %s", e.getMessage());
        }
      });
    }

    // Refresh gRPC channels to force fresh DNS resolution after potential network partition.
    // newLeaderId can be null during elections (no leader yet) - handle defensively.
    final RaftHAServer raftHA = this.raftHA;
    if (raftHA != null) {
      // Update cached leader flag (used for non-critical checks like catch-up detection).
      // The origin-skip in applyTransactionEntry() uses raftHA.isLeader() directly instead.
      currentNodeIsLeader = newLeaderId != null && newLeaderId.equals(raftHA.getLocalPeerId());
      raftHA.refreshRaftClient();
      raftHA.notifyLeaderChanged();
    } else
      currentNodeIsLeader = false;

    if (newLeaderId != null)
      fireCallback(ReplicationCallback.TYPE.LEADER_ELECTED, newLeaderId.toString());
  }

  public long getElectionCount() {
    return electionCount.get();
  }

  public long getLastElectionTime() {
    return lastElectionTime;
  }

  public long getStartTime() {
    return startTime;
  }

  @Override
  public void notifyConfigurationChanged(final long term, final long index,
      final org.apache.ratis.proto.RaftProtos.RaftConfigurationProto newConf) {
    HALog.log(this, HALog.BASIC, "Configuration changed at term=%d, index=%d, peers=%d",
        term, index, newConf.getPeersList().size());

    // Collect new peer IDs and clean up ClusterMonitor entries for removed peers
    final Set<String> newPeerIds = new HashSet<>(newConf.getPeersList().size());
    for (final var peer : newConf.getPeersList()) {
      final String peerId = peer.getId().toStringUtf8();
      newPeerIds.add(peerId);
      fireCallback(ReplicationCallback.TYPE.REPLICA_ONLINE, peerId);
    }

    final RaftHAServer raftHA = this.raftHA;
    if (raftHA != null && raftHA.getClusterMonitor() != null) {
      for (final String trackedId : raftHA.getClusterMonitor().getReplicaLags().keySet())
        if (!newPeerIds.contains(trackedId))
          raftHA.getClusterMonitor().removeReplica(trackedId);
    }
  }

  @Override
  public void notifyServerShutdown(final org.apache.ratis.proto.RaftProtos.RoleInfoProto roleInfo, final boolean allServer) {
    HALog.log(this, HALog.BASIC, "Server shutdown notification (allServer=%s)", allServer);
    fireCallback(ReplicationCallback.TYPE.REPLICA_OFFLINE, server.getServerName());

    // If the server is still supposed to be running (not in SHUTTING_DOWN), the Ratis server
    // closed due to an error (e.g., network partition). Schedule a restart after close completes.
    // This runs on a dedicated daemon thread (not the lifecycleExecutor) to avoid blocking
    // snapshot downloads and other lifecycle tasks during the wait.
    if (server.getStatus() == ArcadeDBServer.STATUS.ONLINE) {
      final Thread restartThread = new Thread(() -> {
        try {
          final RaftHAServer raftHA = this.raftHA;
          if (raftHA != null) {
            final long deadline = System.currentTimeMillis() + 10_000;
            while (raftHA.getRaftLifeCycleState() != org.apache.ratis.util.LifeCycle.State.CLOSED
                && System.currentTimeMillis() < deadline)
              Thread.sleep(100);
            // Re-check status: a graceful shutdown may have started between the
            // outer ONLINE check and now. Don't restart if we're shutting down.
            if (server.getStatus() != ArcadeDBServer.STATUS.ONLINE)
              return;
            raftHA.restartRatisIfNeeded();
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Failed to restart Ratis after shutdown: %s", e.getMessage());
        }
      }, "arcadedb-ratis-restart");
      restartThread.setDaemon(true);
      restartThread.start();
    }
  }

  private void fireCallback(final ReplicationCallback.TYPE type, final Object data) {
    try {
      server.lifecycleEvent(type, data);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error firing %s event: %s", e, type, e.getMessage());
    }
  }

  // -- Utilities --

  /**
   * @deprecated Use {@link RaftLogEntryCodec#parseWalTransaction(Binary)} instead.
   */
  @Deprecated
  static WALFile.WALTransaction parseWalTransaction(final Binary buffer) {
    return RaftLogEntryCodec.parseWalTransaction(buffer);
  }
}
