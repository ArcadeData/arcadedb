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
 * On the originating node, transactions are committed locally via commit2ndPhase() and the state machine
 * apply is skipped to avoid double-application. Origin is determined by comparing the originPeerId
 * embedded in the log entry against the local peer ID, avoiding TOCTOU races with live leadership state.
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
      // Expected errors (database unavailable, corrupted entry, unknown value type): return
      // failed future so Ratis can handle the failure without crashing the node.
      LogManager.instance().log(this, Level.SEVERE, "Error applying Raft log entry at index %d", e, index);
      return CompletableFuture.failedFuture(e);
    } catch (final Exception e) {
      // Unexpected errors (NPE, ClassCastException, etc.) indicate a bug that could cause
      // state divergence if silently swallowed. Crash the state machine so the node recovers
      // via snapshot rather than continuing with potentially inconsistent state.
      LogManager.instance().log(this, Level.SEVERE,
          "CRITICAL: Unexpected error applying Raft log entry at index %d. "
              + "Shutting down to prevent state divergence.", e, index);
      // Schedule stop on a separate thread to avoid deadlock: server.stop() closes the
      // Ratis server, which may try to acquire locks held by the current applyTransaction
      // callback thread.
      final Thread stopThread = new Thread(() -> {
        try { server.stop(); } catch (final Exception ignored) {}
      }, "arcadedb-emergency-stop");
      stopThread.setDaemon(true);
      stopThread.start();
      return CompletableFuture.failedFuture(e);
    }
  }

  private void applyTransactionEntry(final byte[] data) {
    final RaftLogEntryCodec.TransactionEntry entry = RaftLogEntryCodec.deserializeTransaction(data);

    final DatabaseInternal db = server.getDatabase(entry.databaseName());
    if (db == null || !db.isOpen())
      throw new ReplicationException("Database '" + entry.databaseName() + "' is not available");

    // The originating leader already applied changes locally via commit2ndPhase(). Skip the state
    // machine apply to avoid double-applying page changes on the current leader only.
    //
    // The skip only fires when BOTH conditions are true:
    //   1. This node's peer ID matches the originPeerId embedded in the log entry
    //   2. This node is currently the leader
    //
    // Condition 2 is critical for restart safety: if the leader crashes between Raft commit and
    // commit2ndPhase(), the entry is in the Raft log but was never applied locally. On restart,
    // this node becomes a follower (a new leader was elected during the outage). Ratis replays
    // the entry, and because isLeader() returns false, the skip does NOT fire. The entry is
    // applied via the normal follower path. The page version check in applyChanges() handles the
    // case where commit2ndPhase() DID run before the crash (the already-applied pages are
    // detected and skipped with a ConcurrentModificationException log, see below).
    //
    // We call raftHA.isLeader() (which queries Ratis's internal role state directly via
    // getDivision().getInfo().isLeader()) rather than the cached currentNodeIsLeader field.
    // The cached field is updated by notifyLeaderChanged() on a separate thread: if a live
    // leadership transfer happens between that write and the read here, we could see a stale
    // true and skip an entry that was never committed locally via commit2ndPhase(), silently
    // losing the transaction. The direct Ratis query reflects the authoritative role state and
    // eliminates this race. A tiny TOCTOU window (between the isLeader() call and the return
    // below) remains, but cannot cause data loss: commit2ndPhase() is always called before
    // Ratis delivers the commit notification to the application layer on the leader path.
    if (isOriginNode(entry.originPeerId()) && raftHA != null && raftHA.isLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping WAL apply on origin node (commit2ndPhase handles it): db=%s", entry.databaseName());
      return;
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
      final WALFile.WALTransaction walTx = parseWalTransaction(entry.walBuffer());

      LogManager.instance().log(this, Level.FINE, "Applying Raft tx %d (modifiedPages=%d, db=%s)...", walTx.txId,
          walTx.pages.length, entry.databaseName());

      try {
        db.getTransactionManager().applyChanges(walTx, entry.bucketRecordDelta(), false);
      } catch (final com.arcadedb.exception.ConcurrentModificationException e) {
        // applyChanges() throws ConcurrentModificationException in two distinct situations:
        // 1. Benign replay: WAL page version <= DB page version - the entry was already applied
        //    (via WAL recovery or a prior commit). Expected after cold restart or snapshot install.
        // 2. Version gap: WAL page version > DB page version + 1 - an intermediate transaction
        //    was never applied on this node. This should not happen and may indicate state divergence.
        //    The message starts with "Cannot apply changes" in this case.
        final String msg = e.getMessage();
        if (msg != null && msg.startsWith("Cannot apply changes"))
          LogManager.instance().log(this, Level.SEVERE,
              "Unexpected WAL version gap on follower - possible state divergence (db=%s, txId=%d): %s",
              entry.databaseName(), walTx.txId, msg);
        else
          LogManager.instance().log(this, Level.WARNING,
              "Skipping already-applied WAL entry on follower (db=%s, txId=%d): %s",
              entry.databaseName(), walTx.txId, msg);
      }
    }

    // Phase 3: Finalize schema - update metadata and remove dropped files AFTER WAL is safely applied.
    // This ordering ensures a WAL failure leaves only orphan empty files (harmless, cleaned up by snapshot)
    // rather than schema-ahead-of-data inconsistency.
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
    server.getDatabase(entry.databaseName()).getEmbedded().drop();
    server.removeDatabase(entry.databaseName());
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

  static WALFile.WALTransaction parseWalTransaction(final Binary buffer) {
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();

    // Minimum header: txId(8) + timestamp(8) + pages(4) + segmentSize(4) = 24 bytes
    final int headerSize = 2 * Binary.LONG_SERIALIZED_SIZE + 2 * Binary.INT_SERIALIZED_SIZE;
    if (buffer.size() < headerSize)
      throw new ReplicationException(
          "Replicated transaction buffer is truncated: expected at least " + headerSize + " header bytes, got " + buffer.size());

    int pos = 0;
    tx.txId = buffer.getLong(pos);
    pos += Binary.LONG_SERIALIZED_SIZE;

    tx.timestamp = buffer.getLong(pos);
    pos += Binary.LONG_SERIALIZED_SIZE;

    final int pages = buffer.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;

    final int segmentSize = buffer.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;

    if (segmentSize < 0 || pos + segmentSize + Binary.LONG_SERIALIZED_SIZE > buffer.size())
      throw new ReplicationException("Replicated transaction buffer is corrupted (segmentSize=" + segmentSize + ")");

    tx.pages = new WALFile.WALPage[pages];

    for (int i = 0; i < pages; ++i) {
      // Validate that the 4 fixed-size header fields (fileId, pageNumber, changesFrom, changesTo) fit
      if (pos + 4 * Binary.INT_SERIALIZED_SIZE > buffer.size())
        throw new ReplicationException("Replicated transaction buffer is corrupted");

      tx.pages[i] = new WALFile.WALPage();
      tx.pages[i].fileId = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].pageNumber = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].changesFrom = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].changesTo = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final int deltaSize = tx.pages[i].changesTo - tx.pages[i].changesFrom + 1;
      if (deltaSize <= 0)
        throw new ReplicationException(
            "Invalid delta range in replicated transaction: changesFrom=" + tx.pages[i].changesFrom + " changesTo=" + tx.pages[i].changesTo);

      // Validate that the remaining 2 fixed fields + delta bytes fit before reading them
      if (pos + 2 * Binary.INT_SERIALIZED_SIZE + deltaSize > buffer.size())
        throw new ReplicationException("Replicated transaction buffer is corrupted");

      tx.pages[i].currentPageVersion = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].currentPageSize = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final byte[] pageData = new byte[deltaSize];
      tx.pages[i].currentContent = new Binary(pageData);
      buffer.getByteArray(pos, pageData, 0, deltaSize);
      pos += deltaSize;
    }

    // Trailing footer: segmentSize(4) + magicNumber(8)
    final int footerSize = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;
    if (pos + footerSize > buffer.size())
      throw new ReplicationException(
          "Replicated transaction buffer is truncated: expected " + footerSize + " footer bytes at position " + pos + ", buffer size " + buffer.size());

    final int trailingSegmentSize = buffer.getInt(pos);
    pos += Binary.INT_SERIALIZED_SIZE;
    if (trailingSegmentSize != segmentSize)
      throw new ReplicationException(
          "Replicated transaction buffer is corrupted (trailing segment size " + trailingSegmentSize + " != leading " + segmentSize + ")");

    final long magicNumber = buffer.getLong(pos);
    if (magicNumber != WALFile.MAGIC_NUMBER)
      throw new ReplicationException("Replicated transaction buffer is corrupted (bad magic number)");

    return tx;
  }
}
