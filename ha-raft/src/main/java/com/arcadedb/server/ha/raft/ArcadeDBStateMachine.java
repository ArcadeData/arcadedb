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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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
  /** Cached leader flag, updated by notifyLeaderChanged(). Avoids calling raftServer.getDivision() on every applyTransaction(). */
  private volatile boolean                currentNodeIsLeader    = false;
  /** Executor for async lifecycle tasks (snapshot download, Ratis restart) so they can be awaited on close. */
  private final ExecutorService           lifecycleExecutor      = Executors.newSingleThreadExecutor(
      r -> { final Thread t = new Thread(r, "arcadedb-sm-lifecycle"); t.setDaemon(true); return t; });

  public ArcadeDBStateMachine(final ArcadeDBServer server, final RaftHAServer raftHA) {
    this.server = server;
    this.raftHA = raftHA;
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
              installDatabasesFromLeader();
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

      final RaftLogEntry.EntryType type = RaftLogEntry.readType(ByteBuffer.wrap(data));

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
    final RaftLogEntry.TransactionEntry entry = RaftLogEntry.deserializeTransaction(data);

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
    // the entry, and because isCurrentNodeLeader() returns false, the skip does NOT fire. The
    // entry is applied via the normal follower path. The page version check in applyChanges()
    // handles the case where commit2ndPhase() DID run before the crash (the already-applied
    // pages are detected and skipped with a ConcurrentModificationException log, see below).
    if (isOriginNode(entry.originPeerId()) && isCurrentNodeLeader()) {
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
        // After a cold restart or snapshot installation, Ratis may replay entries that were already
        // applied to the database (via WAL recovery or prior commit). The page version check in
        // applyChanges() detects this as a ConcurrentModificationException. This is safe to skip,
        // but must always be visible in logs so operators can distinguish expected replay from
        // unexpected data inconsistency.
        LogManager.instance().log(this, Level.WARNING,
            "Skipping already-applied WAL entry (db=%s, txId=%d): %s", entry.databaseName(), walTx.txId, e.getMessage());
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
    final RaftLogEntry.CreateDatabaseEntry entry = RaftLogEntry.deserializeCreateDatabase(data);

    // The originating node already created the database locally before submitting the Ratis entry.
    if (isOriginNode(entry.originPeerId())) {
      HALog.log(this, HALog.TRACE, "Skipping CREATE_DATABASE on origin node (already created): db=%s", entry.databaseName());
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
    final RaftLogEntry.DropDatabaseEntry entry = RaftLogEntry.deserializeDropDatabase(data);

    // The originating node already dropped the database locally before submitting the Ratis entry.
    if (isOriginNode(entry.originPeerId())) {
      HALog.log(this, HALog.TRACE, "Skipping DROP_DATABASE on origin node (already dropped): db=%s", entry.databaseName());
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
    return Path.of(server.getRootPath(), "ratis-storage", "applied-index");
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
      Files.writeString(file, Long.toString(index));
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Could not write persisted applied index: %s", e.getMessage());
    }
  }

  /**
   * Downloads all databases from the leader via HTTP. Called during chunk-based snapshot
   * installation (installSnapshotEnabled=true) when reinitialize() detects a gap between
   * the snapshot index and persisted applied index (see {@link #needsSnapshotDownload}).
   * The Ratis snapshot only contains a marker file; the actual database data is transferred via HTTP.
   */
  private void installDatabasesFromLeader() throws IOException {
    final RaftHAServer raftHA = this.raftHA;
    if (raftHA == null)
      return;

    String leaderHttpAddr = raftHA.getLeaderHTTPAddress();
    if (leaderHttpAddr == null) {
      LogManager.instance().log(this, Level.WARNING,
          "Cannot determine leader HTTP address for snapshot download, will rely on Raft log replay");
      return;
    }

    LogManager.instance().log(this, Level.INFO,
        "Downloading databases from leader %s during snapshot installation...", leaderHttpAddr);

    // Fetch the leader's database list so we can detect stale databases on this follower.
    // A database dropped on the leader before this follower catches up would otherwise remain
    // as a stale copy on the follower indefinitely.
    final Set<String> leaderDatabases = fetchLeaderDatabaseNames(leaderHttpAddr, raftHA);

    for (final String dbName : server.getDatabaseNames()) {
      if (!leaderDatabases.isEmpty() && !leaderDatabases.contains(dbName)) {
        LogManager.instance().log(this, Level.INFO,
            "Database '%s' exists on this follower but not on the leader, dropping stale copy", dbName);
        try {
          server.getDatabase(dbName).getEmbedded().drop();
          server.removeDatabase(dbName);
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Failed to drop stale database '%s': %s", dbName, e.getMessage());
        }
        continue;
      }

      try {
        LogManager.instance().log(this, Level.INFO, "Installing snapshot for database '%s' from leader %s...",
            dbName, leaderHttpAddr);
        final DatabaseInternal db = server.getDatabase(dbName);
        if (db == null) {
          LogManager.instance().log(this, Level.WARNING,
              "Database '%s' was dropped during snapshot installation, skipping", dbName);
          continue;
        }
        installDatabaseSnapshot(db, leaderHttpAddr, dbName);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Failed to install snapshot for database '%s', skipping: %s", dbName, e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.INFO, "Snapshot installation from leader completed");
  }

  // -- Follower event: notification-mode snapshot installation (kept for backward compatibility) --

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
        // Resolve the leader's HTTP address for snapshot download
        final RaftHAServer raftHA = this.raftHA;
        final RaftPeerId leaderId = RaftPeerId.valueOf(
            roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId());
        final String leaderHttpAddr = raftHA.getPeerHTTPAddress(leaderId);

        if (leaderHttpAddr == null)
          throw new ReplicationException("Cannot determine leader HTTP address for snapshot download");

        // Fetch leader's database list to detect stale databases on this follower
        final Set<String> leaderDatabases = fetchLeaderDatabaseNames(leaderHttpAddr, raftHA);

        // Remove databases that exist locally but not on the leader
        for (final String dbName : new ArrayList<>(server.getDatabaseNames())) {
          if (!leaderDatabases.isEmpty() && !leaderDatabases.contains(dbName)) {
            LogManager.instance().log(this, Level.INFO,
                "Database '%s' exists on this follower but not on the leader, dropping stale copy", dbName);
            try {
              server.getDatabase(dbName).getEmbedded().drop();
              server.removeDatabase(dbName);
            } catch (final Exception e) {
              LogManager.instance().log(this, Level.WARNING,
                  "Failed to drop stale database '%s': %s", dbName, e.getMessage());
            }
            continue;
          }

          LogManager.instance().log(this, Level.INFO, "Installing snapshot for database '%s' from leader %s...",
              dbName, leaderHttpAddr);

          final DatabaseInternal db = server.getDatabase(dbName);
          if (db == null)
            throw new ReplicationException("Database '" + dbName + "' not found during snapshot installation");
          installDatabaseSnapshot(db, leaderHttpAddr, dbName);
        }

        LogManager.instance().log(this, Level.INFO, "Full resync from leader %s completed", leaderId);
        return firstTermIndexInLog;

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error during snapshot installation from leader", e);
        throw new ReplicationException("Error during Raft snapshot installation", e);
      }
    });
  }

  /**
   * Downloads a database snapshot (ZIP) from the leader's HTTP endpoint and replaces local files.
   * The snapshot contains only data files and schema configuration, NOT WAL files.
   * After installation, Ratis replays log entries from the snapshot point.
   * <p>
   * Extraction happens to a temp directory first. The database directory is only replaced after
   * the full download and extraction succeeds, preventing partial-failure corruption.
   */
  /**
   * Queries the leader's HTTP API for its current database list. Returns an empty set on failure
   * (caller should skip stale-database removal if the leader is unreachable rather than dropping everything).
   */
  private Set<String> fetchLeaderDatabaseNames(final String leaderHttpAddr, final RaftHAServer raftHA) {
    try {
      final boolean useSsl = server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
      final String url = (useSsl ? "https" : "http") + "://" + leaderHttpAddr + "/api/v1/server";
      final java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URI(url).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(10_000);
      conn.setReadTimeout(10_000);
      if (raftHA.getClusterToken() != null) {
        conn.setRequestProperty("X-ArcadeDB-Cluster-Token", raftHA.getClusterToken());
        conn.setRequestProperty("X-ArcadeDB-Forwarded-User", "root");
      }
      try {
        if (conn.getResponseCode() == 200) {
          final String body = new String(conn.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
          final com.arcadedb.serializer.json.JSONObject json = new com.arcadedb.serializer.json.JSONObject(body);
          if (json.has("databases")) {
            final com.arcadedb.serializer.json.JSONArray dbs = json.getJSONArray("databases");
            final Set<String> names = new java.util.HashSet<>(dbs.length());
            for (int i = 0; i < dbs.length(); i++)
              names.add(dbs.getString(i));
            return names;
          }
        }
      } finally {
        conn.disconnect();
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Could not fetch database list from leader %s, skipping stale database cleanup: %s",
          leaderHttpAddr, e.getMessage());
    }
    return Set.of();
  }

  private static final int    SNAPSHOT_DOWNLOAD_MAX_RETRIES = 3;
  private static final long[] SNAPSHOT_DOWNLOAD_BACKOFF_MS  = { 5_000, 10_000, 20_000 };

  private void installDatabaseSnapshot(final DatabaseInternal db, final String leaderHttpAddr,
      final String databaseName) throws IOException {

    final String databasePath = db.getDatabasePath();
    final Path dbPath = Path.of(databasePath).normalize().toAbsolutePath();
    final Path tempDir = dbPath.resolveSibling(dbPath.getFileName() + ".snapshot-tmp");
    final Path backupDir = dbPath.resolveSibling(dbPath.getFileName() + ".snapshot-old");

    // Phase 1: Download and extract to temp directory (database stays open and operational).
    // Retries handle transient leader unavailability (e.g., mid-election, restart).
    // The leader address is refreshed on each retry to handle elections during download.
    downloadSnapshotWithRetry(leaderHttpAddr, tempDir, databaseName);

    // Phase 2: Close database and swap directories using a crash-safe marker file.
    // The marker ensures recovery can complete or rollback the swap if the process crashes.
    // Close the underlying LocalDatabase directly - ServerDatabase.close() throws
    // UnsupportedOperationException because server-managed databases are shared.
    db.getEmbedded().close();

    final Path markerFile = dbPath.resolveSibling(dbPath.getFileName() + ".snapshot-pending");

    try {
      // Write the pending marker BEFORE any destructive operation.
      // If we crash after this point, recoverPendingSnapshotSwaps() will finish the job.
      Files.writeString(markerFile, databaseName);

      // Move current database dir out of the way.
      // Delete stale backup from a previous failed swap to avoid FileAlreadyExistsException.
      if (Files.exists(dbPath)) {
        FileUtils.deleteRecursively(backupDir.toFile());
        Files.move(dbPath, backupDir);
      }

      // Move fully-extracted temp dir into place
      Files.move(tempDir, dbPath);

      // Delete WAL files from the new database dir - they are stale after snapshot installation
      deleteStaleWalFiles(dbPath);

      // Swap succeeded - remove marker and backup
      Files.deleteIfExists(markerFile);
      FileUtils.deleteRecursively(backupDir.toFile());

      LogManager.instance().log(this, Level.INFO, "Database snapshot for '%s' installed successfully", databaseName);

    } catch (final Exception e) {
      // Swap failed - try to restore original database
      LogManager.instance().log(this, Level.SEVERE, "Snapshot swap failed for '%s', attempting rollback...", databaseName);
      try {
        if (Files.exists(backupDir)) {
          FileUtils.deleteRecursively(dbPath.toFile());   // remove partial move if any
          Files.move(backupDir, dbPath);
        }
      } catch (final Exception rollbackEx) {
        LogManager.instance().log(this, Level.SEVERE,
            "CRITICAL: Failed to rollback snapshot swap for '%s'. Database may be unavailable. Error: %s",
            databaseName, rollbackEx.getMessage());
      }
      Files.deleteIfExists(markerFile);
      FileUtils.deleteRecursively(tempDir.toFile());
      throw new ReplicationException("Snapshot installation failed during directory swap", e);
    } finally {
      // Force the server to drop the old (now-stale) database reference and reopen from the
      // swapped directory. Without this, server.getDatabase() returns the cached instance that
      // points to the old (deleted) data files.
      try {
        server.removeDatabase(databaseName);
        server.getDatabase(databaseName);
        LogManager.instance().log(this, Level.INFO, "Database '%s' reopened after snapshot installation", databaseName);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "CRITICAL: Failed to reopen database '%s' after snapshot installation. "
                + "This node may need to be restarted to recover. Error: %s",
            databaseName, e.getMessage());
      }
    }
  }

  /**
   * Downloads and extracts a snapshot ZIP into the temp directory, retrying on transient failures
   * (connection refused, HTTP errors) that are common during leader elections or restarts.
   * On success the temp directory contains the fully extracted snapshot.
   * On permanent failure (all retries exhausted) the temp directory is cleaned up and an
   * IOException is thrown.
   */
  private void downloadSnapshotWithRetry(final String initialLeaderAddr, final Path tempDir,
      final String databaseName) throws IOException {
    final boolean useSsl = server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final String protocol = useSsl ? "https" : "http";

    for (int attempt = 1; attempt <= SNAPSHOT_DOWNLOAD_MAX_RETRIES; attempt++) {
      // Refresh leader address on each retry to handle elections during download
      final RaftHAServer raftHA = this.raftHA;
      String leaderAddr = raftHA != null ? raftHA.getLeaderHTTPAddress() : null;
      if (leaderAddr == null)
        leaderAddr = initialLeaderAddr;

      final String snapshotUrl = protocol + "://" + leaderAddr + "/api/v1/ha/snapshot/"
          + java.net.URLEncoder.encode(databaseName, java.nio.charset.StandardCharsets.UTF_8);
      try {
        downloadSnapshot(snapshotUrl, tempDir, databaseName);
        return; // success
      } catch (final IOException | ReplicationException e) {
        FileUtils.deleteRecursively(tempDir.toFile());
        if (attempt == SNAPSHOT_DOWNLOAD_MAX_RETRIES)
          throw e instanceof IOException ? (IOException) e
              : new IOException("Snapshot download failed after " + SNAPSHOT_DOWNLOAD_MAX_RETRIES + " attempts", e);

        final long backoff = SNAPSHOT_DOWNLOAD_BACKOFF_MS[attempt - 1];
        LogManager.instance().log(this, Level.WARNING,
            "Snapshot download from %s failed (attempt %d/%d), retrying in %dms: %s",
            snapshotUrl, attempt, SNAPSHOT_DOWNLOAD_MAX_RETRIES, backoff, e.getMessage());
        try {
          Thread.sleep(backoff);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Snapshot download interrupted during retry backoff", ie);
        }
      }
    }
  }

  /**
   * Downloads a snapshot ZIP from the leader and extracts it into the given temp directory.
   * The temp directory is created fresh (any leftover from a previous attempt is cleaned up).
   * On failure the temp directory is cleaned up and the exception is thrown.
   */
  private void downloadSnapshot(final String snapshotUrl, final Path tempDir,
      final String databaseName) throws IOException {
    LogManager.instance().log(this, Level.INFO, "Downloading database snapshot from %s...", snapshotUrl);

    final HttpURLConnection connection;
    try {
      connection = (HttpURLConnection) new URI(snapshotUrl).toURL().openConnection();
    } catch (final URISyntaxException e) {
      throw new IOException("Invalid snapshot URL: " + snapshotUrl, e);
    }

    // When SSL is enabled, configure the connection to use the server's trust store
    // so that self-signed certificates (e.g., from cert-manager in K8s) are accepted.
    if (connection instanceof javax.net.ssl.HttpsURLConnection httpsConn) {
      try {
        final javax.net.ssl.SSLContext sslContext = server.getHttpServer().getSSLContext();
        if (sslContext != null)
          httpsConn.setSSLSocketFactory(sslContext.getSocketFactory());
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Could not configure SSL for snapshot download, using default trust store: %s", e.getMessage());
      }
    }

    connection.setRequestMethod("GET");
    connection.setConnectTimeout(30_000);
    connection.setReadTimeout(
        server.getConfiguration().getValueAsInteger(GlobalConfiguration.HA_SNAPSHOT_DOWNLOAD_TIMEOUT));

    // Authenticate with cluster token for inter-node auth
    final RaftHAServer raftHA = this.raftHA;
    if (raftHA != null && raftHA.getClusterToken() != null)
      connection.setRequestProperty("X-ArcadeDB-Cluster-Token", raftHA.getClusterToken());

    try {
      final int responseCode = connection.getResponseCode();
      if (responseCode != 200)
        throw new ReplicationException(
            "Failed to download snapshot from " + snapshotUrl + ": HTTP " + responseCode);

      // Clean up leftover temp dir from previous failed attempts
      FileUtils.deleteRecursively(tempDir.toFile());
      Files.createDirectories(tempDir);

      try (final ZipInputStream zipIn = new ZipInputStream(connection.getInputStream())) {
        ZipEntry zipEntry;
        while ((zipEntry = zipIn.getNextEntry()) != null) {
          final Path targetFile = tempDir.resolve(zipEntry.getName()).normalize();

          // Security: prevent zip slip
          if (!targetFile.startsWith(tempDir))
            throw new ReplicationException("Zip slip detected in snapshot: " + zipEntry.getName());

          LogManager.instance().log(this, Level.FINE, "Extracting snapshot file: %s", zipEntry.getName());

          if (zipEntry.isDirectory()) {
            Files.createDirectories(targetFile);
          } else {
            Files.createDirectories(targetFile.getParent());
            try (final FileOutputStream fos = new FileOutputStream(targetFile.toFile())) {
              copyWithLimit(zipIn, fos, 10L * 1024 * 1024 * 1024, zipEntry.getName()); // 10 GB per entry
            }
          }
          zipIn.closeEntry();
        }
      } catch (final Exception e) {
        // Extraction failed - clean up temp dir
        FileUtils.deleteRecursively(tempDir.toFile());
        throw e;
      }
    } finally {
      connection.disconnect();
    }
  }

  // -- Crash recovery for snapshot swap --

  /**
   * Scans the database directory for pending snapshot swap markers and completes or rolls back
   * the swap. This must be called on startup BEFORE opening databases, to handle the case
   * where the process crashed mid-swap.
   * <p>
   * Recovery logic:
   * <ul>
   *   <li>If the temp snapshot dir exists, complete the swap (move temp to live, clean up backup)</li>
   *   <li>If the temp snapshot is gone but backup exists, rollback (restore backup to live)</li>
   *   <li>If the live path already exists (swap completed, marker not deleted), just clean up</li>
   * </ul>
   */
  public static void recoverPendingSnapshotSwaps(final Path databaseDir) {
    final File[] markerFiles = databaseDir.toFile().listFiles(
        (dir, name) -> name.endsWith(".snapshot-pending"));
    if (markerFiles == null || markerFiles.length == 0)
      return;

    for (final File marker : markerFiles) {
      final String baseName = marker.getName().replace(".snapshot-pending", "");
      final Path livePath = databaseDir.resolve(baseName);
      final Path backupPath = databaseDir.resolve(baseName + ".snapshot-old");
      final Path snapshotPath = databaseDir.resolve(baseName + ".snapshot-tmp");

      LogManager.instance().log(ArcadeDBStateMachine.class, Level.WARNING,
          "Found pending snapshot swap marker for database '%s', recovering...", baseName);

      try {
        if (Files.exists(snapshotPath)) {
          // Temp snapshot exists - complete the swap
          if (Files.exists(livePath)) {
            // Live path still there (crash before move-away), move it to backup first.
            // Delete stale backup from a previous partial recovery if present
            FileUtils.deleteRecursively(backupPath.toFile());
            Files.move(livePath, backupPath);
          }

          Files.move(snapshotPath, livePath);
          deleteStaleWalFiles(livePath);
          FileUtils.deleteRecursively(backupPath.toFile());

          LogManager.instance().log(ArcadeDBStateMachine.class, Level.INFO,
              "Snapshot swap recovery completed for database '%s'", baseName);

        } else if (Files.exists(backupPath) && !Files.exists(livePath)) {
          // Temp snapshot is gone, backup exists, live path missing - rollback
          Files.move(backupPath, livePath);

          LogManager.instance().log(ArcadeDBStateMachine.class, Level.WARNING,
              "Snapshot swap rolled back for database '%s' (snapshot data was lost)", baseName);

        } else if (Files.exists(livePath)) {
          // Swap already completed, just clean up leftovers
          FileUtils.deleteRecursively(backupPath.toFile());

          LogManager.instance().log(ArcadeDBStateMachine.class, Level.INFO,
              "Snapshot swap already completed for database '%s', cleaning up", baseName);
        }
        // Recovery succeeded - delete the marker
        marker.delete();
      } catch (final IOException e) {
        // Recovery failed - keep the marker so the next restart can retry.
        // Do NOT delete it: the database directory may be in an intermediate state
        // (e.g., live path moved to backup but snapshot not yet moved into place)
        // and the marker is the only signal that recovery is still needed.
        LogManager.instance().log(ArcadeDBStateMachine.class, Level.SEVERE,
            "CRITICAL: Failed to recover snapshot swap for database '%s'. "
                + "The marker file has been preserved for retry on next startup. Error: %s",
            baseName, e.getMessage());
      }
    }
  }

  private static void deleteStaleWalFiles(final Path dbPath) {
    final File[] walFiles = dbPath.toFile().listFiles((dir, name) -> name.endsWith(".wal"));
    if (walFiles != null)
      for (final File walFile : walFiles)
        if (!walFile.delete())
          LogManager.instance().log(ArcadeDBStateMachine.class, Level.WARNING,
              "Failed to delete stale WAL file: %s", walFile.getName());
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
          // No artificial delay needed: downloadSnapshotWithRetry() handles leader
          // unavailability with retries and exponential backoff (5s/10s/20s).
          installDatabasesFromLeader();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to download databases after leader discovery: %s", e.getMessage());
        }
      });
    }

    // Refresh gRPC channels to force fresh DNS resolution after potential network partition
    final RaftHAServer raftHA = this.raftHA;
    if (raftHA != null) {
      // Update cached leader flag (used by applyTransaction origin-skip to avoid Ratis internal calls)
      currentNodeIsLeader = newLeaderId.equals(raftHA.getLocalPeerId());
      raftHA.refreshRaftClient();
      raftHA.notifyLeaderChanged();
    } else
      currentNodeIsLeader = false;

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

  private static void copyWithLimit(final InputStream in, final OutputStream out, final long maxBytes,
      final String entryName) throws IOException {
    final byte[] buf = new byte[8192];
    long total = 0;
    int read;
    while ((read = in.read(buf)) != -1) {
      total += read;
      if (total > maxBytes)
        throw new ReplicationException(
            "Snapshot entry '" + entryName + "' exceeds size limit of " + maxBytes + " bytes");
      out.write(buf, 0, read);
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
