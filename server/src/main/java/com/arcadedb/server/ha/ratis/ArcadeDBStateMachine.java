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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ReplicationCallback;
import com.arcadedb.server.ha.ReplicationException;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Ratis state machine for ArcadeDB replication. Each committed Raft log entry contains a serialized
 * database transaction (WAL page diffs) that is applied identically on all follower nodes.
 * On the leader, transactions are committed locally via commit2ndPhase() and the state machine
 * apply is skipped to avoid double-application.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeDBStateMachine extends BaseStateMachine {

  private final ArcadeDBServer            server;
  private final SimpleStateMachineStorage storage          = new SimpleStateMachineStorage();
  private final AtomicLong                lastAppliedIndex = new AtomicLong(-1);
  private final AtomicLong                electionCount    = new AtomicLong(0);
  private volatile long                   lastElectionTime = 0;
  private volatile long                   startTime        = System.currentTimeMillis();
  /** True when this follower is replaying log entries to catch up after being behind. */
  private final AtomicBoolean             catchingUp       = new AtomicBoolean(false);

  public ArcadeDBStateMachine(final ArcadeDBServer server) {
    this.server = server;
  }

  @Override
  public void initialize(final RaftServer raftServer, final RaftGroupId groupId, final RaftStorage raftStorage) throws IOException {
    super.initialize(raftServer, groupId, raftStorage);
    storage.init(raftStorage);
    reinitialize();
    LogManager.instance().log(this, Level.INFO, "ArcadeDB Raft state machine initialized (groupId=%s)", groupId);
  }

  @Override
  public void reinitialize() throws IOException {
    final var snapshotInfo = storage.getLatestSnapshot();
    if (snapshotInfo != null)
      lastAppliedIndex.set(snapshotInfo.getIndex());
    else
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

      final RaftLogEntry.EntryType type;
      try {
        type = RaftLogEntry.readType(ByteBuffer.wrap(data));
      } catch (final IllegalArgumentException e) {
        // Unknown entry type - skip (Ratis internal entries like configuration changes)
        LogManager.instance().log(this, Level.FINE, "Skipping unknown Raft log entry at index %d (type byte: %d)", index,
            data.length > 0 ? data[0] : -1);
        lastAppliedIndex.set(index);
        updateLastAppliedTermIndex(logEntry.getTerm(), index);
        return CompletableFuture.completedFuture(Message.EMPTY);
      }

      HALog.log(this, HALog.TRACE, "applyTransaction: index=%d, type=%s, server=%s", index, type, server.getServerName());

      switch (type) {
        case TRANSACTION -> applyTransactionEntry(data);
        case TRANSACTION_FORWARD -> applyTransactionForwardEntry(data);
        case CREATE_DATABASE -> applyCreateDatabase(data);
      }

      final long previousApplied = lastAppliedIndex.getAndSet(index);
      updateLastAppliedTermIndex(logEntry.getTerm(), index);

      // Wake up any threads waiting for this index (READ_YOUR_WRITES, waitForLocalApply)
      final var raftHA = server.getHA();
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

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error applying Raft log entry at index %d", e, index);
      return CompletableFuture.failedFuture(e);
    }
  }

  private void applyTransactionEntry(final byte[] data) {
    final RaftLogEntry.TransactionEntry entry = RaftLogEntry.deserializeTransaction(data);

    final DatabaseInternal db = server.getDatabase(entry.databaseName());
    if (db == null || !db.isOpen())
      throw new ReplicationException("Database '" + entry.databaseName() + "' is not available");

    // On the leader, commit2ndPhase() handles the local page writes AFTER replicateTransaction() returns.
    // Skip the state machine apply to avoid double-applying page changes and bucket record deltas.
    if (isCurrentNodeLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping WAL apply on leader (commit2ndPhase handles it): db=%s", entry.databaseName());
      return;
    }
    HALog.log(this, HALog.DETAILED, "Applying WAL on follower: db=%s, walSize=%d, deltaSize=%d, hasSchema=%s",
        entry.databaseName(), entry.walBuffer() != null ? entry.walBuffer().size() : 0,
        entry.bucketRecordDelta().size(), entry.schemaJson() != null);

    // Phase 1: Create physical files first - WAL pages may reference new file IDs.
    // This must happen before WAL apply so that page writes find the target files.
    if (entry.filesToAdd() != null && !entry.filesToAdd().isEmpty()) {
      try {
        createNewFiles(db, entry.filesToAdd());
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error creating files from Raft log", e);
        throw new ReplicationException("Error creating files from Raft log", e);
      }
    }

    // Phase 2: Apply WAL page changes (if any - schema-only entries have empty WAL buffer)
    if (entry.walBuffer() != null && entry.walBuffer().size() > 0) {
      final WALFile.WALTransaction walTx = parseWalTransaction(entry.walBuffer());

      LogManager.instance().log(this, Level.FINE, "Applying Raft tx %d (modifiedPages=%d, db=%s)...", walTx.txId,
          walTx.pages.length, entry.databaseName());

      db.getTransactionManager().applyChanges(walTx, entry.bucketRecordDelta(), false);
    }

    // Phase 3: Finalize schema - update metadata and remove dropped files AFTER WAL is safely applied.
    // This ordering ensures a WAL failure leaves only orphan empty files (harmless, cleaned up by snapshot)
    // rather than schema-ahead-of-data inconsistency.
    if (entry.schemaJson() != null) {
      try {
        removeDroppedFiles(db, entry.filesToRemove());
        updateSchemaMetadata(db, entry.schemaJson());
        db.getSchema().getEmbedded().load(ComponentFile.MODE.READ_WRITE, false);
        db.getSchema().getEmbedded().initComponents();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error applying schema changes from Raft log", e);
        throw new ReplicationException("Error applying schema changes from Raft log", e);
      }
    }

    // Fire REPLICA_MSG_RECEIVED callback for test infrastructure
    fireCallback(ReplicationCallback.TYPE.REPLICA_MSG_RECEIVED, entry.databaseName());
  }

  private void applyTransactionForwardEntry(final byte[] data) {
    final RaftLogEntry.TransactionForwardEntry entry = RaftLogEntry.deserializeTransactionForward(data);

    final DatabaseInternal db = server.getDatabase(entry.databaseName());
    if (db == null || !db.isOpen())
      throw new ReplicationException("Database '" + entry.databaseName() + "' is not available");

    final WALFile.WALTransaction walTx = parseWalTransaction(entry.walBuffer());

    LogManager.instance().log(this, Level.FINE, "Applying forwarded Raft tx %d (modifiedPages=%d, db=%s)...",
        walTx.txId, walTx.pages.length, entry.databaseName());

    db.getTransactionManager().applyChanges(walTx, entry.bucketRecordDelta(), false);
  }

  private void applyCreateDatabase(final byte[] data) {
    final String databaseName = RaftLogEntry.deserializeCreateDatabase(data);

    // On the leader, the database was already created locally before the Ratis entry was sent.
    if (isCurrentNodeLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping CREATE_DATABASE on leader (already created): db=%s", databaseName);
      return;
    }

    if (server.existsDatabase(databaseName)) {
      HALog.log(this, HALog.BASIC, "Database '%s' already exists on this follower, skipping create", databaseName);
      return;
    }

    HALog.log(this, HALog.BASIC, "Creating database '%s' on follower (replicated from leader)", databaseName);
    server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
  }

  private boolean isCurrentNodeLeader() {
    final RaftHAServer raftHA = server.getHA();
    return raftHA != null && raftHA.isLeader();
  }

  // -- Schema Changes --

  private void createNewFiles(final DatabaseInternal db, final Map<Integer, String> filesToAdd) throws IOException {
    final String databasePath = db.getDatabasePath();
    DatabaseContext.INSTANCE.init(db);
    for (final Map.Entry<Integer, String> entry : filesToAdd.entrySet())
      db.getFileManager().getOrCreateFile(entry.getKey(), databasePath + File.separator + entry.getValue());
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

  // -- Command Forwarding via query() --
  // Commands forwarded from followers are executed via sendReadAfterWrite() -> query().
  // This does NOT go through the Raft log, avoiding deadlock with WAL replication inside the command.
  // The command's side effects (WAL changes, schema changes) are replicated via separate TRANSACTION entries.

  @Override
  public CompletableFuture<Message> query(final Message request) {
    final ByteBuffer buf = request.getContent().asReadOnlyByteBuffer();
    final byte[] data = new byte[buf.remaining()];
    buf.get(data);
    if (data.length > 0 && data[0] == RaftLogEntry.EntryType.COMMAND_FORWARD.code())
      return executeForwardedCommand(data);
    return CompletableFuture.completedFuture(Message.EMPTY);
  }

  private CompletableFuture<Message> executeForwardedCommand(final byte[] data) {
    try {
      final RaftLogEntry.CommandForwardEntry entry = RaftLogEntry.deserializeCommandForward(data);

      LogManager.instance().log(this, Level.FINE, "Executing forwarded command on leader: %s %s (db=%s)",
          entry.language(), entry.command(), entry.databaseName());

      final DatabaseInternal db = server.getDatabase(entry.databaseName());
      if (db == null)
        throw new ReplicationException("Database '" + entry.databaseName() + "' not found for forwarded command");
      DatabaseContext.INSTANCE.init(db);

      HALog.log(this, HALog.DETAILED, "Executing forwarded command on leader: %s %s (db=%s, isLeader=%s)",
          entry.language(), entry.command(), entry.databaseName(), isCurrentNodeLeader());

      final ResultSet rs;
      if (entry.namedParams() != null)
        rs = db.command(entry.language(), entry.command(), entry.namedParams());
      else if (entry.positionalParams() != null)
        rs = db.command(entry.language(), entry.command(), entry.positionalParams());
      else
        rs = db.command(entry.language(), entry.command());

      final byte[] resultBytes = RaftLogEntry.serializeCommandResult(rs);
      return CompletableFuture.completedFuture(Message.valueOf(
          ByteString.copyFrom(resultBytes)));

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing forwarded command", e);
      final String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      final byte[] errBytes = ("E" + errMsg).getBytes(StandardCharsets.UTF_8);
      return CompletableFuture.completedFuture(Message.valueOf(
          ByteString.copyFrom(errBytes)));
    }
  }

  // -- Snapshots --

  /**
   * Returns the last applied index without writing state to SimpleStateMachineStorage.
   * ArcadeDB state lives in the database files on disk, not in the Ratis state machine storage.
   * When a follower is too far behind, notifyInstallSnapshotFromLeader() handles the full resync
   * by downloading the database via HTTP from the leader.
   */
  @Override
  public long takeSnapshot() throws IOException {
    final long currentIndex = lastAppliedIndex.get();
    LogManager.instance().log(this, Level.INFO, "Raft snapshot taken at index %d", currentIndex);
    return currentIndex;
  }

  // -- Follower event: notification-mode snapshot installation --

  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(final RaftProtos.RoleInfoProto roleInfoProto,
      final TermIndex firstTermIndexInLog) {
    LogManager.instance().log(this, Level.INFO,
        "Raft snapshot installation requested from leader (firstLogIndex=%s). Triggering full resync...", firstTermIndexInLog);

    return CompletableFuture.supplyAsync(() -> {
      try {
        // Resolve the leader's HTTP address for snapshot download
        final RaftHAServer raftHA = server.getHA();
        final RaftPeerId leaderId = RaftPeerId.valueOf(
            roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId());
        final String leaderHttpAddr = raftHA.getPeerHTTPAddress(leaderId);

        if (leaderHttpAddr == null)
          throw new ReplicationException("Cannot determine leader HTTP address for snapshot download");

        // Download and install snapshot for each database
        for (final String dbName : server.getDatabaseNames()) {
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
  private void installDatabaseSnapshot(final DatabaseInternal db, final String leaderHttpAddr,
      final String databaseName) throws IOException {
    final boolean useSsl = server.getConfiguration().getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final String snapshotUrl = (useSsl ? "https" : "http") + "://" + leaderHttpAddr + "/api/v1/ha/snapshot/" + databaseName;

    LogManager.instance().log(this, Level.INFO, "Downloading database snapshot from %s...", snapshotUrl);

    final HttpURLConnection connection;
    try {
      connection = (HttpURLConnection) new URI(snapshotUrl).toURL().openConnection();
    } catch (final URISyntaxException e) {
      throw new IOException("Invalid snapshot URL: " + snapshotUrl, e);
    }
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(30_000);
    connection.setReadTimeout(300_000); // 5 minutes for large databases

    // Authenticate with cluster token for inter-node auth
    final var raftHA = server.getHA();
    if (raftHA != null && raftHA.getClusterToken() != null)
      connection.setRequestProperty("X-ArcadeDB-Cluster-Token", raftHA.getClusterToken());

    final String databasePath = db.getDatabasePath();
    final Path dbPath = Path.of(databasePath).normalize().toAbsolutePath();
    final Path tempDir = dbPath.resolveSibling(dbPath.getFileName() + ".snapshot-tmp");
    final Path backupDir = dbPath.resolveSibling(dbPath.getFileName() + ".snapshot-old");

    try {
      final int responseCode = connection.getResponseCode();
      if (responseCode != 200)
        throw new ReplicationException(
            "Failed to download snapshot from " + snapshotUrl + ": HTTP " + responseCode);

      // Clean up leftover temp/backup dirs from previous failed attempts
      deleteDirectoryQuietly(tempDir);
      deleteDirectoryQuietly(backupDir);

      Files.createDirectories(tempDir);

      // Phase 1: Extract to temp directory (database stays open and operational)
      try (final ZipInputStream zipIn = new ZipInputStream(connection.getInputStream())) {
        ZipEntry zipEntry;
        while ((zipEntry = zipIn.getNextEntry()) != null) {
          final Path targetFile = tempDir.resolve(zipEntry.getName()).normalize();

          // Security: prevent zip slip
          if (!targetFile.startsWith(tempDir))
            throw new ReplicationException("Zip slip detected in snapshot: " + zipEntry.getName());

          LogManager.instance().log(this, Level.FINE, "Extracting snapshot file: %s", zipEntry.getName());

          try (final FileOutputStream fos = new FileOutputStream(targetFile.toFile())) {
            copyWithLimit(zipIn, fos, 10L * 1024 * 1024 * 1024, zipEntry.getName()); // 10 GB per entry
          }
          zipIn.closeEntry();
        }
      } catch (final Exception e) {
        // Extraction failed - clean up temp dir and leave database untouched
        deleteDirectoryQuietly(tempDir);
        throw e;
      }

      // Phase 2: Close database and swap directories atomically
      db.close();

      try {
        // Move current database dir out of the way
        Files.move(dbPath, backupDir);

        // Move fully-extracted temp dir into place
        Files.move(tempDir, dbPath);

        // Delete WAL files from the new database dir - they are stale after snapshot installation
        final File[] walFiles = dbPath.toFile().listFiles((dir, name) -> name.endsWith(".wal"));
        if (walFiles != null)
          for (final File walFile : walFiles)
            if (!walFile.delete())
              LogManager.instance().log(this, Level.WARNING, "Failed to delete stale WAL file: %s", walFile.getName());

        // Clean up the backup
        deleteDirectoryQuietly(backupDir);

        LogManager.instance().log(this, Level.INFO, "Database snapshot for '%s' installed successfully", databaseName);

      } catch (final Exception e) {
        // Swap failed - try to restore original database
        LogManager.instance().log(this, Level.SEVERE, "Snapshot swap failed for '%s', attempting rollback...", databaseName);
        try {
          if (Files.exists(backupDir)) {
            deleteDirectoryQuietly(dbPath);   // remove partial move if any
            Files.move(backupDir, dbPath);
          }
        } catch (final Exception rollbackEx) {
          LogManager.instance().log(this, Level.SEVERE,
              "CRITICAL: Failed to rollback snapshot swap for '%s'. Database may be unavailable. Error: %s",
              databaseName, rollbackEx.getMessage());
        }
        deleteDirectoryQuietly(tempDir);
        throw new ReplicationException("Snapshot installation failed during directory swap", e);
      }

    } finally {
      connection.disconnect();
      // Ensure database is re-opened, whether we swapped successfully or rolled back.
      // If reopening fails, the database is effectively unavailable and subsequent Raft applies will
      // cascade-fail. Log at SEVERE so operators can investigate and restart the node.
      try {
        server.getDatabase(databaseName);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "CRITICAL: Failed to reopen database '%s' after snapshot installation. "
                + "This node may need to be restarted to recover. Error: %s",
            databaseName, e.getMessage());
      }
    }
  }

  private static void deleteDirectoryQuietly(final Path dir) {
    if (!Files.exists(dir))
      return;
    try {
      final File[] files = dir.toFile().listFiles();
      if (files != null)
        for (final File f : files)
          f.delete();
      Files.delete(dir);
    } catch (final IOException e) {
      LogManager.instance().log(ArcadeDBStateMachine.class, Level.WARNING,
          "Failed to clean up directory: %s (%s)", dir, e.getMessage());
    }
  }

  // -- Event notifications --

  @Override
  public void notifyLeaderChanged(final RaftGroupMemberId groupMemberId, final RaftPeerId newLeaderId) {
    HALog.log(this, HALog.BASIC, "Leader changed to %s (group: %s)", newLeaderId, groupMemberId);
    electionCount.incrementAndGet();
    lastElectionTime = System.currentTimeMillis();

    // Refresh gRPC channels to force fresh DNS resolution after potential network partition
    final var raftHA = server.getHA();
    if (raftHA != null) {
      raftHA.refreshRaftClient();
      raftHA.notifyLeaderChanged();
    }

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
    // Fire REPLICA_ONLINE for each peer in the new configuration
    for (final var peer : newConf.getPeersList())
      fireCallback(ReplicationCallback.TYPE.REPLICA_ONLINE, peer.getId().toStringUtf8());
  }

  @Override
  public void notifyServerShutdown(final org.apache.ratis.proto.RaftProtos.RoleInfoProto roleInfo, final boolean allServer) {
    HALog.log(this, HALog.BASIC, "Server shutdown notification (allServer=%s)", allServer);
    fireCallback(ReplicationCallback.TYPE.REPLICA_OFFLINE, server.getServerName());
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
      if (pos > buffer.size())
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

      tx.pages[i].currentPageVersion = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].currentPageSize = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final byte[] pageData = new byte[deltaSize];
      tx.pages[i].currentContent = new Binary(pageData);
      buffer.getByteArray(pos, pageData, 0, deltaSize);
      pos += deltaSize;
    }

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
