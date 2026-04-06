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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.ReplicationException;
import org.apache.ratis.proto.RaftProtos;
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
import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
      }

      lastAppliedIndex.set(index);
      updateLastAppliedTermIndex(logEntry.getTerm(), index);

      // Wake up any threads waiting for this index (READ_YOUR_WRITES, waitForLocalApply)
      final var raftHA = server.getHA();
      if (raftHA != null)
        raftHA.notifyApplied();

      return CompletableFuture.completedFuture(Message.EMPTY);

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error applying Raft log entry at index %d", e, index);
      return CompletableFuture.failedFuture(e);
    }
  }

  private void applyTransactionEntry(final byte[] data) {
    final RaftLogEntry.TransactionEntry entry = RaftLogEntry.deserializeTransaction(data);

    final DatabaseInternal db = server.getDatabase(entry.databaseName());
    if (!db.isOpen())
      throw new ReplicationException("Database '" + entry.databaseName() + "' is closed");

    // On the leader, the transaction was already committed locally via commit2ndPhase().
    // Skip the entire apply to avoid double-applying page changes and bucket record deltas.
    if (isCurrentNodeLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping WAL apply on leader (already committed locally): db=%s", entry.databaseName());
      return;
    }
    HALog.log(this, HALog.DETAILED, "Applying WAL on follower: db=%s, walSize=%d, deltaSize=%d, hasSchema=%s",
        entry.databaseName(), entry.walBuffer() != null ? entry.walBuffer().size() : 0,
        entry.bucketRecordDelta().size(), entry.schemaJson() != null);

    // Follower path: apply schema changes first (if any)
    if (entry.schemaJson() != null) {
      try {
        applySchemaChanges(db, entry.schemaJson(), entry.filesToAdd(), entry.filesToRemove());
        db.getSchema().getEmbedded().load(ComponentFile.MODE.READ_WRITE, false);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error applying schema changes from Raft log", e);
        throw new ReplicationException("Error applying schema changes from Raft log", e);
      }
    }

    // Apply WAL page changes (if any - schema-only entries have empty WAL buffer)
    if (entry.walBuffer() != null && entry.walBuffer().size() > 0) {
      final WALFile.WALTransaction walTx = parseWalTransaction(entry.walBuffer());

      LogManager.instance().log(this, Level.FINE, "Applying Raft tx %d (modifiedPages=%d, db=%s)...", walTx.txId,
          walTx.pages.length, entry.databaseName());

      db.getTransactionManager().applyChanges(walTx, entry.bucketRecordDelta(), false);
    }

    if (entry.schemaJson() != null)
      db.getSchema().getEmbedded().initComponents();

    // Fire REPLICA_MSG_RECEIVED callback for test infrastructure
    fireCallback(com.arcadedb.server.ReplicationCallback.TYPE.REPLICA_MSG_RECEIVED, entry.databaseName());
  }

  private void applyTransactionForwardEntry(final byte[] data) {
    final RaftLogEntry.TransactionForwardEntry entry = RaftLogEntry.deserializeTransactionForward(data);

    final DatabaseInternal db = server.getDatabase(entry.databaseName());
    if (!db.isOpen())
      throw new ReplicationException("Database '" + entry.databaseName() + "' is closed");

    final WALFile.WALTransaction walTx = parseWalTransaction(entry.walBuffer());

    LogManager.instance().log(this, Level.FINE, "Applying forwarded Raft tx %d (modifiedPages=%d, db=%s)...",
        walTx.txId, walTx.pages.length, entry.databaseName());

    db.getTransactionManager().applyChanges(walTx, entry.bucketRecordDelta(), false);
  }

  private boolean isCurrentNodeLeader() {
    final RaftHAServer raftHA = server.getRaftHA();
    return raftHA != null && raftHA.isLeader();
  }

  // -- Schema Changes --

  private void applySchemaChanges(final DatabaseInternal db, final String schemaJson, final Map<Integer, String> filesToAdd,
      final Map<Integer, String> filesToRemove) throws IOException {
    final String databasePath = db.getDatabasePath();

    DatabaseContext.INSTANCE.init(db);

    if (filesToAdd != null)
      for (final Map.Entry<Integer, String> entry : filesToAdd.entrySet())
        db.getFileManager().getOrCreateFile(entry.getKey(), databasePath + File.separator + entry.getValue());

    if (filesToRemove != null)
      for (final Map.Entry<Integer, String> entry : filesToRemove.entrySet()) {
        db.getPageManager().deleteFile(db, entry.getKey());
        db.getFileManager().dropFile(entry.getKey());
        db.getSchema().getEmbedded().removeFile(entry.getKey());
      }

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
      DatabaseContext.INSTANCE.init(db);

      HALog.log(this, HALog.DETAILED, "Executing forwarded command on leader: %s %s (db=%s, isLeader=%s)",
          entry.language(), entry.command(), entry.databaseName(), isCurrentNodeLeader());

      final com.arcadedb.query.sql.executor.ResultSet rs;
      if (entry.namedParams() != null)
        rs = db.command(entry.language(), entry.command(), entry.namedParams());
      else if (entry.positionalParams() != null)
        rs = db.command(entry.language(), entry.command(), entry.positionalParams());
      else
        rs = db.command(entry.language(), entry.command());

      final byte[] resultBytes = RaftLogEntry.serializeCommandResult(rs);
      return CompletableFuture.completedFuture(Message.valueOf(
          org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(resultBytes)));

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error executing forwarded command", e);
      final String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      final byte[] errBytes = ("E" + errMsg).getBytes(java.nio.charset.StandardCharsets.UTF_8);
      return CompletableFuture.completedFuture(Message.valueOf(
          org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(errBytes)));
    }
  }

  // -- Snapshots --

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
        final RaftHAServer raftHA = server.getRaftHA();
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
   */
  private void installDatabaseSnapshot(final DatabaseInternal db, final String leaderHttpAddr,
      final String databaseName) throws IOException {
    final String snapshotUrl = "http://" + leaderHttpAddr + "/api/v1/ha/snapshot/" + databaseName;

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

    try {
      final int responseCode = connection.getResponseCode();
      if (responseCode != 200)
        throw new ReplicationException(
            "Failed to download snapshot from " + snapshotUrl + ": HTTP " + responseCode);

      // Close the database before replacing files
      final String databasePath = db.getDatabasePath();
      db.close();

      // Extract the ZIP to the database directory, overwriting existing files
      final Path dbPath = Path.of(databasePath).normalize().toAbsolutePath();
      try (final ZipInputStream zipIn = new ZipInputStream(connection.getInputStream())) {
        ZipEntry zipEntry;
        while ((zipEntry = zipIn.getNextEntry()) != null) {
          final File targetFile = new File(databasePath, zipEntry.getName());

          // Security: prevent zip slip (lexical check - both sides normalized the same way)
          if (!targetFile.toPath().normalize().toAbsolutePath().startsWith(dbPath))
            throw new ReplicationException("Zip slip detected in snapshot: " + zipEntry.getName());

          LogManager.instance().log(this, Level.FINE, "Extracting snapshot file: %s", targetFile.getName());

          try (final FileOutputStream fos = new FileOutputStream(targetFile)) {
            zipIn.transferTo(fos);
          }
          zipIn.closeEntry();
        }
      }

      // Delete WAL files - they are stale after snapshot installation
      final File dbDir = new File(databasePath);
      final File[] walFiles = dbDir.listFiles((dir, name) -> name.endsWith(".wal"));
      if (walFiles != null)
        for (final File walFile : walFiles)
          if (!walFile.delete())
            LogManager.instance().log(this, Level.WARNING, "Failed to delete stale WAL file: %s", walFile.getName());

      LogManager.instance().log(this, Level.INFO, "Database snapshot for '%s' installed successfully", databaseName);

      // Reopen the database (will be re-loaded by the server on next access)
    } finally {
      connection.disconnect();
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
    if (raftHA != null)
      raftHA.refreshRaftClient();

    fireCallback(com.arcadedb.server.ReplicationCallback.TYPE.LEADER_ELECTED, newLeaderId.toString());
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
      fireCallback(com.arcadedb.server.ReplicationCallback.TYPE.REPLICA_ONLINE, peer.getId().toStringUtf8());
  }

  @Override
  public void notifyServerShutdown(final org.apache.ratis.proto.RaftProtos.RoleInfoProto roleInfo, final boolean allServer) {
    HALog.log(this, HALog.BASIC, "Server shutdown notification (allServer=%s)", allServer);
    fireCallback(com.arcadedb.server.ReplicationCallback.TYPE.REPLICA_OFFLINE, server.getServerName());
  }

  private void fireCallback(final com.arcadedb.server.ReplicationCallback.TYPE type, final Object data) {
    try {
      server.lifecycleEvent(type, data);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error firing %s event: %s", e, type, e.getMessage());
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

    if (pos + segmentSize + Binary.LONG_SERIALIZED_SIZE > buffer.size())
      throw new ReplicationException("Replicated transaction buffer is corrupted");

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

      tx.pages[i].currentPageVersion = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;
      tx.pages[i].currentPageSize = buffer.getInt(pos);
      pos += Binary.INT_SERIALIZED_SIZE;

      final byte[] pageData = new byte[deltaSize];
      tx.pages[i].currentContent = new Binary(pageData);
      buffer.getByteArray(pos, pageData, 0, deltaSize);
      pos += deltaSize;
    }

    final long magicNumber = buffer.getLong(pos + Binary.INT_SERIALIZED_SIZE);
    if (magicNumber != WALFile.MAGIC_NUMBER)
      throw new ReplicationException("Replicated transaction buffer is corrupted (bad magic number)");

    return tx;
  }
}
