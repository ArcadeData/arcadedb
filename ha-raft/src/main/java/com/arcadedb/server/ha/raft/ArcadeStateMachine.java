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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
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
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Ratis state machine that applies committed log entries to ArcadeDB databases.
 * Handles two types of entries: TX_ENTRY (WAL page diffs) and SCHEMA_ENTRY (DDL commands).
 */
public class ArcadeStateMachine extends BaseStateMachine {

  private final SimpleStateMachineStorage storage          = new SimpleStateMachineStorage();
  private final AtomicLong                lastAppliedIndex = new AtomicLong(-1);
  private final AtomicLong                electionCount    = new AtomicLong(0);
  private volatile long                   lastElectionTime = 0;
  private final long                      startTime        = System.currentTimeMillis();

  private volatile ArcadeDBServer server;
  private volatile RaftHAServer   raftHAServer;

  public void setServer(final ArcadeDBServer server) {
    this.server = server;
  }

  public void setRaftHAServer(final RaftHAServer raftHAServer) {
    this.raftHAServer = raftHAServer;
  }

  /**
   * Initialises the state machine using Ratis-native SimpleStateMachineStorage so that snapshot
   * index tracking is delegated to the framework instead of a hand-rolled text file.
   */
  @Override
  public void initialize(final RaftServer raftServer, final RaftGroupId groupId, final RaftStorage raftStorage) throws IOException {
    super.initialize(raftServer, groupId, raftStorage);
    storage.init(raftStorage);
    reinitialize();
    LogManager.instance().log(this, Level.INFO, "ArcadeStateMachine initialized (groupId=%s)", groupId);
  }

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

  @Override
  public CompletableFuture<Message> applyTransaction(final TransactionContext trx) {
    final LogEntryProto entry = trx.getLogEntry();
    final ByteString data = entry.getStateMachineLogEntry().getLogData();
    final TermIndex termIndex = TermIndex.valueOf(entry);

    try {
      final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(data);

      switch (decoded.type()) {
        case TX_ENTRY -> applyTxEntry(decoded);
        case SCHEMA_ENTRY -> applySchemaEntry(decoded);
        case INSTALL_DATABASE_ENTRY -> applyInstallDatabaseEntry(decoded);
      }

      lastAppliedIndex.set(termIndex.getIndex());
      updateLastAppliedTermIndex(termIndex.getTerm(), termIndex.getIndex());
      // Wake up any threads waiting for this index (READ_YOUR_WRITES, waitForLocalApply)
      if (raftHAServer != null)
        raftHAServer.notifyApplied();
      return CompletableFuture.completedFuture(Message.valueOf("OK"));

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error applying raft log entry at index %d", e, termIndex.getIndex());
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Records a snapshot checkpoint so Ratis can compact the log up to the last-applied index.
   * <p>
   * The ArcadeDB database files on disk are inherently the snapshot state — every committed
   * transaction is already durably flushed by the {@link com.arcadedb.engine.TransactionManager}.
   * Returning the last-applied index here tells Ratis it may purge log entries up to that index,
   * reducing log disk usage over time.
   */
  @Override
  public long takeSnapshot() {
    final long currentIndex = lastAppliedIndex.get();
    if (currentIndex < 0)
      return RaftLog.INVALID_LOG_INDEX;
    HALog.log(this, HALog.BASIC, "ArcadeStateMachine: snapshot checkpoint at index %d", currentIndex);
    return currentIndex;
  }

  /**
   * Called by Ratis when the leader changes for this group. Logs the new leader and
   * this node's role using human-readable display names. Also starts or stops the
   * replica lag monitor depending on whether this node is the new leader.
   */
  @Override
  public void notifyLeaderChanged(final RaftGroupMemberId groupMemberId, final RaftPeerId newLeaderId) {
    super.notifyLeaderChanged(groupMemberId, newLeaderId);

    electionCount.incrementAndGet();
    lastElectionTime = System.currentTimeMillis();

    if (raftHAServer == null || newLeaderId == null)
      return;

    final String leaderName = raftHAServer.getPeerDisplayName(newLeaderId);
    LogManager.instance().log(this, Level.INFO, "Leader elected: %s", leaderName);

    // Recreate the RaftClient so its gRPC channels perform fresh DNS resolution.
    // After a network partition, channels to isolated peers enter TRANSIENT_FAILURE
    // with exponential back-off (up to ~120 s). Refreshing on every leader change
    // ensures the client can reach all peers as soon as the partition heals.
    raftHAServer.refreshRaftClient();

    if (newLeaderId.equals(raftHAServer.getLocalPeerId())) {
      LogManager.instance().log(this, Level.INFO, "This node is now LEADER");
      raftHAServer.startLagMonitor();
    } else {
      LogManager.instance().log(this, Level.INFO, "This node is now REPLICA (leader: %s)", leaderName);
      raftHAServer.stopLagMonitor();
    }

    // Wake up any threads waiting for leadership change (e.g. leaveCluster)
    final Object notifier = raftHAServer.getLeaderChangeNotifier();
    synchronized (notifier) {
      notifier.notifyAll();
    }
  }

  /**
   * Called by Ratis when the follower's log is too far behind the leader's compacted log.
   * Downloads the full database snapshot from the leader via HTTP and replaces local database files.
   */
  @Override
  public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      final RaftProtos.RoleInfoProto roleInfoProto, final TermIndex firstTermIndexInLog) {

    LogManager.instance().log(this, Level.INFO,
        "Snapshot installation requested from leader (firstLogIndex=%s). Starting full resync...", firstTermIndexInLog);

    return CompletableFuture.supplyAsync(() -> {
      try {
        final RaftPeerId leaderId = RaftPeerId.valueOf(
            roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId());
        final String leaderHttpAddr = raftHAServer.getPeerHttpAddress(leaderId);

        if (leaderHttpAddr == null)
          throw new RuntimeException("Cannot determine leader HTTP address for snapshot download");

        final String clusterToken = server.getConfiguration().getValueAsString(
            com.arcadedb.GlobalConfiguration.HA_CLUSTER_TOKEN);

        for (final String dbName : server.getDatabaseNames()) {
          LogManager.instance().log(this, Level.INFO,
              "Installing snapshot for database '%s' from leader %s...", dbName, leaderHttpAddr);
          installDatabaseSnapshot(dbName, leaderHttpAddr, clusterToken);
        }

        LogManager.instance().log(this, Level.INFO, "Full resync from leader completed");
        return firstTermIndexInLog;

      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error during snapshot installation from leader", e);
        throw new RuntimeException("Error during Raft snapshot installation", e);
      }
    });
  }

  private void installDatabaseSnapshot(final String databaseName, final String leaderHttpAddr,
      final String clusterToken) throws java.io.IOException {

    final String snapshotUrl = "http://" + leaderHttpAddr + "/api/v1/ha/snapshot/" + databaseName;
    HALog.log(this, HALog.BASIC, "Downloading snapshot from %s", snapshotUrl);

    final java.net.HttpURLConnection connection;
    try {
      connection = (java.net.HttpURLConnection) new java.net.URI(snapshotUrl).toURL().openConnection();
    } catch (final java.net.URISyntaxException e) {
      throw new java.io.IOException("Invalid snapshot URL: " + snapshotUrl, e);
    }
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(30_000);
    connection.setReadTimeout(300_000);

    if (clusterToken != null && !clusterToken.isEmpty())
      connection.setRequestProperty("X-ArcadeDB-Cluster-Token", clusterToken);

    try {
      final int responseCode = connection.getResponseCode();
      if (responseCode != 200)
        throw new java.io.IOException("Failed to download snapshot: HTTP " + responseCode);

      final DatabaseInternal db = (DatabaseInternal) server.getDatabase(databaseName);
      final String databasePath = db.getDatabasePath();
      db.close();

      final java.nio.file.Path dbPath = java.nio.file.Path.of(databasePath).normalize().toAbsolutePath();
      try (final java.util.zip.ZipInputStream zipIn = new java.util.zip.ZipInputStream(
          connection.getInputStream())) {
        java.util.zip.ZipEntry zipEntry;
        while ((zipEntry = zipIn.getNextEntry()) != null) {
          final java.io.File targetFile = new java.io.File(databasePath, zipEntry.getName());

          if (!targetFile.toPath().normalize().toAbsolutePath().startsWith(dbPath))
            throw new java.io.IOException("Zip slip detected in snapshot: " + zipEntry.getName());

          try (final java.io.FileOutputStream fos = new java.io.FileOutputStream(targetFile)) {
            zipIn.transferTo(fos);
          }
          zipIn.closeEntry();
        }
      }

      final java.io.File dbDir = new java.io.File(databasePath);
      final java.io.File[] walFiles = dbDir.listFiles((dir, name) -> name.endsWith(".wal"));
      if (walFiles != null)
        for (final java.io.File walFile : walFiles)
          if (!walFile.delete())
            LogManager.instance().log(this, Level.WARNING, "Failed to delete stale WAL file: %s", walFile.getName());

      HALog.log(this, HALog.BASIC, "Snapshot for '%s' installed successfully", databaseName);

    } finally {
      connection.disconnect();
    }
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

  private void applyTxEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    // On the leader, the transaction was already applied via commit2ndPhase() in RaftReplicatedDatabase.
    // Only replicas need to apply WAL changes from the state machine.
    if (raftHAServer != null && raftHAServer.isLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping tx apply on leader for database '%s'", decoded.databaseName());
      return;
    }

    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());
    final WALFile.WALTransaction walTx = deserializeWalTransaction(decoded.walData());

    HALog.log(this, HALog.DETAILED, "Applying tx %d to database '%s' (pages=%d)",
        walTx.txId, decoded.databaseName(), walTx.pages.length);

    // ignoreErrors=true: during Raft log replay on restart, log entries may already be applied to the
    // database files (Ratis last-applied tracking can lag behind durable page writes). Skipping
    // already-applied pages (page version >= log version) is safe; version-gap warnings are still logged.
    db.getTransactionManager().applyChanges(walTx, decoded.bucketRecordDelta(), true);
  }

  private void applySchemaEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    // On the leader, schema changes were already applied locally during the transaction
    if (raftHAServer != null && raftHAServer.isLeader()) {
      HALog.log(this, HALog.TRACE, "Skipping schema apply on leader for database '%s'", decoded.databaseName());
      return;
    }

    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());
    final String databasePath = db.getDatabasePath();

    HALog.log(this, HALog.DETAILED,
        "Applying schema entry to database '%s': filesToAdd=%d, filesToRemove=%d, hasSchemaJson=%s",
        decoded.databaseName(),
        decoded.filesToAdd() != null ? decoded.filesToAdd().size() : 0,
        decoded.filesToRemove() != null ? decoded.filesToRemove().size() : 0,
        decoded.schemaJson() != null && !decoded.schemaJson().isEmpty());

    try {
      if (decoded.filesToAdd() != null)
        for (final Map.Entry<Integer, String> fileEntry : decoded.filesToAdd().entrySet())
          db.getFileManager().getOrCreateFile(fileEntry.getKey(), databasePath + File.separator + fileEntry.getValue());

      if (decoded.filesToRemove() != null)
        for (final Map.Entry<Integer, String> fileEntry : decoded.filesToRemove().entrySet()) {
          db.getPageManager().deleteFile(db, fileEntry.getKey());
          db.getFileManager().dropFile(fileEntry.getKey());
          db.getSchema().getEmbedded().removeFile(fileEntry.getKey());
        }

      if (decoded.schemaJson() != null && !decoded.schemaJson().isEmpty())
        db.getSchema().getEmbedded().update(new JSONObject(decoded.schemaJson()));

      // Reload the schema from disk so types, buckets, and file IDs are registered in memory
      db.getSchema().getEmbedded().load(ComponentFile.MODE.READ_WRITE, true);

    } catch (final IOException e) {
      throw new RuntimeException("Failed to apply schema entry for database '" + decoded.databaseName() + "'", e);
    }

    // Apply any WAL entries buffered during schema recording (e.g., initial index page writes)
    // These must be applied after file creation so the target files already exist on the replica
    final List<byte[]> walEntries = decoded.walEntries();
    if (walEntries != null && !walEntries.isEmpty()) {
      final List<Map<Integer, Integer>> bucketDeltas = decoded.bucketDeltas();
      for (int i = 0; i < walEntries.size(); i++) {
        final byte[] walData = walEntries.get(i);
        final Map<Integer, Integer> bucketDelta = (bucketDeltas != null && i < bucketDeltas.size())
            ? bucketDeltas.get(i)
            : Collections.emptyMap();
        final WALFile.WALTransaction walTx = deserializeWalTransaction(walData);
        // ignoreErrors=true: same rationale as applyTxEntry - replay safety during node restart
        db.getTransactionManager().applyChanges(walTx, bucketDelta, true);
      }
      HALog.log(this, HALog.DETAILED,
          "Applied %d buffered WAL entries from schema entry to database '%s'",
          walEntries.size(), decoded.databaseName());
    }

    HALog.log(this, HALog.DETAILED, "Applied schema change to database '%s'", decoded.databaseName());
  }

  private void applyInstallDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    final String databaseName = decoded.databaseName();

    // Skip if the database is already present locally — either because this node created it
    // (leader path) or because a previous Raft replay already applied this entry.
    // Do NOT unconditionally skip on the leader: the createDatabase() HTTP call may have been
    // received by a follower, which created the database locally and committed the Raft entry.
    // In that case the leader has never opened the database and must create it here.
    if (server.existsDatabase(databaseName)) {
      HALog.log(this, HALog.TRACE, "Database '%s' already present, skipping install-database entry", databaseName);
      return;
    }

    server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
    LogManager.instance().log(this, Level.INFO, "Database '%s' created via Raft install-database entry", databaseName);
  }

  /**
   * Deserializes a WAL transaction from raw bytes using the WALFile binary format.
   * <p>
   * Format: txId (long), timestamp (long), pageCount (int), segmentSize (int),
   * then for each page: fileId (int), pageNumber (int), changesFrom (int),
   * changesTo (int), currentPageVersion (int), currentPageSize (int),
   * delta bytes (changesTo - changesFrom + 1).
   */
  static WALFile.WALTransaction deserializeWalTransaction(final byte[] data) {
    final ByteBuffer buf = ByteBuffer.wrap(data);
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();

    tx.txId = buf.getLong();
    tx.timestamp = buf.getLong();
    final int pageCount = buf.getInt();
    buf.getInt(); // segmentSize - not needed for deserialization

    tx.pages = new WALFile.WALPage[pageCount];

    for (int i = 0; i < pageCount; i++) {
      final WALFile.WALPage page = new WALFile.WALPage();
      page.fileId = buf.getInt();
      page.pageNumber = buf.getInt();
      page.changesFrom = buf.getInt();
      page.changesTo = buf.getInt();
      page.currentPageVersion = buf.getInt();
      page.currentPageSize = buf.getInt();

      final int deltaSize = page.changesTo - page.changesFrom + 1;
      final byte[] content = new byte[deltaSize];
      buf.get(content);
      page.currentContent = new Binary(content);

      tx.pages[i] = page;
    }

    return tx;
  }
}
