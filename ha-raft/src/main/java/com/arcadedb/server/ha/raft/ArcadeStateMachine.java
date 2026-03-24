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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

/**
 * Ratis state machine that applies committed log entries to ArcadeDB databases.
 * Handles two types of entries: TX_ENTRY (WAL page diffs) and SCHEMA_ENTRY (DDL commands).
 */
public class ArcadeStateMachine extends BaseStateMachine {

  /** File name used to persist the last-applied term and index across restarts. */
  private static final String LAST_APPLIED_FILE = "arcade-last-applied.txt";

  private volatile ArcadeDBServer  server;
  private volatile RaftHAServer   raftHAServer;
  private volatile TermIndex      lastAppliedTermIndex;
  private volatile Path           lastAppliedFile;

  public void setServer(final ArcadeDBServer server) {
    this.server = server;
  }

  public void setRaftHAServer(final RaftHAServer raftHAServer) {
    this.raftHAServer = raftHAServer;
  }

  /**
   * Initialises the state machine. Loads the persisted last-applied index from disk so that Ratis
   * knows not to re-replay log entries that have already been durably applied to the ArcadeDB files.
   */
  @Override
  public void initialize(final RaftServer raftServer, final RaftGroupId groupId, final RaftStorage storage) throws IOException {
    super.initialize(raftServer, groupId, storage);

    // Locate the state-machine subdirectory inside the Raft storage directory
    final File smDir = storage.getStorageDir().getStateMachineDir();
    if (smDir != null) {
      smDir.mkdirs();
      lastAppliedFile = smDir.toPath().resolve(LAST_APPLIED_FILE);
      if (Files.exists(lastAppliedFile)) {
        try {
          final String content = Files.readString(lastAppliedFile, StandardCharsets.UTF_8).trim();
          final String[] parts = content.split(":");
          if (parts.length == 2) {
            final long term  = Long.parseLong(parts[0]);
            final long index = Long.parseLong(parts[1]);
            lastAppliedTermIndex = TermIndex.valueOf(term, index);
            LogManager.instance().log(this, Level.INFO,
                "ArcadeStateMachine: restored lastApplied term=%d index=%d from disk", term, index);
          }
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "ArcadeStateMachine: failed to parse last-applied file, starting fresh", e);
          lastAppliedTermIndex = null;
        }
      }
    }
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

      lastAppliedTermIndex = termIndex;
      persistLastApplied(termIndex);
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
   * <p>
   * <b>Limitation:</b> snapshot-based resync ({@code installSnapshot()}) for replicas that fall
   * behind the compacted log is not yet implemented. Replicas that miss purged log entries
   * cannot catch up automatically and require a manual data copy from the leader's database
   * directory. To minimise this risk, set {@code HA_RAFT_SNAPSHOT_THRESHOLD} high enough that
   * replicas can recover via normal log replay before entries are purged (default: 10 000).
   */
  @Override
  public long takeSnapshot() {
    final TermIndex last = getLastAppliedTermIndex();
    if (last == null)
      return RaftLog.INVALID_LOG_INDEX;
    LogManager.instance().log(this, Level.FINE, "ArcadeStateMachine: snapshot checkpoint at index %d", last.getIndex());
    return last.getIndex();
  }

  @Override
  public TermIndex getLastAppliedTermIndex() {
    return lastAppliedTermIndex;
  }

  /**
   * Writes the last-applied term:index to a small file so the state machine can restore
   * its position on restart and avoid re-replaying already-applied log entries.
   */
  private void persistLastApplied(final TermIndex termIndex) {
    if (lastAppliedFile == null)
      return;
    try {
      final String content = termIndex.getTerm() + ":" + termIndex.getIndex();
      Files.writeString(lastAppliedFile, content, StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "ArcadeStateMachine: failed to persist last-applied index", e);
    }
  }

  private void applyTxEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    // On the leader, the transaction was already applied via commit2ndPhase() in RaftReplicatedDatabase.
    // Only replicas need to apply WAL changes from the state machine.
    if (raftHAServer != null && raftHAServer.isLeader()) {
      LogManager.instance().log(this, Level.FINE, "Skipping tx apply on leader for database '%s'", decoded.databaseName());
      return;
    }

    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());
    final WALFile.WALTransaction walTx = deserializeWalTransaction(decoded.walData());

    LogManager.instance().log(this, Level.FINE, "Applying tx %d to database '%s' (pages=%d)",
        walTx.txId, decoded.databaseName(), walTx.pages.length);

    // ignoreErrors=true: during Raft log replay on restart, log entries may already be applied to the
    // database files (Ratis last-applied tracking can lag behind durable page writes). Skipping
    // already-applied pages (page version >= log version) is safe; version-gap warnings are still logged.
    db.getTransactionManager().applyChanges(walTx, decoded.bucketRecordDelta(), true);
  }

  private void applySchemaEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    // On the leader, schema changes were already applied locally during the transaction
    if (raftHAServer != null && raftHAServer.isLeader()) {
      LogManager.instance().log(this, Level.FINE, "Skipping schema apply on leader for database '%s'", decoded.databaseName());
      return;
    }

    final DatabaseInternal db = (DatabaseInternal) server.getDatabase(decoded.databaseName());
    final String databasePath = db.getDatabasePath();

    LogManager.instance().log(this, Level.FINE,
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
      LogManager.instance().log(this, Level.FINE,
          "Applied %d buffered WAL entries from schema entry to database '%s'",
          walEntries.size(), decoded.databaseName());
    }

    LogManager.instance().log(this, Level.FINE, "Applied schema change to database '%s'", decoded.databaseName());
  }

  private void applyInstallDatabaseEntry(final RaftLogEntryCodec.DecodedEntry decoded) {
    // On the leader the database was already created locally; skip.
    if (raftHAServer != null && raftHAServer.isLeader()) {
      LogManager.instance().log(this, Level.FINE, "Skipping install-database on leader for '%s'", decoded.databaseName());
      return;
    }

    final String databaseName = decoded.databaseName();
    if (server.existsDatabase(databaseName)) {
      LogManager.instance().log(this, Level.FINE, "Database '%s' already present on this replica, skipping install", databaseName);
      return;
    }

    server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
    LogManager.instance().log(this, Level.INFO, "Database '%s' created on replica via Raft install-database entry", databaseName);
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
