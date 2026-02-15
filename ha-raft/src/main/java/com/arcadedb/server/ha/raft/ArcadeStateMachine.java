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
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

/**
 * Ratis state machine that applies committed log entries to ArcadeDB databases.
 * Handles two types of entries: TX_ENTRY (WAL page diffs) and SCHEMA_ENTRY (DDL commands).
 */
public class ArcadeStateMachine extends BaseStateMachine {

  private volatile ArcadeDBServer  server;
  private volatile RaftHAServer   raftHAServer;
  private volatile TermIndex      lastAppliedTermIndex;

  public void setServer(final ArcadeDBServer server) {
    this.server = server;
  }

  public void setRaftHAServer(final RaftHAServer raftHAServer) {
    this.raftHAServer = raftHAServer;
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
      }

      lastAppliedTermIndex = termIndex;
      return CompletableFuture.completedFuture(Message.valueOf("OK"));

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error applying raft log entry at index %d", e, termIndex.getIndex());
      return CompletableFuture.failedFuture(e);
    }
  }

  @Override
  public TermIndex getLastAppliedTermIndex() {
    return lastAppliedTermIndex;
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

    db.getTransactionManager().applyChanges(walTx, decoded.bucketRecordDelta(), false);
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

    LogManager.instance().log(this, Level.FINE, "Applied schema change to database '%s'", decoded.databaseName());
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
