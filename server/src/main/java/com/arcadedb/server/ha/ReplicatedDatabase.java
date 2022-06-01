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
package com.arcadedb.server.ha;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.database.EmbeddedModifier;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordCallback;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.RecordFactory;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.message.CommandForwardRequest;
import com.arcadedb.server.ha.message.DatabaseAlignRequest;
import com.arcadedb.server.ha.message.DatabaseAlignResponse;
import com.arcadedb.server.ha.message.DatabaseChangeStructureRequest;
import com.arcadedb.server.ha.message.TxForwardRequest;
import com.arcadedb.server.ha.message.TxRequest;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ReplicatedDatabase implements DatabaseInternal {
  private final ArcadeDBServer   server;
  private final EmbeddedDatabase proxied;
  private final HAServer.QUORUM  quorum;
  private final long             timeout;

  public ReplicatedDatabase(final ArcadeDBServer server, final EmbeddedDatabase proxied) {
    if (!server.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL))
      throw new ConfigurationException("Cannot use replicated database if transaction WAL is disabled");

    this.server = server;
    this.proxied = proxied;
    this.quorum = HAServer.QUORUM.valueOf(proxied.getConfiguration().getValueAsString(GlobalConfiguration.HA_QUORUM).toUpperCase());
    this.timeout = proxied.getConfiguration().getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);
    this.proxied.setWrappedDatabaseInstance(this);
  }

  @Override
  public void commit() {
    proxied.incrementStatsTxCommits();

    final boolean isLeader = server.getHA().isLeader();

    proxied.executeInReadLock(() -> {
      proxied.checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      final TransactionContext tx = current.getLastTransaction();
      try {

        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(isLeader);

        try {
          if (phase1 != null) {
            final Binary bufferChanges = phase1.result;

            if (isLeader)
              replicateTx(tx, phase1, bufferChanges);
            else {
              // USE A BIGGER TIMEOUT CONSIDERING THE DOUBLE LATENCY
              final TxForwardRequest command = new TxForwardRequest(ReplicatedDatabase.this, bufferChanges, tx.getIndexChanges().toMap());
              server.getHA().forwardCommandToLeader(command, timeout * 2);
              tx.reset();
            }
          } else {
            tx.reset();
          }
        } catch (NeedRetryException | TransactionException e) {
          rollback();
          throw e;
        } catch (Exception e) {
          rollback();
          throw new TransactionException("Error on commit distributed transaction", e);
        }

        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();

      } finally {
        current.popIfNotLastTransaction();
      }

      return null;
    });
  }

  public void replicateTx(final TransactionContext tx, final TransactionContext.TransactionPhase1 phase1, final Binary bufferChanges) {
    final int configuredServers = server.getHA().getConfiguredServers();

    final int reqQuorum;
    switch (quorum) {
    case NONE:
      reqQuorum = 0;
      break;
    case ONE:
      reqQuorum = 1;
      break;
    case TWO:
      reqQuorum = 2;
      break;
    case THREE:
      reqQuorum = 3;
      break;
    case MAJORITY:
      reqQuorum = (configuredServers / 2) + 1;
      break;
    case ALL:
      reqQuorum = configuredServers;
      break;
    default:
      throw new IllegalArgumentException("Quorum " + quorum + " not managed");
    }

    final TxRequest req = new TxRequest(getName(), bufferChanges, reqQuorum > 1);

    final DatabaseChangeStructureRequest changeStructureRequest = getChangeStructure(-1);
    if (changeStructureRequest != null) {
      // RESET STRUCTURE CHANGES FROM THIS POINT ONWARDS
      proxied.getFileManager().stopRecordingChanges();
      proxied.getFileManager().startRecordingChanges();
      req.changeStructure = changeStructureRequest;
    }

    server.getHA().sendCommandToReplicasWithQuorum(req, reqQuorum, timeout);

    // COMMIT 2ND PHASE ONLY IF THE QUORUM HAS BEEN REACHED
    tx.commit2ndPhase(phase1);
  }

  public long getTimeout() {
    return timeout;
  }

  public ArcadeDBServer getServer() {
    return server;
  }

  @Override
  public DatabaseInternal getWrappedDatabaseInstance() {
    return this;
  }

  @Override
  public Map<String, Object> getWrappers() {
    return proxied.getWrappers();
  }

  @Override
  public void setWrapper(String name, Object instance) {
    proxied.setWrapper(name, instance);
  }

  @Override
  public void checkPermissionsOnDatabase(final SecurityDatabaseUser.DATABASE_ACCESS access) {
    proxied.checkPermissionsOnDatabase(access);
  }

  @Override
  public void checkPermissionsOnFile(final int fileId, final SecurityDatabaseUser.ACCESS access) {
    proxied.checkPermissionsOnFile(fileId, access);
  }

  @Override
  public long getResultSetLimit() {
    return proxied.getResultSetLimit();
  }

  @Override
  public long getReadTimeout() {
    return proxied.getReadTimeout();
  }

  @Override
  public Map<String, Object> getStats() {
    return proxied.getStats();
  }

  @Override
  public EmbeddedDatabase getEmbedded() {
    return proxied;
  }

  @Override
  public DatabaseContext.DatabaseContextTL getContext() {
    return proxied.getContext();
  }

  @Override
  public void close() {
    proxied.close();
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException("Server proxied database instance cannot be drop");
  }

  @Override
  public void registerCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    proxied.registerCallback(event, callback);
  }

  @Override
  public void unregisterCallback(final CALLBACK_EVENT event, final Callable<Void> callback) {
    proxied.unregisterCallback(event, callback);
  }

  @Override
  public void executeCallbacks(final CALLBACK_EVENT event) throws IOException {
    proxied.executeCallbacks(event);
  }

  @Override
  public GraphEngine getGraphEngine() {
    return proxied.getGraphEngine();
  }

  @Override
  public TransactionManager getTransactionManager() {
    return proxied.getTransactionManager();
  }

  @Override
  public void createRecord(final MutableDocument record) {
    proxied.createRecord(record);
  }

  @Override
  public void createRecord(final Record record, final String bucketName) {
    proxied.createRecord(record, bucketName);
  }

  @Override
  public void createRecordNoLock(final Record record, final String bucketName, boolean discardRecordAfter) {
    proxied.createRecordNoLock(record, bucketName, false);
  }

  @Override
  public void updateRecord(final Record record) {
    proxied.updateRecord(record);
  }

  @Override
  public void updateRecordNoLock(final Record record, boolean discardRecordAfter) {
    proxied.updateRecordNoLock(record, discardRecordAfter);
  }

  @Override
  public void deleteRecordNoLock(final Record record) {
    proxied.deleteRecordNoLock(record);
  }

  @Override
  public DocumentIndexer getIndexer() {
    return proxied.getIndexer();
  }

  @Override
  public void kill() {
    proxied.kill();
  }

  @Override
  public WALFileFactory getWALFileFactory() {
    return proxied.getWALFileFactory();
  }

  @Override
  public StatementCache getStatementCache() {
    return proxied.getStatementCache();
  }

  @Override
  public ExecutionPlanCache getExecutionPlanCache() {
    return proxied.getExecutionPlanCache();
  }

  @Override
  public int getNewEdgeListSize(int previousSize) {
    return proxied.getNewEdgeListSize(previousSize);
  }

  @Override
  public String getName() {
    return proxied.getName();
  }

  @Override
  public PaginatedFile.MODE getMode() {
    return proxied.getMode();
  }

  @Override
  public DatabaseAsyncExecutor async() {
    return proxied.async();
  }

  @Override
  public String getDatabasePath() {
    return proxied.getDatabasePath();
  }

  @Override
  public String getCurrentUserName() {
    return proxied.getCurrentUserName();
  }

  @Override
  public ContextConfiguration getConfiguration() {
    return proxied.getConfiguration();
  }

  @Override
  public TransactionContext getTransaction() {
    return proxied.getTransaction();
  }

  @Override
  public boolean isTransactionActive() {
    return proxied.isTransactionActive();
  }

  @Override
  public boolean checkTransactionIsActive(boolean createTx) {
    return proxied.checkTransactionIsActive(createTx);
  }

  @Override
  public void transaction(final TransactionScope txBlock) {
    proxied.transaction(txBlock);
  }

  @Override
  public boolean isAutoTransaction() {
    return proxied.isAutoTransaction();
  }

  @Override
  public void setAutoTransaction(final boolean autoTransaction) {
    proxied.setAutoTransaction(autoTransaction);
  }

  @Override
  public void begin() {
    proxied.begin();
  }

  @Override
  public void rollback() {
    proxied.rollback();
  }

  @Override
  public void rollbackAllNested() {
    proxied.rollbackAllNested();
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    proxied.scanType(typeName, polymorphic, callback);
  }

  @Override
  public void scanType(String typeName, boolean polymorphic, DocumentCallback callback, ErrorRecordCallback errorRecordCallback) {
    proxied.scanType(typeName, polymorphic, callback, errorRecordCallback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    proxied.scanBucket(bucketName, callback);
  }

  @Override
  public void scanBucket(String bucketName, RecordCallback callback, ErrorRecordCallback errorRecordCallback) {
    proxied.scanBucket(bucketName, callback, errorRecordCallback);
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    return proxied.lookupByRID(rid, loadContent);
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    return proxied.iterateType(typeName, polymorphic);
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    return proxied.iterateBucket(bucketName);
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String keyName, final Object keyValue) {
    return proxied.lookupByKey(type, keyName, keyValue);
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String[] keyNames, final Object[] keyValues) {
    return proxied.lookupByKey(type, keyNames, keyValues);
  }

  @Override
  public void deleteRecord(final Record record) {
    proxied.deleteRecord(record);
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    return proxied.countType(typeName, polymorphic);
  }

  @Override
  public long countBucket(final String bucketName) {
    return proxied.countBucket(bucketName);
  }

  @Override
  public MutableDocument newDocument(final String typeName) {
    return proxied.newDocument(typeName);
  }

  @Override
  public MutableEmbeddedDocument newEmbeddedDocument(final EmbeddedModifier modifier, final String typeName) {
    return proxied.newEmbeddedDocument(modifier, typeName);
  }

  @Override
  public MutableVertex newVertex(String typeName) {
    return proxied.newVertex(typeName);
  }

  @Override
  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType, final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional,
      final Object... properties) {

    return proxied.newEdgeByKeys(sourceVertex, destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues, createVertexIfNotExist, edgeType,
        bidirectional, properties);
  }

  @Override
  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames, final Object[] sourceVertexKeyValues,
      final String destinationVertexType, final String[] destinationVertexKeyNames, final Object[] destinationVertexKeyValues,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional, final Object... properties) {

    return proxied.newEdgeByKeys(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues, destinationVertexType, destinationVertexKeyNames,
        destinationVertexKeyValues, createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public Schema getSchema() {
    return proxied.getSchema();
  }

  @Override
  public RecordEvents getEvents() {
    return proxied.getEvents();
  }

  @Override
  public FileManager getFileManager() {
    return proxied.getFileManager();
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinActiveTx) {
    return proxied.transaction(txBlock, joinActiveTx);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int retries) {
    return proxied.transaction(txBlock, joinCurrentTx, retries);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int retries, final OkCallback ok, final ErrorCallback error) {
    return proxied.transaction(txBlock, joinCurrentTx, retries, ok, error);
  }

  @Override
  public RecordFactory getRecordFactory() {
    return proxied.getRecordFactory();
  }

  @Override
  public BinarySerializer getSerializer() {
    return proxied.getSerializer();
  }

  @Override
  public PageManager getPageManager() {
    return proxied.getPageManager();
  }

  @Override
  public int hashCode() {
    return proxied.hashCode();
  }

  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof Database))
      return false;

    final Database other = (Database) o;
    return Objects.equals(getDatabasePath(), other.getDatabasePath());
  }

  @Override
  public ResultSet command(final String language, final String query, final Object... args) {
    if (!server.getHA().isLeader()) {
      final QueryEngine.AnalyzedQuery analyzed = proxied.getQueryEngineManager().getInstance(language, this).analyze(query);
      if (analyzed.isDDL()) {
        // USE A BIGGER TIMEOUT CONSIDERING THE DOUBLE LATENCY
        final CommandForwardRequest command = new CommandForwardRequest(ReplicatedDatabase.this, language, query, null, args);
        return (ResultSet) server.getHA().forwardCommandToLeader(command, timeout * 2);
      }
      return proxied.command(language, query, args);
    }

    return proxied.command(language, query, args);
  }

  @Override
  public ResultSet command(final String language, final String query, final Map<String, Object> args) {
    if (!server.getHA().isLeader()) {
      final QueryEngine.AnalyzedQuery analyzed = proxied.getQueryEngineManager().getInstance(language, this).analyze(query);
      if (analyzed.isDDL()) {
        // USE A BIGGER TIMEOUT CONSIDERING THE DOUBLE LATENCY
        final CommandForwardRequest command = new CommandForwardRequest(ReplicatedDatabase.this, language, query, args, null);
        return (ResultSet) server.getHA().forwardCommandToLeader(command, timeout * 2);
      }
      return proxied.command(language, query, args);
    }

    return proxied.command(language, query, args);
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... args) {
    return proxied.query(language, query, args);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> args) {
    return proxied.query(language, query, args);
  }

  @Override
  public ResultSet execute(String language, String script, Object... args) {
    return proxied.execute(language, script, args);
  }

  @Override
  public ResultSet execute(String language, String script, Map<String, Object> args) {
    return proxied.execute(language, script, args);
  }

  @Override
  public <RET> RET executeInReadLock(final Callable<RET> callable) {
    return proxied.executeInReadLock(callable);
  }

  @Override
  public <RET> RET executeInWriteLock(final Callable<RET> callable) {
    return proxied.executeInWriteLock(callable);
  }

  @Override
  public boolean isReadYourWrites() {
    return proxied.isReadYourWrites();
  }

  @Override
  public Database setReadYourWrites(final boolean value) {
    proxied.setReadYourWrites(value);
    return this;
  }

  @Override
  public int getEdgeListSize() {
    return proxied.getEdgeListSize();
  }

  @Override
  public Database setEdgeListSize(final int size) {
    proxied.setEdgeListSize(size);
    return this;
  }

  @Override
  public Database setUseWAL(final boolean useWAL) {
    return proxied.setUseWAL(useWAL);
  }

  @Override
  public Database setWALFlush(final WALFile.FLUSH_TYPE flush) {
    return proxied.setWALFlush(flush);
  }

  @Override
  public boolean isAsyncFlush() {
    return proxied.isAsyncFlush();
  }

  @Override
  public Database setAsyncFlush(final boolean value) {
    return proxied.setAsyncFlush(value);
  }

  @Override
  public boolean isOpen() {
    return proxied.isOpen();
  }

  @Override
  public String toString() {
    return proxied.toString();
  }

  public <RET> RET recordFileChanges(final Callable<Object> callback) {
    final HAServer ha = server.getHA();

    final AtomicReference<Object> result = new AtomicReference<>();

    // ACQUIRE A DATABASE WRITE LOCK. THE LOCK IS REENTRANT, SO THE ACQUISITION DOWN THE LINE IS GOING TO PASS BECAUSE ALREADY ACQUIRED HERE
    final DatabaseChangeStructureRequest command = proxied.executeInWriteLock(() -> {
      if (!ha.isLeader()) {
        // NOT THE LEADER: NOT RESPONSIBLE TO SEND CHANGES TO OTHER SERVERS
        // TODO: Issue #118SchemaException
        throw new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server", ha.getLeaderName());
//        result.set(callback.call());
//        return null;
      }

      if (!proxied.getFileManager().startRecordingChanges()) {
        // ALREADY RECORDING
        result.set(callback.call());
        return null;
      }

      final long schemaVersionBefore = proxied.getSchema().getEmbedded().getVersion();

      try {
        result.set(callback.call());

        return getChangeStructure(schemaVersionBefore);

      } finally {
        proxied.getFileManager().stopRecordingChanges();
      }
    });

    if (command != null) {
      // SEND THE COMMAND OUTSIDE THE EXCLUSIVE LOCK
      final int quorum = ha.getConfiguredServers();
      ha.sendCommandToReplicasWithQuorum(command, quorum, timeout);
    }

    return (RET) result.get();
  }

  @Override
  public void saveConfiguration() throws IOException {
    proxied.saveConfiguration();
  }

  /**
   * Aligns the database against all the replicas. This fixes any replication problem occurred by overwriting the database content of replicas. This process
   * first calculates the checksum of every files in the database. Then sends the checksums to the replicas, waiting for a response from each of them about
   * which files differ. In case one or more files differ, a page by page CRC is calculated and sent to the replica. The replica responds with the page id
   * of the page that differs, so the leader will send only the pages that differ to the replica to be overwritten.
   */
  @Override
  public Map<String, Object> alignToReplicas() {
    final HAServer ha = server.getHA();
    if (!ha.isLeader()) {
      // NOT THE LEADER
      throw new ServerIsNotTheLeaderException("Align database can be executed only on the leader server", ha.getLeaderName());
    }

    final Map<String, Object> result = new HashMap<>();

    final int quorum = ha.getConfiguredServers();
    if (quorum == 1)
      // NO ACTIVE NODES
      return result;

    final Map<Integer, Long> fileChecksums = new HashMap<>();
    final Map<Integer, Long> fileSizes = new HashMap<>();

    // ACQUIRE A READ LOCK. TRANSACTION CAN STILL RUN, BUT CREATION OF NEW FILES (BUCKETS, TYPES, INDEXES) WILL BE PUT ON PAUSE UNTIL THIS LOCK IS RELEASED
    executeInReadLock(() -> {
      // AVOID FLUSHING OF DATA PAGES TO DISK
      proxied.getPageManager().suspendPageFlushing(true);
      try {
        final List<PaginatedFile> files = proxied.getFileManager().getFiles();

        for (PaginatedFile paginatedFile : files)
          if (paginatedFile != null) {
            long fileChecksum = paginatedFile.calculateChecksum();
            fileChecksums.put(paginatedFile.getFileId(), fileChecksum);
            fileSizes.put(paginatedFile.getFileId(), paginatedFile.getSize());
          }

        final DatabaseAlignRequest request = new DatabaseAlignRequest(getName(), getSchema().getEmbedded().toJSON().toString(), fileChecksums, fileSizes);
        final List<Object> responsePayloads = ha.sendCommandToReplicasWithQuorum(request, quorum, 120_000);

        if (responsePayloads != null) {
          for (Object o : responsePayloads) {
            final DatabaseAlignResponse response = (DatabaseAlignResponse) o;
            result.put(response.getRemoteServerName(), response.getAlignedPages());
          }
        }

      } finally {
        proxied.getPageManager().suspendPageFlushing(false);
      }
      return null;
    });

    return result;
  }

  private DatabaseChangeStructureRequest getChangeStructure(final long schemaVersionBefore) {
    final List<FileManager.FileChange> fileChanges = proxied.getFileManager().getRecordedChanges();

    final boolean schemaChanged = proxied.getSchema().getEmbedded().isDirty() || //
        schemaVersionBefore < 0 || proxied.getSchema().getEmbedded().getVersion() != schemaVersionBefore;

    if (fileChanges == null ||//
        (fileChanges.isEmpty() && !schemaChanged))
      // NO CHANGES
      return null;

    final Map<Integer, String> addFiles = new HashMap<>();
    final Map<Integer, String> removeFiles = new HashMap<>();
    for (FileManager.FileChange c : fileChanges) {
      if (c.create)
        addFiles.put(c.fileId, c.fileName);
      else
        removeFiles.put(c.fileId, c.fileName);
    }

    final String serializedSchema;
    if (schemaChanged) {
      // SEND THE SCHEMA CONFIGURATION WITH NEXT VERSION (ON CURRENT SERVER WILL BE INCREMENTED + SAVED AT COMMIT TIME)
      final JSONObject schemaJson = proxied.getSchema().getEmbedded().toJSON();
      schemaJson.put("schemaVersion", schemaJson.getLong("schemaVersion") + 1);
      serializedSchema = schemaJson.toString();
    } else
      serializedSchema = "";

    return new DatabaseChangeStructureRequest(proxied.getName(), serializedSchema, addFiles, removeFiles);
  }
}
