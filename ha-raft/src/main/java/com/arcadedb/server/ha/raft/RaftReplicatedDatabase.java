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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.database.Record;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.*;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.opencypher.query.CypherPlanCache;
import com.arcadedb.query.opencypher.query.CypherStatementCache;
import com.arcadedb.query.select.Select;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * A {@link DatabaseInternal} wrapper that intercepts commit() to submit WAL changes through Raft consensus.
 * On the leader, the transaction is committed locally after Raft consensus is reached.
 * On replicas, the transaction is forwarded to the leader via the Raft client.
 */
public class RaftReplicatedDatabase implements DatabaseInternal {
  private final ArcadeDBServer server;
  private final LocalDatabase  proxied;
  private final RaftHAServer   raftHAServer;


  public RaftReplicatedDatabase(final ArcadeDBServer server, final LocalDatabase proxied, final RaftHAServer raftHAServer) {
    this.server = server;
    this.proxied = proxied;
    this.raftHAServer = raftHAServer;
    this.proxied.setWrappedDatabaseInstance(this);
  }

  @Override
  public void commit() {
    proxied.incrementStatsTxCommits();

    final boolean leader = isLeader();

    proxied.executeInReadLock(() -> {
      proxied.checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      final TransactionContext tx = current.getLastTransaction();
      try {

        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(leader);

        try {
          if (phase1 != null) {
            final Binary bufferChanges = phase1.result;
            final byte[] walData = bufferChanges.toByteArray();
            final Map<Integer, Integer> bucketDeltas = tx.getBucketRecordDelta();
            final ByteString entry = RaftLogEntryCodec.encodeTxEntry(getName(), walData, bucketDeltas);

            final RaftClientReply reply = raftHAServer.getClient().io().send(Message.valueOf(entry));

            if (!reply.isSuccess())
              throw new TransactionException("Raft consensus failed for transaction commit on database '" + getName() + "'");

            if (leader)
              tx.commit2ndPhase(phase1);
            else
              tx.reset();
          } else
            tx.reset();
        } catch (final NeedRetryException | TransactionException e) {
          rollback();
          throw e;
        } catch (final Exception e) {
          rollback();
          throw new TransactionException("Error on commit distributed transaction via Raft", e);
        }

        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();

      } finally {
        current.popIfNotLastTransaction();
      }

      return null;
    });
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Object... args) {
    if (!isLeader()) {
      final QueryEngine queryEngine = proxied.getQueryEngineManager().getEngine(language, this);
      if (queryEngine.isExecutedByTheLeader() || queryEngine.analyze(query).isDDL())
        return forwardCommandToLeaderViaRaft(language, query, null, args);
      return proxied.command(language, query, configuration, args);
    }

    return proxied.command(language, query, configuration, args);
  }

  @Override
  public ResultSet command(final String language, final String query) {
    return command(language, query, server.getConfiguration());
  }

  @Override
  public ResultSet command(final String language, final String query, final Object... args) {
    return command(language, query, server.getConfiguration(), args);
  }

  @Override
  public ResultSet command(final String language, final String query, final Map<String, Object> args) {
    return command(language, query, server.getConfiguration(), args);
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Map<String, Object> args) {
    if (!isLeader()) {
      final QueryEngine queryEngine = proxied.getQueryEngineManager().getEngine(language, this);
      if (queryEngine.isExecutedByTheLeader() || queryEngine.analyze(query).isDDL())
        return forwardCommandToLeaderViaRaft(language, query, args, null);
    }

    return proxied.command(language, query, configuration, args);
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
  public void setWrapper(final String name, final Object instance) {
    proxied.setWrapper(name, instance);
  }

  @Override
  public Object getGlobalVariable(final String name) {
    return proxied.getGlobalVariable(name);
  }

  @Override
  public Object setGlobalVariable(final String name, final Object value) {
    return proxied.setGlobalVariable(name, value);
  }

  @Override
  public Map<String, Object> getGlobalVariables() {
    return proxied.getGlobalVariables();
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
  public LocalDatabase getEmbedded() {
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
  public void createRecordNoLock(final Record record, final String bucketName, final boolean discardRecordAfter) {
    proxied.createRecordNoLock(record, bucketName, discardRecordAfter);
  }

  @Override
  public void updateRecord(final Record record) {
    proxied.updateRecord(record);
  }

  @Override
  public void updateRecordNoLock(final Record record, final boolean discardRecordAfter) {
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
  public CypherStatementCache getCypherStatementCache() {
    return proxied.getCypherStatementCache();
  }

  @Override
  public CypherPlanCache getCypherPlanCache() {
    return proxied.getCypherPlanCache();
  }

  @Override
  public String getName() {
    return proxied.getName();
  }

  @Override
  public ComponentFile.MODE getMode() {
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
  public long getSize() {
    return proxied.getSize();
  }

  @Override
  public String getCurrentUserName() {
    return proxied.getCurrentUserName();
  }

  @Override
  public Select select() {
    return proxied.select();
  }

  @Override
  public ContextConfiguration getConfiguration() {
    return proxied.getConfiguration();
  }

  @Override
  public Record invokeAfterReadEvents(final Record record) {
    return record;
  }

  @Override
  public TransactionContext getTransactionIfExists() {
    return proxied.getTransactionIfExists();
  }

  @Override
  public boolean isTransactionActive() {
    return proxied.isTransactionActive();
  }

  @Override
  public int getNestedTransactions() {
    return proxied.getNestedTransactions();
  }

  @Override
  public boolean checkTransactionIsActive(final boolean createTx) {
    return proxied.checkTransactionIsActive(createTx);
  }

  @Override
  public boolean isAsyncProcessing() {
    return proxied.isAsyncProcessing();
  }

  @Override
  public LocalTransactionExplicitLock acquireLock() {
    return proxied.acquireLock();
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
  public void begin(final TRANSACTION_ISOLATION_LEVEL isolationLevel) {
    proxied.begin(isolationLevel);
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
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback,
      final ErrorRecordCallback errorRecordCallback) {
    proxied.scanType(typeName, polymorphic, callback, errorRecordCallback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    proxied.scanBucket(bucketName, callback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback,
      final ErrorRecordCallback errorRecordCallback) {
    proxied.scanBucket(bucketName, callback, errorRecordCallback);
  }

  @Override
  public boolean existsRecord(final RID rid) {
    return proxied.existsRecord(rid);
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
  public MutableVertex newVertex(final String typeName) {
    return proxied.newVertex(typeName);
  }

  @Override
  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType,
      final String[] destinationVertexKeyNames, final Object[] destinationVertexKeyValues,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional,
      final Object... properties) {
    return proxied.newEdgeByKeys(sourceVertex, destinationVertexType, destinationVertexKeyNames,
        destinationVertexKeyValues, createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public QueryEngine getQueryEngine(final String language) {
    return proxied.getQueryEngine(language);
  }

  @Override
  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames,
      final Object[] sourceVertexKeyValues, final String destinationVertexType,
      final String[] destinationVertexKeyNames, final Object[] destinationVertexKeyValues,
      final boolean createVertexIfNotExist, final String edgeType, final boolean bidirectional,
      final Object... properties) {
    return proxied.newEdgeByKeys(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues,
        destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues, createVertexIfNotExist,
        edgeType, bidirectional, properties);
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
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int retries,
      final OkCallback ok, final ErrorCallback error) {
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
  public ResultSet query(final String language, final String query) {
    return proxied.query(language, query);
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... args) {
    return proxied.query(language, query, args);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> args) {
    return proxied.query(language, query, args);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Object... args) {
    return proxied.execute(language, script, args);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Map<String, Object> args) {
    return proxied.execute(language, script, server.getConfiguration(), args);
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
  public <RET> RET executeLockingFiles(final Collection<Integer> fileIds, final Callable<RET> callable) {
    return proxied.executeLockingFiles(fileIds, callable);
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
  public Database setTransactionIsolationLevel(final TRANSACTION_ISOLATION_LEVEL level) {
    return proxied.setTransactionIsolationLevel(level);
  }

  @Override
  public TRANSACTION_ISOLATION_LEVEL getTransactionIsolationLevel() {
    return proxied.getTransactionIsolationLevel();
  }

  @Override
  public Database setUseWAL(final boolean useWAL) {
    return proxied.setUseWAL(useWAL);
  }

  @Override
  public Database setWALFlush(final WALFile.FlushType flush) {
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
    return proxied.toString() + "[" + server.getServerName() + "]";
  }

  @Override
  public <RET> RET recordFileChanges(final Callable<Object> callback) {
    if (!isLeader())
      return proxied.recordFileChanges(callback);

    // On the leader, record file changes and send them via Raft immediately
    // (like the legacy HA system) so replicas have the files before WAL pages arrive
    final boolean alreadyRecording = !proxied.getFileManager().startRecordingChanges();
    if (alreadyRecording)
      return proxied.recordFileChanges(callback);

    final long schemaVersionBefore = proxied.getSchema().getEmbedded().getVersion();

    // Capture schema changes, then send via Raft after releasing the write lock
    final Map<Integer, String> addFiles = new HashMap<>();
    final Map<Integer, String> removeFiles = new HashMap<>();
    String serializedSchema = "";

    try {
      final RET result = proxied.recordFileChanges(callback);

      // Capture file changes
      final List<FileManager.FileChange> fileChanges = proxied.getFileManager().getRecordedChanges();
      final boolean schemaChanged = proxied.getSchema().getEmbedded().isDirty() ||
          schemaVersionBefore < 0 || proxied.getSchema().getEmbedded().getVersion() != schemaVersionBefore;

      if (fileChanges != null)
        for (final FileManager.FileChange c : fileChanges) {
          if (c.create)
            addFiles.put(c.fileId, c.fileName);
          else
            removeFiles.put(c.fileId, c.fileName);
        }

      if (schemaChanged) {
        final JSONObject schemaJson = proxied.getSchema().getEmbedded().toJSON();
        schemaJson.put("schemaVersion", schemaJson.getLong("schemaVersion") + 1);
        serializedSchema = schemaJson.toString();
      }

      // Send schema changes via Raft so replicas have the files before WAL pages arrive
      if (!addFiles.isEmpty() || !removeFiles.isEmpty() || schemaChanged) {
        final ByteString schemaEntry = RaftLogEntryCodec.encodeSchemaEntry(getName(), serializedSchema, addFiles, removeFiles);
        final RaftClientReply reply = raftHAServer.getClient().io().send(Message.valueOf(schemaEntry));
        if (!reply.isSuccess())
          throw new TransactionException("Raft consensus failed for schema change on database '" + getName() + "'");
        LogManager.instance().log(this, Level.FINE, "Schema changes replicated via Raft: addFiles=%d, removeFiles=%d, schemaChanged=%s",
            addFiles.size(), removeFiles.size(), schemaChanged);
      }

      return result;
    } catch (final IOException e) {
      throw new TransactionException("Error sending schema changes via Raft", e);
    } finally {
      proxied.getFileManager().stopRecordingChanges();
    }
  }

  @Override
  public void saveConfiguration() throws IOException {
    proxied.saveConfiguration();
  }

  @Override
  public long getLastUpdatedOn() {
    return proxied.getLastUpdatedOn();
  }

  @Override
  public long getLastUsedOn() {
    return proxied.getLastUsedOn();
  }

  @Override
  public long getOpenedOn() {
    return proxied.getOpenedOn();
  }

  @Override
  public Map<String, Object> alignToReplicas() {
    return proxied.alignToReplicas();
  }

  @Override
  public SecurityManager getSecurity() {
    return proxied.getSecurity();
  }

  public boolean isLeader() {
    return raftHAServer != null && raftHAServer.isLeader();
  }

  /**
   * Forwards a DDL or leader-only command to the Raft leader via the Raft client.
   * This is a placeholder that will be fully implemented when the command forwarding
   * protocol is finalized.
   */
  private ResultSet forwardCommandToLeaderViaRaft(final String language, final String query,
      final Map<String, Object> mapArgs, final Object[] positionalArgs) {
    // TODO: Implement command forwarding via Raft log entry (schema entry type)
    throw new UnsupportedOperationException(
        "Command forwarding to leader via Raft is not yet implemented for language='" + language + "', query='" + query + "'");
  }
}
