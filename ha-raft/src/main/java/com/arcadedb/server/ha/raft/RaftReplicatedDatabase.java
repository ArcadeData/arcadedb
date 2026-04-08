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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.database.Record;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.*;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.opencypher.query.CypherPlanCache;
import com.arcadedb.query.opencypher.query.CypherStatementCache;
import com.arcadedb.query.select.Select;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.HAServerPlugin;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.logging.Level;

/**
 * A {@link DatabaseInternal} wrapper that intercepts commit() to submit WAL changes through Raft consensus.
 * On the leader, the transaction is committed locally after Raft consensus is reached.
 * On replicas, the transaction is forwarded to the leader via the Raft client.
 */
public class RaftReplicatedDatabase implements DatabaseInternal, HAReplicatedDatabase {
  /**
   * Carries transaction state between Phase 1 (WAL capture under lock) and
   * Replication (without lock) and Phase 2 (local apply under lock).
   */
  private record ReplicationPayload(
      TransactionContext tx,
      TransactionContext.TransactionPhase1 phase1,
      byte[] walData,
      Map<Integer, Integer> bucketDeltas
  ) {}

  // Thread-local buffers used to accumulate WAL data when commit() is called inside
  // a recordFileChanges() callback. The buffered entries are then embedded in the
  // SCHEMA_ENTRY so replicas receive them atomically with the file-creation step.
  private static final ThreadLocal<List<byte[]>>               schemaWalBuffer        = ThreadLocal.withInitial(ArrayList::new);
  private static final ThreadLocal<List<Map<Integer, Integer>>> schemaBucketDeltaBuffer = ThreadLocal.withInitial(ArrayList::new);

  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  public record ReadConsistencyContext(Database.READ_CONSISTENCY consistency, long readAfterIndex) {
  }

  private static final ThreadLocal<ReadConsistencyContext> READ_CONSISTENCY_CONTEXT = new ThreadLocal<>();

  public static ReadConsistencyContext getReadConsistencyContext() {
    return READ_CONSISTENCY_CONTEXT.get();
  }

  /**
   * Static helper for setting the read consistency context from tests or non-instance contexts.
   */
  static void applyReadConsistencyContext(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
    READ_CONSISTENCY_CONTEXT.set(new ReadConsistencyContext(consistency, readAfterIndex));
  }

  /**
   * Static helper for clearing the read consistency context from tests or non-instance contexts.
   */
  static void removeReadConsistencyContext() {
    READ_CONSISTENCY_CONTEXT.remove();
  }

  @Override
  public void setReadConsistencyContext(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
    READ_CONSISTENCY_CONTEXT.set(new ReadConsistencyContext(consistency, readAfterIndex));
  }

  @Override
  public void clearReadConsistencyContext() {
    READ_CONSISTENCY_CONTEXT.remove();
  }

  @Override
  public long getLastAppliedIndex() {
    return raftHAServer != null ? raftHAServer.getLastAppliedIndex() : -1;
  }

  private final ArcadeDBServer server;
  private final LocalDatabase  proxied;
  private final RaftHAServer   raftHAServer;


  public RaftReplicatedDatabase(final ArcadeDBServer server, final LocalDatabase proxied, final RaftHAServer raftHAServer) {
    this.server = server;
    this.proxied = proxied;
    this.raftHAServer = raftHAServer;
    this.proxied.setWrappedDatabaseInstance(this);
  }

  private RaftHAServer requireRaftServer() {
    final RaftHAServer s = raftHAServer;
    if (s == null)
      throw new NeedRetryException("Raft HA server is not available (server may be restarting)");
    return s;
  }

  @Override
  public void commit() {
    proxied.incrementStatsWriteTx();

    final boolean leader = isLeader();

    // When commit() is called from inside a recordFileChanges() callback on the leader,
    // the files being created do not yet exist on replicas. Sending a TX_ENTRY now would
    // fail on replicas because the target files are missing. Instead, buffer the WAL data
    // here and embed it in the SCHEMA_ENTRY that recordFileChanges() sends after the callback.
    if (leader && proxied.getFileManager().getRecordedChanges() != null) {
      proxied.executeInReadLock(() -> {
        proxied.checkTransactionIsActive(false);
        final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
        final TransactionContext tx = current.getLastTransaction();
        try {
          final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
          if (phase1 != null) {
            tx.commit2ndPhase(phase1);
            schemaWalBuffer.get().add(phase1.result.toByteArray());
            schemaBucketDeltaBuffer.get().add(new HashMap<>(tx.getBucketRecordDelta()));
          } else
            tx.reset();
          if (getSchema().getEmbedded().isDirty())
            getSchema().getEmbedded().saveConfiguration();
        } finally {
          current.popIfNotLastTransaction();
        }
        return null;
      });
      return;
    }

    // --- PHASE 1 (read lock): capture WAL bytes and delta ---
    final ReplicationPayload payload = proxied.executeInReadLock(() -> {
      proxied.checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      final TransactionContext tx = current.getLastTransaction();
      try {
        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(leader);

        if (phase1 != null) {
          final byte[] walData = phase1.result.toByteArray();
          final Map<Integer, Integer> bucketDeltas = new HashMap<>(tx.getBucketRecordDelta());
          return new ReplicationPayload(tx, phase1, walData, bucketDeltas);
        }

        // Read-only transaction: nothing to replicate.
        tx.reset();
        current.popIfNotLastTransaction();
        return null;
      } catch (final NeedRetryException | TransactionException e) {
        rollback();
        throw e;
      } catch (final Exception e) {
        rollback();
        throw new TransactionException("Error on commit distributed transaction (phase 1)", e);
      }
    });

    // Read-only transaction: nothing more to do.
    if (payload == null) {
      // Schema changes made inside a transaction() block (e.g. createVertexType()) are
      // replicated via Raft during recordFileChanges() but LocalSchema.saveConfiguration()
      // was blocked by the active transaction and did not persist the schema to disk on the
      // leader. Now that the transaction context has been reset, trigger the deferred save so
      // the leader increments versionSerial by the same amount as the replicas do in their
      // readConfiguration() finally block.
      if (leader && proxied.getSchema().getEmbedded().isDirty())
        proxied.getSchema().getEmbedded().saveConfiguration();
      return;
    }

    // --- REPLICATION (no lock held): send WAL to Raft and wait for quorum ---
    try {
      final ByteString entry = RaftLogEntryCodec.encodeTxEntry(getName(), payload.walData(), payload.bucketDeltas());
      final RaftHAServer raft = requireRaftServer();
      raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());
    } catch (final ArcadeDBException e) {
      rollback();
      throw e;
    } catch (final Exception e) {
      rollback();
      throw new TransactionException("Error on commit distributed transaction (replication)", e);
    }

    // --- PHASE 2 (read lock on leader): quorum reached, apply locally ---
    if (!leader) {
      payload.tx().reset();
      final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      ctx.popIfNotLastTransaction();
      return;
    }

    proxied.executeInReadLock(() -> {
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      try {
        payload.tx().commit2ndPhase(payload.phase1());

        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Phase 2 commit failed AFTER successful Raft replication (db=%s). "
                + "Stepping down to prevent stale reads. Error: %s", getName(), e.getMessage());
        try {
          if (raftHAServer != null && raftHAServer.isLeader())
            raftHAServer.stepDown();
        } catch (final Exception stepDownEx) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to step down after phase 2 failure (db=%s). Manual restart required.", getName());
        }
        throw e;
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
  public TransactionContext getTransaction() {
    return DatabaseInternal.super.getTransaction();
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
  public GraphBatch.Builder batch() {
    return proxied.batch();
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
    waitForReadConsistency();
    return proxied.query(language, query);
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... args) {
    waitForReadConsistency();
    return proxied.query(language, query, args);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> args) {
    waitForReadConsistency();
    return proxied.query(language, query, args);
  }

  private void waitForReadConsistency() {
    if (isLeader())
      return;

    if (raftHAServer == null)
      return;

    final ReadConsistencyContext ctx = READ_CONSISTENCY_CONTEXT.get();
    if (ctx == null)
      return;

    final Database.READ_CONSISTENCY consistency = ctx.consistency();
    if (consistency == null || consistency == Database.READ_CONSISTENCY.EVENTUAL)
      return;

    if (consistency == Database.READ_CONSISTENCY.READ_YOUR_WRITES) {
      if (ctx.readAfterIndex() >= 0)
        raftHAServer.waitForAppliedIndex(ctx.readAfterIndex());
    } else if (consistency == Database.READ_CONSISTENCY.LINEARIZABLE) {
      if (ctx.readAfterIndex() >= 0)
        raftHAServer.waitForAppliedIndex(ctx.readAfterIndex());
      else
        raftHAServer.waitForLocalApply();
    }
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
  public void setDataEncryption(DataEncryption encryption) {
    DatabaseInternal.super.setDataEncryption(encryption);
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
    if (!isLeader()) {
      final String leaderAddr = raftHAServer != null ? raftHAServer.getLeaderHttpAddress() : null;
      throw new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server",
          leaderAddr != null ? leaderAddr : "");
    }

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

    // Clear thread-local WAL buffers so any commits inside the callback are captured fresh
    schemaWalBuffer.get().clear();
    schemaBucketDeltaBuffer.get().clear();

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

      if (schemaChanged)
        serializedSchema = proxied.getSchema().getEmbedded().toJSON().toString();

      // Collect any WAL entries buffered by commit() calls that occurred inside the callback
      final List<byte[]> walEntries = new ArrayList<>(schemaWalBuffer.get());
      final List<Map<Integer, Integer>> bucketDeltas = new ArrayList<>(schemaBucketDeltaBuffer.get());
      schemaWalBuffer.get().clear();
      schemaBucketDeltaBuffer.get().clear();

      // Send schema changes via Raft so replicas have the files before WAL pages arrive.
      // Embedded walEntries carry the initial page writes (e.g. index root pages) so
      // replicas apply them immediately after creating the files - in the correct order.
      if (!addFiles.isEmpty() || !removeFiles.isEmpty() || schemaChanged) {
        final ByteString schemaEntry = RaftLogEntryCodec.encodeSchemaEntry(getName(), serializedSchema, addFiles, removeFiles, walEntries, bucketDeltas);
        final RaftHAServer raft = requireRaftServer();
        raft.getGroupCommitter().submitAndWait(schemaEntry.toByteArray(), raft.getQuorumTimeout());
        HALog.log(this, HALog.DETAILED,
            "Schema changes replicated via Raft: addFiles=%d, removeFiles=%d, schemaChanged=%s, embeddedWalEntries=%d",
            addFiles.size(), removeFiles.size(), schemaChanged, walEntries.size());
      }

      return result;
    } finally {
      schemaWalBuffer.get().clear();
      schemaBucketDeltaBuffer.get().clear();
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

  @Override
  public boolean isLeader() {
    return raftHAServer != null && raftHAServer.isLeader();
  }

  @Override
  public String getLeaderHttpAddress() {
    return raftHAServer != null ? raftHAServer.getLeaderHttpAddress() : null;
  }

  @Override
  public HAServerPlugin.QUORUM getQuorum() {
    // Raft consensus is inherently majority-based
    return HAServerPlugin.QUORUM.MAJORITY;
  }

  @Override
  public void createInReplicas() {
    final ByteString entry = RaftLogEntryCodec.encodeInstallDatabaseEntry(getName());
    try {
      final RaftHAServer raft = requireRaftServer();
      raft.getGroupCommitter().submitAndWait(entry.toByteArray(), raft.getQuorumTimeout());
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending install-database entry via Raft for database '" + getName() + "'", e);
    }
    LogManager.instance().log(this, Level.INFO, "Database '%s' install-database entry committed via Raft", getName());
  }

  /**
   * Forwards a DDL or leader-only command to the Raft leader via HTTP POST to
   * {@code /api/v1/command/{dbName}}. The response JSON is parsed back into a
   * {@link ResultSet} so the caller sees results transparently.
   */
  private ResultSet forwardCommandToLeaderViaRaft(final String language, final String query,
      final Map<String, Object> mapArgs, final Object[] positionalArgs) {
    final RaftHAServer raft = requireRaftServer();
    final String leaderHttpAddress = raft.getLeaderHttpAddress();
    if (leaderHttpAddress == null)
      throw new TransactionException("Cannot forward command to leader: leader HTTP address is not available");

    final JSONObject body = new JSONObject();
    body.put("language", language);
    body.put("command", query);
    if (mapArgs != null && !mapArgs.isEmpty())
      body.put("params", new JSONObject(mapArgs));
    else if (positionalArgs != null && positionalArgs.length > 0)
      body.put("params", new com.arcadedb.serializer.json.JSONArray(Arrays.asList(positionalArgs)));

    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://" + leaderHttpAddress + "/api/v1/command/" + getName()))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()));

    final String clusterToken = server.getConfiguration().getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
    if (clusterToken != null && !clusterToken.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);

    // When called from the internal Java API (not HTTP), there is no security context and
    // getCurrentUserName() returns null. Fall back to "root" so the cluster token forwarding
    // succeeds - the call is already trusted because it originates in-process. This is safe
    // only when the cluster token header is also enforced on the receiving side.
    final String proxiedUser = proxied.getCurrentUserName();
    final String currentUser = (proxiedUser != null && !proxiedUser.isBlank()) ? proxiedUser : "root";
    builder.header("X-ArcadeDB-Forwarded-User", currentUser);

    try {
      final HttpResponse<String> response = HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200)
        throw new TransactionException(
            "Leader returned HTTP " + response.statusCode() + " for forwarded command: " + response.body());

      return parseResultSetFromJson(response.body());
    } catch (final TransactionException e) {
      throw e;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TransactionException("Interrupted while forwarding command to leader at " + leaderHttpAddress, e);
    } catch (final Exception e) {
      throw new TransactionException("Error forwarding command to leader at " + leaderHttpAddress, e);
    }
  }

  /**
   * Parses the JSON response from the leader's /api/v1/command endpoint into a {@link ResultSet}.
   * Expected format: {@code {"result": [{...}, {...}, ...]}}
   */
  static ResultSet parseResultSetFromJson(final String json) {
    final JSONObject responseJson = new JSONObject(json);
    final InternalResultSet resultSet = new InternalResultSet();

    if (responseJson.has("result")) {
      final Object resultObj = responseJson.get("result");
      if (resultObj instanceof com.arcadedb.serializer.json.JSONArray resultArray) {
        for (int i = 0; i < resultArray.length(); i++) {
          final Object item = resultArray.get(i);
          if (item instanceof JSONObject jsonObj)
            resultSet.add(new ResultInternal(jsonObj.toMap()));
          else
            resultSet.add(new ResultInternal(Map.of("value", item)));
        }
      }
    }

    return resultSet;
  }
}
