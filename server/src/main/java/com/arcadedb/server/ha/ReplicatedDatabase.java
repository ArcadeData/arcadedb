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
import com.arcadedb.database.*;
import com.arcadedb.database.Record;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.*;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
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
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.message.DatabaseChangeStructureRequest;
import com.arcadedb.server.ha.ratis.HALog;
import com.arcadedb.server.ha.ratis.RaftHAServer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class ReplicatedDatabase implements DatabaseInternal {
  protected final ArcadeDBServer server;
  protected final LocalDatabase proxied;
  protected final long timeout;

  public ReplicatedDatabase(final ArcadeDBServer server, final LocalDatabase proxied) {
    if (!server.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL))
      throw new ConfigurationException("Cannot use replicated database if transaction WAL is disabled");

    this.server = server;
    this.proxied = proxied;
    this.timeout = proxied.getConfiguration().getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);
    this.proxied.setWrappedDatabaseInstance(this);
  }

  @Override
  public void commit() {
    final boolean leader = isLeader();
    HALog.log(this, HALog.TRACE, "commit() called: db=%s, isLeader=%s", getName(), leader);

    proxied.executeInReadLock(() -> {
      proxied.checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      final TransactionContext tx = current.getLastTransaction();

      try {
        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(leader);

        try {
          if (phase1 != null) {
            proxied.incrementStatsWriteTx();
            final Binary bufferChanges = phase1.result;

            if (leader)
              replicateFromLeader(tx, phase1, bufferChanges);
            else {
              // Follower writes must go to the leader. Throw so the client retries on the leader.
              tx.reset();
              throw new ServerIsNotTheLeaderException("Write operations must be executed on the leader server",
                  server.getRaftHA().getLeaderHTTPAddress());
            }
          } else {
            proxied.incrementStatsReadTx();
            tx.reset();
          }
        } catch (final NeedRetryException | TransactionException e) {
          rollback();
          throw e;
        } catch (final Exception e) {
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

  /**
   * Leader path: commit locally first, then replicate via Ratis.
   */
  private void replicateFromLeader(final TransactionContext tx, final TransactionContext.TransactionPhase1 phase1,
      final Binary bufferChanges) {
    final RaftHAServer raftHA = server.getRaftHA();

    // Detect schema changes
    String schemaJson = null;
    Map<Integer, String> filesToAdd = null;
    Map<Integer, String> filesToRemove = null;

    final DatabaseChangeStructureRequest changeStructure = getChangeStructure(-1);
    if (changeStructure != null) {
      proxied.getFileManager().stopRecordingChanges();
      proxied.getFileManager().startRecordingChanges();

      schemaJson = changeStructure.getSchemaJson();
      filesToAdd = changeStructure.getFilesToAdd();
      filesToRemove = changeStructure.getFilesToRemove();
    }

    // Capture delta BEFORE commit2ndPhase (which resets the transaction)
    final Map<Integer, Integer> delta = tx.getBucketRecordDelta();
    HALog.log(this, HALog.TRACE, "Captured bucketRecordDelta before commit2ndPhase: %s", delta);

    // DESIGN: "leader commits first" - the transaction is committed locally before Ratis replication.
    // If replicateTransaction() fails (quorum not reached, leader step-down), the local node has the
    // change but followers do not. This is intentional: it avoids holding locks during the Ratis
    // round-trip and lets the leader serve reads immediately. Followers will eventually catch up
    // via Ratis log replay, or via snapshot installation if they fall too far behind.
    // On replication failure, the exception propagates to the caller (HTTP handler), which returns
    // an error to the client - the client should retry.
    tx.commit2ndPhase(phase1);

    // Replicate via Ratis
    HALog.log(this, HALog.DETAILED, "Replicating WAL via Ratis: db=%s, walSize=%d, deltaSize=%d, schema=%s",
        getName(), bufferChanges.size(), delta.size(), schemaJson != null);
    raftHA.replicateTransaction(getName(), delta, bufferChanges, schemaJson, filesToAdd, filesToRemove);
    HALog.log(this, HALog.TRACE, "WAL replication completed: db=%s", getName());
  }

  @Override
  public DatabaseInternal getWrappedDatabaseInstance() {
    return this;
  }

  @Override
  public SecurityManager getSecurity() {
    return server.getSecurity();
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
  public void scanBucket(final String bucketName, final RecordCallback callback, final ErrorRecordCallback errorRecordCallback) {
    proxied.scanBucket(bucketName, callback, errorRecordCallback);
  }

  @Override
  public boolean existsRecord(RID rid) {
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
  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType, final String[] destinationVertexKeyNames,
                            final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
                            final boolean bidirectional, final Object... properties) {

    return proxied.newEdgeByKeys(sourceVertex, destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues,
            createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public QueryEngine getQueryEngine(final String language) {
    return proxied.getQueryEngine(language);
  }

  @Override
  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames,
                            final Object[] sourceVertexKeyValues, final String destinationVertexType, final String[] destinationVertexKeyNames,
                            final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
                            final boolean bidirectional, final Object... properties) {

    return proxied.newEdgeByKeys(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues, destinationVertexType,
            destinationVertexKeyNames, destinationVertexKeyValues, createVertexIfNotExist, edgeType, bidirectional, properties);
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
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int retries, final OkCallback ok,
                             final ErrorCallback error) {
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
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
                           final Object... args) {
    if (!isLeader()) {
      final QueryEngine queryEngine = proxied.getQueryEngineManager().getEngine(language, this);
      final QueryEngine.AnalyzedQuery analyzed = queryEngine.analyze(query);
      if (!analyzed.isIdempotent() || analyzed.isDDL())
        // Throw ServerIsNotTheLeaderException - the HTTP proxy in AbstractServerHttpHandler
        // catches this and forwards the request to the leader transparently
        throw new ServerIsNotTheLeaderException("Write commands must be executed on the leader server",
            server.getRaftHA().getLeaderHTTPAddress());
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
      final QueryEngine.AnalyzedQuery analyzed = queryEngine.analyze(query);
      if (!analyzed.isIdempotent() || analyzed.isDDL())
        throw new ServerIsNotTheLeaderException("Write commands must be executed on the leader server",
            server.getRaftHA().getLeaderHTTPAddress());
    }
    return proxied.command(language, query, configuration, args);
  }

  /**
   * Forwards a command to the leader via the Ratis state machine query() path.
   * No HTTP proxy, no auth issues - uses the already-established gRPC channel.
   * <p>
   * NOTE: Currently unused - kept for future use as an alternative to HTTP proxy forwarding.
   * Do not remove.
   * <p>
   * TODO: The query() path has a page visibility issue - pages modified by the command on the leader
   * are not visible to the follower's local database until the WAL is replayed. Currently using
   * HTTP proxy fallback (see AbstractServerHttpHandler.proxyToLeader) instead.
   */
  private ResultSet forwardCommandToLeader(final String language, final String query, final Map<String, Object> namedParams,
      final Object[] positionalParams) {
    HALog.log(this, HALog.DETAILED, "Forwarding command to leader: %s %s (db=%s)", language, query, getName());

    // Rollback the local transaction started by DatabaseAbstractHandler.transaction() wrapper.
    // The command executes on the leader, so no local changes should be committed.
    if (isTransactionActive())
      rollback();

    final RaftHAServer raftHA = server.getRaftHA();
    final byte[] resultBytes = raftHA.forwardCommand(getName(), language, query, namedParams, positionalParams);
    HALog.log(this, HALog.TRACE, "Command forwarded successfully: %d bytes result", resultBytes.length);

    // Wait for the leader's WAL changes to be applied locally on this follower.
    // Without this, a subsequent read on this server may not see the changes yet.
    raftHA.waitForLocalApply();

    // Check for error response
    if (resultBytes.length > 0 && resultBytes[0] == 'E') {
      final String error = new String(resultBytes, 1, resultBytes.length - 1);
      throw new com.arcadedb.exception.CommandExecutionException(error);
    }

    // Deserialize binary result into ResultSet
    final java.util.List<Map<String, Object>> rows =
        com.arcadedb.server.ha.ratis.RaftLogEntry.deserializeCommandResult(resultBytes);
    final com.arcadedb.query.sql.executor.InternalResultSet rs = new com.arcadedb.query.sql.executor.InternalResultSet();
    for (final Map<String, Object> row : rows) {
      final com.arcadedb.query.sql.executor.ResultInternal result = new com.arcadedb.query.sql.executor.ResultInternal(proxied);
      result.setPropertiesFromMap(row);
      rs.add(result);
    }
    return rs;
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

  /**
   * Waits for the follower to catch up to the required consistency level before executing a read.
   * On the leader, this is a no-op (data is always current).
   * <p>
   * The consistency level and bookmark are read from the current thread's request context
   * (set by the HTTP handler via {@link #setReadConsistencyContext}).
   */
  private void waitForReadConsistency() {
    if (isLeader())
      return;

    final var raftHA = server.getRaftHA();
    if (raftHA == null)
      return;

    final var ctx = READ_CONSISTENCY_CONTEXT.get();
    if (ctx == null)
      return;

    final Database.READ_CONSISTENCY consistency = ctx.consistency;
    if (consistency == null || consistency == Database.READ_CONSISTENCY.EVENTUAL)
      return;

    if (consistency == Database.READ_CONSISTENCY.READ_YOUR_WRITES) {
      if (ctx.readAfterIndex >= 0)
        raftHA.waitForAppliedIndex(ctx.readAfterIndex);
    } else if (consistency == Database.READ_CONSISTENCY.LINEARIZABLE) {
      // Use the client's bookmark if available (from write response headers - reflects the leader's commit index).
      // Otherwise fall back to waiting for the local commit index to be applied.
      if (ctx.readAfterIndex >= 0)
        raftHA.waitForAppliedIndex(ctx.readAfterIndex);
      else
        raftHA.waitForLocalApply();
    }
  }

  /** Thread-local context for read consistency, set by the HTTP handler before query execution. */
  private static final ThreadLocal<ReadConsistencyContext> READ_CONSISTENCY_CONTEXT = new ThreadLocal<>();

  public static void setReadConsistencyContext(final Database.READ_CONSISTENCY consistency, final long readAfterIndex) {
    READ_CONSISTENCY_CONTEXT.set(new ReadConsistencyContext(consistency, readAfterIndex));
  }

  public static void clearReadConsistencyContext() {
    READ_CONSISTENCY_CONTEXT.remove();
  }

  private record ReadConsistencyContext(Database.READ_CONSISTENCY consistency, long readAfterIndex) {}

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

  public <RET> RET recordFileChanges(final Callable<Object> callback) {
    final RaftHAServer raftHA = server.getRaftHA();

    final AtomicReference<Object> result = new AtomicReference<>();

    proxied.executeInWriteLock(() -> {
      if (!isLeader())
        throw new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server",
            raftHA.getLeaderName());

      if (!proxied.getFileManager().startRecordingChanges()) {
        result.set(callback.call());
        return null;
      }

      final long schemaVersionBefore = proxied.getSchema().getEmbedded().getVersion();

      try {
        result.set(callback.call());
        return null;
      } finally {
        final DatabaseChangeStructureRequest command = getChangeStructure(schemaVersionBefore);
        proxied.getFileManager().stopRecordingChanges();

        if (command != null)
          raftHA.replicateTransaction(getName(), Map.of(), new Binary(0), command.getSchemaJson(), command.getFilesToAdd(),
              command.getFilesToRemove());
      }
    });

    return (RET) result.get();
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

  public RaftHAServer.Quorum getQuorum() {
    final RaftHAServer raftHA = server.getRaftHA();
    return raftHA != null ? raftHA.getQuorum() : RaftHAServer.Quorum.MAJORITY;
  }

  /**
   * With Ratis, alignment is handled automatically by the Raft log + snapshot mechanism.
   */
  @Override
  public Map<String, Object> alignToReplicas() {
    LogManager.instance().log(this, Level.INFO, "alignToReplicas() - Raft consensus ensures alignment");
    return Map.of();
  }

  /**
   * With Ratis, new databases are replicated via the Raft state machine on all nodes.
   */
  public void createInReplicas() {
    LogManager.instance().log(this, Level.INFO, "createInReplicas() - Raft handles replication to all peers");
  }

  protected DatabaseChangeStructureRequest getChangeStructure(final long schemaVersionBefore) {
    final List<FileManager.FileChange> fileChanges = proxied.getFileManager().getRecordedChanges();

    final boolean schemaChanged = proxied.getSchema().getEmbedded().isDirty() || //
            schemaVersionBefore < 0 || proxied.getSchema().getEmbedded().getVersion() != schemaVersionBefore;

    if (fileChanges == null ||//
            (fileChanges.isEmpty() && !schemaChanged))
      // NO CHANGES
      return null;

    final Map<Integer, String> addFiles = new HashMap<>();
    final Map<Integer, String> removeFiles = new HashMap<>();
    for (final FileManager.FileChange c : fileChanges) {
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

  protected boolean isLeader() {
    final RaftHAServer raftHA = server.getRaftHA();
    return raftHA != null && raftHA.isLeader();
  }
}
