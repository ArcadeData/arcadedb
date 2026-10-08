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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.EmbeddedModifier;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordCallback;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.RecordFactory;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionExplicitLock;
import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.BackupDirectoryResolver;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MaintenanceCoordinator;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.opencypher.optimizer.statistics.GraphStatisticsCache;
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
import com.arcadedb.server.monitor.ProfilingResultSet;
import com.arcadedb.server.monitor.ServerQueryProfiler;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.UnaryOperator;

/**
 * Wrapper of database returned from the server when runs embedded that prevents the close(), drop() and kill() by the user.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ServerDatabase implements DatabaseInternal {
  private final ArcadeDBServer   server;
  private final DatabaseInternal wrapped;

  public ServerDatabase(final ArcadeDBServer server, final DatabaseInternal wrapped) {
    this.server = server;
    this.wrapped = wrapped;

    // BIND THE SERVER'S ADMISSION POLICY TO THE DATABASE ITSELF (issue #7443). 'BACKUP DATABASE' and
    // 'IMPORT DATABASE' are SQL statements the ENGINE executes, and the engine cannot see the server - the
    // dependency runs the other way - so the only thing they can reach is what the database carries. Every one of
    // this server's databases is wrapped here, in the one constructor ArcadeDBServer uses for all four of its open
    // paths, so binding it here covers them all; setWrapper delegates down to the embedded instance, which is the
    // same map the statement reads through whichever wrapper layer it happens to hold.
    if (server != null) {
      wrapped.setWrapper(MaintenanceCoordinator.WRAPPER_NAME, server.getBackupCoordinator());
      // AND THE ONE DEFINITION OF WHERE THIS SERVER KEEPS ITS BACKUPS (issue #7863), for the same reason and
      // through the same channel: 'BACKUP DATABASE' resolved it from arcadedb.server.backupDirectory alone, so an
      // archive it wrote while config/backup.json named a different directory was invisible to 'list backups' and
      // out of reach of 'delete backup' and 'restore backup'. Resolved on CALL rather than captured here, so a
      // 'set backup config' that moves the directory is seen by the next statement, and so the control plane's
      // chain stays the single authority instead of being copied into a field at open time.
      wrapped.setWrapper(BackupDirectoryResolver.WRAPPER_NAME,
          (BackupDirectoryResolver) () -> new ServerControlPlane(server).resolveBackupDirectory().toString());
    }
  }

  /**
   * The instance every operation of this handle runs on: the database's CURRENT wrapper, not the instance this handle
   * was built around (issues #8282, #8383). A handle resolved before the HA plugin wrapped the database holds the plain
   * {@link LocalDatabase}; the wrap installs the replicated wrapper on that same instance
   * ({@link LocalDatabase#setWrappedDatabaseInstance}), so reading it here reaches the wrapper however old the handle is
   * - and past a re-wrap too, for a handle built around a wrapper a plugin restart has since replaced
   * ({@code ArcadeDBServer.rewrapDatabases()}), whose Raft server is the one the restart discarded. Connection-scoped
   * handles - Postgres, MongoDB, Bolt, gRPC - live exactly that long, and what they would bypass is not only the commit:
   * the wrapper's {@code command()} is where a follower forwards a write to the leader instead of executing it on its
   * own state (issue #4039), its {@code query()} is where it applies the read-consistency barrier, and its reads and
   * {@code begin()} are where it refuses a client while the database directory is being replaced.
   * <p>
   * On a database nobody wrapped this is the database itself, and on a handle built around the current wrapper it is
   * that wrapper, so only a stale handle changes behavior. Resolved here and not in {@code LocalDatabase}: engine code
   * calls the inner instance on purpose (a WAL-less vector graph persist, for one), and must keep doing so.
   * <p>
   * Resolved per call, so a transaction whose {@code begin()} and {@code commit()} fall on either side of a re-wrap
   * begins through one wrapper and commits through the next. That is still one transaction: every wrapper delegates to
   * the same embedded {@link LocalDatabase}, and the transaction lives in its thread context, keyed by the database path.
   * <p>
   * {@link #getWrappedDatabaseInstance()}, {@link #getEmbedded()}, {@code equals()}, {@code hashCode()} and
   * {@code toString()} stay on the captured instance: they describe what this handle was built around, which is what the
   * server's own registry maintenance ({@code rewrapDatabases()}) reads.
   */
  private DatabaseInternal current() {
    return wrapped.getEmbedded().getWrappedDatabaseInstance();
  }

  private ServerQueryProfiler getProfiler() {
    return server != null ? server.getQueryProfiler() : null;
  }

  public DatabaseInternal getWrappedDatabaseInstance() {
    return wrapped;
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException("Embedded database taken from the server are shared and therefore cannot be dropped");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Embedded database taken from the server are shared and therefore cannot be closed");
  }

  public void kill() {
    throw new UnsupportedOperationException("Embedded database taken from the server are shared and therefore cannot be killed");
  }

  @Override
  public DatabaseAsyncExecutor async() {
    return current().async();
  }

  public Map<String, Object> getStats() {
    return current().getStats();
  }

  @Override
  public long getModificationCount() {
    return current().getModificationCount();
  }

  @Override
  public String getDatabasePath() {
    return current().getDatabasePath();
  }

  @Override
  public long getSize() {
    return current().getSize();
  }

  @Override
  public String getCurrentUserName() {
    return current().getCurrentUserName();
  }

  @Override
  public Select select() {
    return current().select();
  }

  @Override
  public GraphBatch.Builder batch() {
    return current().batch();
  }

  @Override
  public boolean isReplicated() {
    return current().isReplicated();
  }

  /**
   * Delegated with {@link #isReplicated()}: a server handle around a replicated database must answer the cap its
   * replication layer enforces, not the database's own configuration (issue #9430).
   */
  @Override
  public ContextConfiguration getReplicationConfiguration() {
    return current().getReplicationConfiguration();
  }

  /**
   * Delegated like {@link #isReplicated()}, which it is read together with. Inheriting the interface default
   * ({@code true}) made every handle on a follower answer "replicated, and the leader", so engine code handed a server
   * handle - a graph analytical view built through the embedded API on {@code server.getDatabase()}, for one - that
   * keeps work leader-only by asking {@code isReplicated() && !isLeader()} ran it on a follower anyway. The HA hooks
   * below were likewise inherited as their standalone no-op defaults.
   */
  @Override
  public boolean isLeader() {
    return current().isLeader();
  }

  @Override
  public boolean runWithCompactionReplication(final Callable<Boolean> compaction) throws IOException, InterruptedException {
    return current().runWithCompactionReplication(compaction);
  }

  @Override
  public void recordTimeSeriesSealedChange(final String typeName, final int shardIndex, final String sealedFileName,
      final byte[] sealedBytes) {
    current().recordTimeSeriesSealedChange(typeName, shardIndex, sealedFileName, sealedBytes);
  }

  @Override
  public void countRecordsRead(final long count) {
    current().countRecordsRead(count);
  }

  @Override
  public Map<String, Object> alignToReplicas() {
    throw new UnsupportedOperationException("Align Database not supported");
  }

  @Override
  public Record invokeAfterReadEvents(final Record record) {
    return record;
  }

  public TransactionContext getTransactionIfExists() {
    return current().getTransactionIfExists();
  }

  @Override
  public void begin() {
    current().begin();
  }

  @Override
  public void begin(final TRANSACTION_ISOLATION_LEVEL isolationLevel) {
    current().begin(isolationLevel);
  }

  /**
   * Commits through the database's CURRENT wrapper (issue #8282), like every other operation of this handle: see
   * {@link #current()}.
   */
  @Override
  public void commit() {
    current().commit();
  }

  @Override
  public void rollback() {
    current().rollback();
  }

  @Override
  public void rollbackAllNested() {
    current().rollbackAllNested();
  }

  @Override
  public long countBucket(final String bucketName) {
    return current().countBucket(bucketName);
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    return current().countType(typeName, polymorphic);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    current().scanType(typeName, polymorphic, callback);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback,
      final ErrorRecordCallback errorRecordCallback) {
    current().scanType(typeName, polymorphic, callback, errorRecordCallback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    current().scanBucket(bucketName, callback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback, final ErrorRecordCallback errorRecordCallback) {
    current().scanBucket(bucketName, callback, errorRecordCallback);
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    return current().iterateType(typeName, polymorphic);
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    return current().iterateBucket(bucketName);
  }

  public void checkPermissionsOnDatabase(final SecurityDatabaseUser.DATABASE_ACCESS access) {
    current().checkPermissionsOnDatabase(access);
  }

  public void checkPermissionsOnFile(final int fileId, final SecurityDatabaseUser.ACCESS access) {
    current().checkPermissionsOnFile(fileId, access);
  }

  @Override
  public void checkPermissionsOnType(final String typeName, final SecurityDatabaseUser.ACCESS access) {
    current().checkPermissionsOnType(typeName, access);
  }

  public long getResultSetLimit() {
    return current().getResultSetLimit();
  }

  public long getReadTimeout() {
    return current().getReadTimeout();
  }

  @Override
  public boolean existsRecord(final RID rid) {
    return current().existsRecord(rid);
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    return current().lookupByRID(rid, loadContent);
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String keyName, final Object keyValue) {
    return current().lookupByKey(type, keyName, keyValue);
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String[] keyNames, final Object[] keyValues) {
    return current().lookupByKey(type, keyNames, keyValues);
  }

  public void registerCallback(final DatabaseInternal.CALLBACK_EVENT event, final Callable<Void> callback) {
    current().registerCallback(event, callback);
  }

  public void unregisterCallback(final DatabaseInternal.CALLBACK_EVENT event, final Callable<Void> callback) {
    current().unregisterCallback(event, callback);
  }

  public GraphEngine getGraphEngine() {
    return current().getGraphEngine();
  }

  public TransactionManager getTransactionManager() {
    return current().getTransactionManager();
  }

  @Override
  public boolean isReadYourWrites() {
    return current().isReadYourWrites();
  }

  @Override
  public Database setReadYourWrites(final boolean readYourWrites) {
    current().setReadYourWrites(readYourWrites);
    return this;
  }

  @Override
  public Database setTransactionIsolationLevel(final TRANSACTION_ISOLATION_LEVEL level) {
    return current().setTransactionIsolationLevel(level);
  }

  @Override
  public TRANSACTION_ISOLATION_LEVEL getTransactionIsolationLevel() {
    return current().getTransactionIsolationLevel();
  }

  @Override
  public Database setUseWAL(final boolean useWAL) {
    return current().setUseWAL(useWAL);
  }

  @Override
  public Database setWALFlush(final WALFile.FlushType flush) {
    return current().setWALFlush(flush);
  }

  @Override
  public boolean isAsyncFlush() {
    return current().isAsyncFlush();
  }

  @Override
  public Database setAsyncFlush(final boolean value) {
    return current().setAsyncFlush(value);
  }

  public void createRecord(final MutableDocument record) {
    current().createRecord(record);
  }

  public void createRecord(final Record record, final String bucketName) {
    current().createRecord(record, bucketName);
  }

  public void createRecordNoLock(final Record record, final String bucketName, final boolean discardRecordAfter) {
    current().createRecordNoLock(record, bucketName, discardRecordAfter);
  }

  @Override
  public RID restoreRecord(final Record record, final LocalBucket bucket, final long position) {
    return current().restoreRecord(record, bucket, position);
  }

  public void updateRecord(final Record record) {
    current().updateRecord(record);
  }

  public void updateRecordNoLock(final Record record, final boolean discardRecordAfter) {
    current().updateRecordNoLock(record, discardRecordAfter);
  }

  @Override
  public boolean deleteRecordNoLock(final Record record) {
    return current().deleteRecordNoLock(record);
  }

  @Override
  public void deleteEdgeSkippingEndpoint(final Edge edge, final RID skipEndpoint) {
    current().deleteEdgeSkippingEndpoint(edge, skipEndpoint);
  }

  @Override
  public void deleteRecord(final Record record) {
    current().deleteRecord(record);
  }

  @Override
  public boolean isTransactionActive() {
    return current().isTransactionActive();
  }

  @Override
  public int getNestedTransactions() {
    return current().getNestedTransactions();
  }

  @Override
  public TransactionExplicitLock acquireLock() {
    return current().acquireLock();
  }

  @Override
  public void transaction(final TransactionScope txBlock) {
    current().transaction(txBlock);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx) {
    return current().transaction(txBlock, joinCurrentTx);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int retries) {
    return current().transaction(txBlock, joinCurrentTx, retries);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int attempts, final OkCallback ok,
      final ErrorCallback error) {
    return current().transaction(txBlock, joinCurrentTx, attempts, ok, error);
  }

  public RecordFactory getRecordFactory() {
    return current().getRecordFactory();
  }

  @Override
  public Schema getSchema() {
    return current().getSchema();
  }

  @Override
  public RecordEvents getEvents() {
    return current().getEvents();
  }

  public BinarySerializer getSerializer() {
    return current().getSerializer();
  }

  public PageManager getPageManager() {
    return current().getPageManager();
  }

  @Override
  public MutableDocument newDocument(final String typeName) {
    return current().newDocument(typeName);
  }

  public MutableEmbeddedDocument newEmbeddedDocument(final EmbeddedModifier modifier, final String typeName) {
    return current().newEmbeddedDocument(modifier, typeName);
  }

  @Override
  public MutableVertex newVertex(final String typeName) {
    return current().newVertex(typeName);
  }

  @Override
  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames,
      final Object[] sourceVertexKeyValues, final String destinationVertexType, final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final Object... properties) {
    return current().newEdgeByKeys(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues, destinationVertexType,
        destinationVertexKeyNames, destinationVertexKeyValues, createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType, final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final Object... properties) {
    return current().newEdgeByKeys(sourceVertex, destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues,
        createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public QueryEngine getQueryEngine(final String language) {
    return current().getQueryEngine(language);
  }

  @Override
  public boolean isAutoTransaction() {
    return current().isAutoTransaction();
  }

  @Override
  public void setAutoTransaction(final boolean autoTransaction) {
    current().setAutoTransaction(autoTransaction);
  }

  public FileManager getFileManager() {
    return current().getFileManager();
  }

  @Override
  public String getName() {
    return current().getName();
  }

  @Override
  public ComponentFile.MODE getMode() {
    return current().getMode();
  }

  @Override
  public boolean checkTransactionIsActive(final boolean createTx) {
    return current().checkTransactionIsActive(createTx);
  }

  @Override
  public boolean isAsyncProcessing() {
    return current().isAsyncProcessing();
  }

  @Override
  public void waitForAsyncCompletion() {
    current().waitForAsyncCompletion();
  }

  @Override
  public AsyncQuiesce quiesceAsync() {
    return current().quiesceAsync();
  }

  public DocumentIndexer getIndexer() {
    return current().getIndexer();
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Object... args) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.command(language, query, configuration, args);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.command(language, query, configuration, args);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, query, beginNanos);
  }

  @Override
  public ResultSet command(final String language, final String query) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.command(language, query);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.command(language, query);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, query, beginNanos);
  }

  @Override
  public ResultSet command(final String language, final String query, final Object... parameters) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.command(language, query, parameters);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.command(language, query, parameters);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, query, beginNanos);
  }

  @Override
  public ResultSet command(final String language, final String query, final Map<String, Object> parameters) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.command(language, query, parameters);
    final Map<String, Object> profilingParams = new HashMap<>(parameters);
    profilingParams.put("$profileExecution", true);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.command(language, query, profilingParams);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, query, beginNanos);
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Map<String, Object> args) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.command(language, query, configuration, args);
    final Map<String, Object> profilingArgs = new HashMap<>(args);
    profilingArgs.put("$profileExecution", true);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.command(language, query, configuration, profilingArgs);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, query, beginNanos);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Map<String, Object> params) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.execute(language, script, params);
    final Map<String, Object> profilingParams = new HashMap<>(params);
    profilingParams.put("$profileExecution", true);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.execute(language, script, profilingParams);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, script, beginNanos);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Object... args) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.execute(language, script, args);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.execute(language, script, args);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, script, beginNanos);
  }

  @Override
  public ResultSet query(final String language, final String query) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.query(language, query);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.query(language, query);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, query, beginNanos);
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... parameters) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.query(language, query, parameters);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.query(language, query, parameters);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, query, beginNanos);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> parameters) {
    final DatabaseInternal db = current();
    final ServerQueryProfiler profiler = getProfiler();
    if (profiler == null || !profiler.isRecording())
      return db.query(language, query, parameters);
    final Map<String, Object> profilingParams = new HashMap<>(parameters);
    profilingParams.put("$profileExecution", true);
    final long beginNanos = System.nanoTime();
    final ResultSet rs = db.query(language, query, profilingParams);
    return new ProfilingResultSet(rs, profiler, db.getName(), language, query, beginNanos);
  }

  @Override
  public boolean equals(final Object o) {
    return wrapped.equals(o);
  }

  public DatabaseContext.DatabaseContextTL getContext() {
    return current().getContext();
  }

  @Override
  public <RET> RET executeInReadLock(final Callable<RET> callable) {
    return current().executeInReadLock(callable);
  }

  @Override
  public <RET> RET executeInWriteLock(final Callable<RET> callable) {
    return current().executeInWriteLock(callable);
  }

  @Override
  public <RET> RET executeLockingFiles(final Collection<Integer> fileIds, final Callable<RET> callable) {
    return current().executeLockingFiles(fileIds, callable);
  }

  public <RET> RET recordFileChanges(final Callable<Object> callback) {
    return current().recordFileChanges(callback);
  }

  @Override
  public void saveConfiguration() throws IOException {
    current().saveConfiguration();
  }

  public StatementCache getStatementCache() {
    return current().getStatementCache();
  }

  public ExecutionPlanCache getExecutionPlanCache() {
    return current().getExecutionPlanCache();
  }

  @Override
  public CypherStatementCache getCypherStatementCache() {
    return current().getCypherStatementCache();
  }

  @Override
  public CypherPlanCache getCypherPlanCache() {
    return current().getCypherPlanCache();
  }

  @Override
  public GraphStatisticsCache getGraphStatisticsCache() {
    return current().getGraphStatisticsCache();
  }

  public WALFileFactory getWALFileFactory() {
    return current().getWALFileFactory();
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  public void executeCallbacks(final DatabaseInternal.CALLBACK_EVENT event) throws IOException {
    current().executeCallbacks(event);
  }

  public DatabaseInternal getEmbedded() {
    return wrapped.getEmbedded();
  }

  @Override
  public ContextConfiguration getConfiguration() {
    return current().getConfiguration();
  }

  @Override
  public boolean isOpen() {
    return current().isOpen();
  }

  @Override
  public boolean isFencedForRecovery() {
    return current().isFencedForRecovery();
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }

  public Map<String, Object> getWrappers() {
    return current().getWrappers();
  }

  public void setWrapper(final String name, final Object instance) {
    current().setWrapper(name, instance);
  }

  @Override
  public Object getGlobalVariable(final String name) {
    return current().getGlobalVariable(name);
  }

  @Override
  public Object setGlobalVariable(final String name, final Object value) {
    return current().setGlobalVariable(name, value);
  }

  @Override
  public Object setGlobalVariableIfAbsent(final String name, final Object value) {
    return current().setGlobalVariableIfAbsent(name, value);
  }

  @Override
  public Object setGlobalVariableIfPresent(final String name, final Object value) {
    return current().setGlobalVariableIfPresent(name, value);
  }

  @Override
  public Object computeGlobalVariable(final String name, final UnaryOperator<Object> remapping) {
    return current().computeGlobalVariable(name, remapping);
  }

  @Override
  public Map<String, Object> getGlobalVariables() {
    return current().getGlobalVariables();
  }

  @Override
  public SecurityManager getSecurity() {
    return current().getSecurity();
  }

  @Override
  public long getLastUpdatedOn() {
    return current().getLastUpdatedOn();
  }

  @Override
  public long getLastUsedOn() {
    return current().getLastUsedOn();
  }

  @Override
  public long getOpenedOn() {
    return current().getOpenedOn();
  }
}
