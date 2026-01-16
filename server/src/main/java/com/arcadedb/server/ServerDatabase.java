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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordCallback;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.RecordFactory;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionExplicitLock;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.opencypher.query.CypherPlanCache;
import com.arcadedb.query.opencypher.query.CypherStatementCache;
import com.arcadedb.query.select.Select;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.BinarySerializer;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Wrapper of database returned from the server when runs embedded that prevents the close(), drop() and kill() by the user.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ServerDatabase implements DatabaseInternal {
  private final DatabaseInternal wrapped;

  public ServerDatabase(final DatabaseInternal wrapped) {
    this.wrapped = wrapped;
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
    return wrapped.async();
  }

  public Map<String, Object> getStats() {
    return wrapped.getStats();
  }

  @Override
  public String getDatabasePath() {
    return wrapped.getDatabasePath();
  }

  @Override
  public long getSize() {
    return wrapped.getSize();
  }

  @Override
  public String getCurrentUserName() {
    return wrapped.getCurrentUserName();
  }

  @Override
  public Select select() {
    return wrapped.select();
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
    return wrapped.getTransactionIfExists();
  }

  @Override
  public void begin() {
    wrapped.begin();
  }

  @Override
  public void begin(final TRANSACTION_ISOLATION_LEVEL isolationLevel) {
    wrapped.begin(isolationLevel);
  }

  @Override
  public void commit() {
    wrapped.commit();
  }

  @Override
  public void rollback() {
    wrapped.rollback();
  }

  @Override
  public void rollbackAllNested() {
    wrapped.rollbackAllNested();
  }

  @Override
  public long countBucket(final String bucketName) {
    return wrapped.countBucket(bucketName);
  }

  @Override
  public long countType(final String typeName, final boolean polymorphic) {
    return wrapped.countType(typeName, polymorphic);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback) {
    wrapped.scanType(typeName, polymorphic, callback);
  }

  @Override
  public void scanType(final String typeName, final boolean polymorphic, final DocumentCallback callback,
      final ErrorRecordCallback errorRecordCallback) {
    wrapped.scanType(typeName, polymorphic, callback, errorRecordCallback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    wrapped.scanBucket(bucketName, callback);
  }

  @Override
  public void scanBucket(final String bucketName, final RecordCallback callback, final ErrorRecordCallback errorRecordCallback) {
    wrapped.scanBucket(bucketName, callback, errorRecordCallback);
  }

  @Override
  public Iterator<Record> iterateType(final String typeName, final boolean polymorphic) {
    return wrapped.iterateType(typeName, polymorphic);
  }

  @Override
  public Iterator<Record> iterateBucket(final String bucketName) {
    return wrapped.iterateBucket(bucketName);
  }

  public void checkPermissionsOnDatabase(final SecurityDatabaseUser.DATABASE_ACCESS access) {
    wrapped.checkPermissionsOnDatabase(access);
  }

  public void checkPermissionsOnFile(final int fileId, final SecurityDatabaseUser.ACCESS access) {
    wrapped.checkPermissionsOnFile(fileId, access);
  }

  public long getResultSetLimit() {
    return wrapped.getResultSetLimit();
  }

  public long getReadTimeout() {
    return wrapped.getReadTimeout();
  }

  @Override
  public boolean existsRecord(final RID rid) {
    return wrapped.existsRecord(rid);
  }

  @Override
  public Record lookupByRID(final RID rid, final boolean loadContent) {
    return wrapped.lookupByRID(rid, loadContent);
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String keyName, final Object keyValue) {
    return wrapped.lookupByKey(type, keyName, keyValue);
  }

  @Override
  public IndexCursor lookupByKey(final String type, final String[] keyNames, final Object[] keyValues) {
    return wrapped.lookupByKey(type, keyNames, keyValues);
  }

  public void registerCallback(final DatabaseInternal.CALLBACK_EVENT event, final Callable<Void> callback) {
    wrapped.registerCallback(event, callback);
  }

  public void unregisterCallback(final DatabaseInternal.CALLBACK_EVENT event, final Callable<Void> callback) {
    wrapped.unregisterCallback(event, callback);
  }

  public GraphEngine getGraphEngine() {
    return wrapped.getGraphEngine();
  }

  public TransactionManager getTransactionManager() {
    return wrapped.getTransactionManager();
  }

  @Override
  public boolean isReadYourWrites() {
    return wrapped.isReadYourWrites();
  }

  @Override
  public Database setReadYourWrites(final boolean readYourWrites) {
    wrapped.setReadYourWrites(readYourWrites);
    return this;
  }

  @Override
  public Database setTransactionIsolationLevel(final TRANSACTION_ISOLATION_LEVEL level) {
    return wrapped.setTransactionIsolationLevel(level);
  }

  @Override
  public TRANSACTION_ISOLATION_LEVEL getTransactionIsolationLevel() {
    return wrapped.getTransactionIsolationLevel();
  }

  @Override
  public int getEdgeListSize() {
    return wrapped.getEdgeListSize();
  }

  @Override
  public Database setUseWAL(final boolean useWAL) {
    return wrapped.setUseWAL(useWAL);
  }

  @Override
  public Database setWALFlush(final WALFile.FlushType flush) {
    return wrapped.setWALFlush(flush);
  }

  @Override
  public boolean isAsyncFlush() {
    return wrapped.isAsyncFlush();
  }

  @Override
  public Database setAsyncFlush(final boolean value) {
    return wrapped.setAsyncFlush(value);
  }

  public void createRecord(final MutableDocument record) {
    wrapped.createRecord(record);
  }

  public void createRecord(final Record record, final String bucketName) {
    wrapped.createRecord(record, bucketName);
  }

  public void createRecordNoLock(final Record record, final String bucketName, final boolean discardRecordAfter) {
    wrapped.createRecordNoLock(record, bucketName, false);
  }

  public void updateRecord(final Record record) {
    wrapped.updateRecord(record);
  }

  public void updateRecordNoLock(final Record record, final boolean discardRecordAfter) {
    wrapped.updateRecordNoLock(record, discardRecordAfter);
  }

  @Override
  public void deleteRecordNoLock(final Record record) {
    wrapped.deleteRecordNoLock(record);
  }

  @Override
  public void deleteRecord(final Record record) {
    wrapped.deleteRecord(record);
  }

  @Override
  public boolean isTransactionActive() {
    return wrapped.isTransactionActive();
  }

  @Override
  public int getNestedTransactions() {
    return wrapped.getNestedTransactions();
  }

  @Override
  public TransactionExplicitLock acquireLock() {
    return wrapped.acquireLock();
  }

  @Override
  public void transaction(final TransactionScope txBlock) {
    wrapped.transaction(txBlock);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx) {
    return wrapped.transaction(txBlock, joinCurrentTx);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int retries) {
    return wrapped.transaction(txBlock, joinCurrentTx, retries);
  }

  @Override
  public boolean transaction(final TransactionScope txBlock, final boolean joinCurrentTx, final int attempts, final OkCallback ok,
      final ErrorCallback error) {
    return wrapped.transaction(txBlock, joinCurrentTx, attempts, ok, error);
  }

  public RecordFactory getRecordFactory() {
    return wrapped.getRecordFactory();
  }

  @Override
  public Schema getSchema() {
    return wrapped.getSchema();
  }

  @Override
  public RecordEvents getEvents() {
    return wrapped.getEvents();
  }

  public BinarySerializer getSerializer() {
    return wrapped.getSerializer();
  }

  public PageManager getPageManager() {
    return wrapped.getPageManager();
  }

  @Override
  public MutableDocument newDocument(final String typeName) {
    return wrapped.newDocument(typeName);
  }

  public MutableEmbeddedDocument newEmbeddedDocument(final EmbeddedModifier modifier, final String typeName) {
    return wrapped.newEmbeddedDocument(modifier, typeName);
  }

  @Override
  public MutableVertex newVertex(final String typeName) {
    return wrapped.newVertex(typeName);
  }

  @Override
  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames,
      final Object[] sourceVertexKeyValues, final String destinationVertexType, final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final Object... properties) {
    return wrapped.newEdgeByKeys(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues, destinationVertexType,
        destinationVertexKeyNames, destinationVertexKeyValues, createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public Edge newEdgeByKeys(final Vertex sourceVertex, final String destinationVertexType, final String[] destinationVertexKeyNames,
      final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final Object... properties) {
    return wrapped.newEdgeByKeys(sourceVertex, destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues,
        createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public QueryEngine getQueryEngine(final String language) {
    return wrapped.getQueryEngine(language);
  }

  @Override
  public boolean isAutoTransaction() {
    return wrapped.isAutoTransaction();
  }

  @Override
  public void setAutoTransaction(final boolean autoTransaction) {
    wrapped.setAutoTransaction(autoTransaction);
  }

  public FileManager getFileManager() {
    return wrapped.getFileManager();
  }

  @Override
  public String getName() {
    return wrapped.getName();
  }

  @Override
  public ComponentFile.MODE getMode() {
    return wrapped.getMode();
  }

  @Override
  public boolean checkTransactionIsActive(final boolean createTx) {
    return wrapped.checkTransactionIsActive(createTx);
  }

  @Override
  public boolean isAsyncProcessing() {
    return wrapped.isAsyncProcessing();
  }

  public DocumentIndexer getIndexer() {
    return wrapped.getIndexer();
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Object... args) {
    return wrapped.command(language, query, configuration, args);
  }

  @Override
  public ResultSet command(final String language, final String query, final Object... parameters) {
    return wrapped.command(language, query, parameters);
  }

  @Override
  public ResultSet command(final String language, final String query, final Map<String, Object> parameters) {
    return wrapped.command(language, query, parameters);
  }

  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Map<String, Object> args) {
    return wrapped.command(language, query, configuration, args);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Map<String, Object> params) {
    return wrapped.execute(language, script, params);
  }

  @Deprecated
  @Override
  public ResultSet execute(final String language, final String script, final Object... args) {
    return wrapped.execute(language, script, args);
  }

  @Override
  public ResultSet query(final String language, final String query, final Object... parameters) {
    return wrapped.query(language, query, parameters);
  }

  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> parameters) {
    return wrapped.query(language, query, parameters);
  }

  @Override
  public boolean equals(final Object o) {
    return wrapped.equals(o);
  }

  public DatabaseContext.DatabaseContextTL getContext() {
    return wrapped.getContext();
  }

  @Override
  public <RET> RET executeInReadLock(final Callable<RET> callable) {
    return wrapped.executeInReadLock(callable);
  }

  @Override
  public <RET> RET executeInWriteLock(final Callable<RET> callable) {
    return wrapped.executeInWriteLock(callable);
  }

  @Override
  public <RET> RET executeLockingFiles(final Collection<Integer> fileIds, final Callable<RET> callable) {
    return wrapped.executeLockingFiles(fileIds, callable);
  }

  public <RET> RET recordFileChanges(final Callable<Object> callback) {
    return wrapped.recordFileChanges(callback);
  }

  @Override
  public void saveConfiguration() throws IOException {
    wrapped.saveConfiguration();
  }

  public StatementCache getStatementCache() {
    return wrapped.getStatementCache();
  }

  public ExecutionPlanCache getExecutionPlanCache() {
    return wrapped.getExecutionPlanCache();
  }

  @Override
  public CypherStatementCache getCypherStatementCache() {
    return wrapped.getCypherStatementCache();
  }

  @Override
  public CypherPlanCache getCypherPlanCache() {
    return wrapped.getCypherPlanCache();
  }

  public WALFileFactory getWALFileFactory() {
    return wrapped.getWALFileFactory();
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  public void executeCallbacks(final DatabaseInternal.CALLBACK_EVENT event) throws IOException {
    wrapped.executeCallbacks(event);
  }

  public DatabaseInternal getEmbedded() {
    return wrapped.getEmbedded();
  }

  @Override
  public ContextConfiguration getConfiguration() {
    return wrapped.getConfiguration();
  }

  @Override
  public boolean isOpen() {
    return wrapped.isOpen();
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }

  @Override
  public Database setEdgeListSize(final int size) {
    wrapped.setEdgeListSize(size);
    return this;
  }

  public int getNewEdgeListSize(final int previousSize) {
    return wrapped.getNewEdgeListSize(previousSize);
  }

  public Map<String, Object> getWrappers() {
    return wrapped.getWrappers();
  }

  public void setWrapper(final String name, final Object instance) {
    wrapped.setWrapper(name, instance);
  }

  @Override
  public long getLastUpdatedOn() {
    return wrapped.getLastUpdatedOn();
  }

  @Override
  public long getLastUsedOn() {
    return wrapped.getLastUsedOn();
  }

  @Override
  public long getOpenedOn() {
    return wrapped.getOpenedOn();
  }
}
