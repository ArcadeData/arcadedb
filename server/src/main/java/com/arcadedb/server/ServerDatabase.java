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
import com.arcadedb.database.RecordFactory;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
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
    throw new UnsupportedOperationException("Embedded database taken from the server cannot be dropped");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Embedded database taken from the server cannot be closed");
  }

  public void kill() {
    throw new UnsupportedOperationException("Embedded database taken from the server cannot be killed");
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
  public String getCurrentUserName() {
    return wrapped.getCurrentUserName();
  }

  public TransactionContext getTransaction() {
    return wrapped.getTransaction();
  }

  @Override
  public void begin() {
    wrapped.begin();
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
  public long countBucket(String bucketName) {
    return wrapped.countBucket(bucketName);
  }

  @Override
  public long countType(String typeName, boolean polymorphic) {
    return wrapped.countType(typeName, polymorphic);
  }

  @Override
  public void scanType(String typeName, boolean polymorphic, DocumentCallback callback) {
    wrapped.scanType(typeName, polymorphic, callback);
  }

  @Override
  public void scanBucket(String bucketName, RecordCallback callback) {
    wrapped.scanBucket(bucketName, callback);
  }

  @Override
  public Iterator<Record> iterateType(String typeName, boolean polymorphic) {
    return wrapped.iterateType(typeName, polymorphic);
  }

  @Override
  public Iterator<Record> iterateBucket(String bucketName) {
    return wrapped.iterateBucket(bucketName);
  }

  public void checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS access) {
    wrapped.checkPermissionsOnDatabase(access);
  }

  public void checkPermissionsOnFile(int fileId, SecurityDatabaseUser.ACCESS access) {
    wrapped.checkPermissionsOnFile(fileId, access);
  }

  public long getResultSetLimit() {
    return wrapped.getResultSetLimit();
  }

  public long getReadTimeout() {
    return wrapped.getReadTimeout();
  }

  @Override
  public Record lookupByRID(RID rid, boolean loadContent) {
    return wrapped.lookupByRID(rid, loadContent);
  }

  @Override
  public IndexCursor lookupByKey(String type, String keyName, Object keyValue) {
    return wrapped.lookupByKey(type, keyName, keyValue);
  }

  @Override
  public IndexCursor lookupByKey(String type, String[] keyNames, Object[] keyValues) {
    return wrapped.lookupByKey(type, keyNames, keyValues);
  }

  public void registerCallback(DatabaseInternal.CALLBACK_EVENT event, Callable<Void> callback) {
    wrapped.registerCallback(event, callback);
  }

  public void unregisterCallback(DatabaseInternal.CALLBACK_EVENT event, Callable<Void> callback) {
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
  public void setReadYourWrites(boolean readYourWrites) {
    wrapped.setReadYourWrites(readYourWrites);
  }

  @Override
  public Database setUseWAL(boolean useWAL) {
    return wrapped.setUseWAL(useWAL);
  }

  @Override
  public Database setWALFlush(WALFile.FLUSH_TYPE flush) {
    return wrapped.setWALFlush(flush);
  }

  @Override
  public boolean isAsyncFlush() {
    return wrapped.isAsyncFlush();
  }

  @Override
  public Database setAsyncFlush(boolean value) {
    return wrapped.setAsyncFlush(value);
  }

  public void createRecord(MutableDocument record) {
    wrapped.createRecord(record);
  }

  public void createRecord(Record record, String bucketName) {
    wrapped.createRecord(record, bucketName);
  }

  public void createRecordNoLock(Record record, String bucketName) {
    wrapped.createRecordNoLock(record, bucketName);
  }

  public void updateRecord(Record record) {
    wrapped.updateRecord(record);
  }

  public void updateRecordNoLock(Record record) {
    wrapped.updateRecordNoLock(record);
  }

  @Override
  public void deleteRecord(Record record) {
    wrapped.deleteRecord(record);
  }

  @Override
  public boolean isTransactionActive() {
    return wrapped.isTransactionActive();
  }

  @Override
  public void transaction(TransactionScope txBlock) {
    wrapped.transaction(txBlock);
  }

  @Override
  public boolean transaction(TransactionScope txBlock, boolean joinCurrentTx) {
    return wrapped.transaction(txBlock, joinCurrentTx);
  }

  @Override
  public boolean transaction(TransactionScope txBlock, boolean joinCurrentTx, int retries) {
    return wrapped.transaction(txBlock, joinCurrentTx, retries);
  }

  @Override
  public boolean transaction(TransactionScope txBlock, boolean joinCurrentTx, int attempts, OkCallback ok, ErrorCallback error) {
    return wrapped.transaction(txBlock, joinCurrentTx, attempts, ok, error);
  }

  public RecordFactory getRecordFactory() {
    return wrapped.getRecordFactory();
  }

  @Override
  public Schema getSchema() {
    return wrapped.getSchema();
  }

  public BinarySerializer getSerializer() {
    return wrapped.getSerializer();
  }

  public PageManager getPageManager() {
    return wrapped.getPageManager();
  }

  @Override
  public MutableDocument newDocument(String typeName) {
    return wrapped.newDocument(typeName);
  }

  public MutableEmbeddedDocument newEmbeddedDocument(EmbeddedModifier modifier, String typeName) {
    return wrapped.newEmbeddedDocument(modifier, typeName);
  }

  @Override
  public MutableVertex newVertex(String typeName) {
    return wrapped.newVertex(typeName);
  }

  @Override
  public Edge newEdgeByKeys(String sourceVertexType, String[] sourceVertexKeyNames, Object[] sourceVertexKeyValues, String destinationVertexType,
      String[] destinationVertexKeyNames, Object[] destinationVertexKeyValues, boolean createVertexIfNotExist, String edgeType, boolean bidirectional,
      Object... properties) {
    return wrapped.newEdgeByKeys(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues, destinationVertexType, destinationVertexKeyNames,
        destinationVertexKeyValues, createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public Edge newEdgeByKeys(Vertex sourceVertex, String destinationVertexType, String[] destinationVertexKeyNames, Object[] destinationVertexKeyValues,
      boolean createVertexIfNotExist, String edgeType, boolean bidirectional, Object... properties) {
    return wrapped.newEdgeByKeys(sourceVertex, destinationVertexType, destinationVertexKeyNames, destinationVertexKeyValues, createVertexIfNotExist, edgeType,
        bidirectional, properties);
  }

  @Override
  public boolean isAutoTransaction() {
    return wrapped.isAutoTransaction();
  }

  @Override
  public void setAutoTransaction(boolean autoTransaction) {
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
  public PaginatedFile.MODE getMode() {
    return wrapped.getMode();
  }

  @Override
  public boolean checkTransactionIsActive(boolean createTx) {
    return wrapped.checkTransactionIsActive(createTx);
  }

  public DocumentIndexer getIndexer() {
    return wrapped.getIndexer();
  }

  @Override
  public ResultSet command(String language, String query, Object... parameters) {
    return wrapped.command(language, query, parameters);
  }

  @Override
  public ResultSet command(String language, String query, Map<String, Object> parameters) {
    return wrapped.command(language, query, parameters);
  }

  @Override
  public ResultSet execute(String language, String script, Map<String, Object> params) {
    return wrapped.execute(language, script, params);
  }

  @Override
  public ResultSet execute(String language, String script, Object... args) {
    return wrapped.execute(language, script, args);
  }

  @Override
  public ResultSet query(String language, String query, Object... parameters) {
    return wrapped.query(language, query, parameters);
  }

  @Override
  public ResultSet query(String language, String query, Map<String, Object> parameters) {
    return wrapped.query(language, query, parameters);
  }

  @Override
  public boolean equals(Object o) {
    return wrapped.equals(o);
  }

  public DatabaseContext.DatabaseContextTL getContext() {
    return wrapped.getContext();
  }

  @Override
  public <RET> RET executeInReadLock(Callable<RET> callable) {
    return wrapped.executeInReadLock(callable);
  }

  @Override
  public <RET> RET executeInWriteLock(Callable<RET> callable) {
    return wrapped.executeInWriteLock(callable);
  }

  public <RET> RET recordFileChanges(Callable<Object> callback) {
    return wrapped.recordFileChanges(callback);
  }

  public StatementCache getStatementCache() {
    return wrapped.getStatementCache();
  }

  public ExecutionPlanCache getExecutionPlanCache() {
    return wrapped.getExecutionPlanCache();
  }

  public WALFileFactory getWALFileFactory() {
    return wrapped.getWALFileFactory();
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  public void executeCallbacks(DatabaseInternal.CALLBACK_EVENT event) throws IOException {
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
  public void setEdgeListSize(int size) {
    wrapped.setEdgeListSize(size);
  }

  public int getEdgeListSize(int previousSize) {
    return wrapped.getEdgeListSize(previousSize);
  }

  public Map<String, Object> getWrappers() {
    return wrapped.getWrappers();
  }

  public void setWrapper(String name, Object instance) {
    wrapped.setWrapper(name, instance);
  }
}
