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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.EmbeddedModifier;
import com.arcadedb.database.LocalTransactionExplicitLock;
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
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLMethod;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.BinarySerializer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodTransformTest {
  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodTransform();
  }

  @Test
  void nulIReturnedAsNull() {
    final Object result = method.execute(null, null, null, null);
    assertThat(result).isNull();
  }

  @Test
  void toLowerCase() {
    final BasicCommandContext context = getMockedContext();

    final Object result = method.execute(Set.of("A", "B"), null, context, new String[] { "toLowerCase" });
    assertThat(result).isInstanceOf(Set.class);
    assertThat(result).asInstanceOf(InstanceOfAssertFactories.SET).contains("a");
    assertThat(result).asInstanceOf(InstanceOfAssertFactories.SET).contains("b");

    final Object result2 = method.execute(List.of("A", "B"), null, context, new String[] { "toLowerCase" });
    assertThat(result2).isInstanceOf(List.class);
    assertThat(result2).asInstanceOf(InstanceOfAssertFactories.LIST).contains("a");
    assertThat(result2).asInstanceOf(InstanceOfAssertFactories.LIST).contains("b");
  }

  @Test
  void toUpperCase() {
    final BasicCommandContext context = getMockedContext();

    final Object result = method.execute(Set.of("A", "b"), null, context, new String[] { "toUpperCase" });
    assertThat(result).isInstanceOf(Set.class);
    assertThat(result).asInstanceOf(InstanceOfAssertFactories.SET).contains("A");
    assertThat(result).asInstanceOf(InstanceOfAssertFactories.SET).contains("B");

    final Object result2 = method.execute(List.of("a", "B"), null, context, new String[] { "toUpperCase" });
    assertThat(result2).isInstanceOf(List.class);
    assertThat(result2).asInstanceOf(InstanceOfAssertFactories.LIST).contains("A");
    assertThat(result2).asInstanceOf(InstanceOfAssertFactories.LIST).contains("B");
  }

  @Test
  void chain() {
    final BasicCommandContext context = getMockedContext();

    final Object result = method.execute(Set.of(" AA ", " bb "), null, context, new String[] { "trim", "toUpperCase" });
    assertThat(result).isInstanceOf(Set.class);
    assertThat(result).asInstanceOf(InstanceOfAssertFactories.SET).contains("AA");
    assertThat(result).asInstanceOf(InstanceOfAssertFactories.SET).contains("BB");

    final Object result2 = method.execute(List.of(" aa ", " BB "), null, context, new String[] { "trim", "toLowerCase" });
    assertThat(result2).isInstanceOf(List.class);
    assertThat(result2).asInstanceOf(InstanceOfAssertFactories.LIST).contains("aa");
    assertThat(result2).asInstanceOf(InstanceOfAssertFactories.LIST).contains("bb");
  }

  private static BasicCommandContext getMockedContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(new DatabaseInternal() {
      @Override
      public Record invokeAfterReadEvents(final Record record) {
        return record;
      }

      @Override
      public long getSize() {
        return 0L;
      }

      @Override
      public TransactionContext getTransactionIfExists() {
        return null;
      }

      @Override
      public MutableEmbeddedDocument newEmbeddedDocument(EmbeddedModifier modifier, String typeName) {
        return null;
      }

      @Override
      public DatabaseInternal getEmbedded() {
        return null;
      }

      @Override
      public DatabaseContext.DatabaseContextTL getContext() {
        return null;
      }

      @Override
      public FileManager getFileManager() {
        return null;
      }

      @Override
      public RecordFactory getRecordFactory() {
        return null;
      }

      @Override
      public BinarySerializer getSerializer() {
        return null;
      }

      @Override
      public PageManager getPageManager() {
        return null;
      }

      @Override
      public DatabaseInternal getWrappedDatabaseInstance() {
        return null;
      }

      @Override
      public Map<String, Object> getWrappers() {
        return null;
      }

      @Override
      public void setWrapper(String name, Object instance) {
      }

      @Override
      public void checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS access) {
      }

      @Override
      public void checkPermissionsOnFile(int fileId, SecurityDatabaseUser.ACCESS access) {
      }

      @Override
      public boolean checkTransactionIsActive(boolean createTx) {
        return false;
      }

      @Override
      public boolean isAsyncProcessing() {
        return false;
      }

      @Override
      public long getResultSetLimit() {
        return 0;
      }

      @Override
      public long getReadTimeout() {
        return 0;
      }

      @Override
      public void registerCallback(CALLBACK_EVENT event, Callable<Void> callback) {

      }

      @Override
      public void unregisterCallback(CALLBACK_EVENT event, Callable<Void> callback) {

      }

      @Override
      public void executeCallbacks(CALLBACK_EVENT event) throws IOException {

      }

      @Override
      public GraphEngine getGraphEngine() {
        return null;
      }

      @Override
      public TransactionManager getTransactionManager() {
        return null;
      }

      @Override
      public void createRecord(MutableDocument record) {

      }

      @Override
      public void createRecord(Record record, String bucketName) {

      }

      @Override
      public void createRecordNoLock(Record record, String bucketName, boolean discardRecordAfter) {

      }

      @Override
      public void updateRecord(Record record) {

      }

      @Override
      public void updateRecordNoLock(Record record, boolean discardRecordAfter) {

      }

      @Override
      public void deleteRecordNoLock(Record record) {

      }

      @Override
      public void kill() {

      }

      @Override
      public DocumentIndexer getIndexer() {
        return null;
      }

      @Override
      public WALFileFactory getWALFileFactory() {
        return null;
      }

      @Override
      public StatementCache getStatementCache() {
        return null;
      }

      @Override
      public ExecutionPlanCache getExecutionPlanCache() {
        return null;
      }

      @Override
      public CypherStatementCache getCypherStatementCache() {
        return null;
      }

      @Override
      public CypherPlanCache getCypherPlanCache() {
        return null;
      }

      @Override
      public int getNewEdgeListSize(int previousSize) {
        return 0;
      }

      @Override
      public <RET> RET recordFileChanges(Callable<Object> callback) {
        return null;
      }

      @Override
      public long getLastUpdatedOn() {
        return 0;
      }

      @Override
      public long getLastUsedOn() {
        return 0;
      }

      @Override
      public long getOpenedOn() {
        return 0;
      }

      @Override
      public void saveConfiguration() throws IOException {

      }

      @Override
      public Map<String, Object> alignToReplicas() {
        return null;
      }

      @Override
      public <RET> RET executeLockingFiles(Collection<Integer> fileIds, Callable<RET> callable) {
        return null;
      }

      @Override
      public ContextConfiguration getConfiguration() {
        return null;
      }

      @Override
      public ComponentFile.MODE getMode() {
        return null;
      }

      @Override
      public DatabaseAsyncExecutor async() {
        return null;
      }

      @Override
      public String getDatabasePath() {
        return null;
      }

      @Override
      public boolean isOpen() {
        return false;
      }

      @Override
      public String getCurrentUserName() {
        return null;
      }

      @Override
      public Select select() {
        return null;
      }

      @Override
      public ResultSet command(String language, String query, Map<String, Object> args) {
        return null;
      }

      @Override
      public ResultSet command(String language, String query, ContextConfiguration configuration, Map<String, Object> args) {
        return null;
      }

      @Override
      public ResultSet query(String language, String query, Map<String, Object> args) {
        return null;
      }

      @Override
      public ResultSet execute(String language, String script, Map<String, Object> args) {
        return null;
      }

      @Override
      public boolean isAutoTransaction() {
        return false;
      }

      @Override
      public void setAutoTransaction(boolean autoTransaction) {

      }

      @Override
      public void rollbackAllNested() {

      }

      @Override
      public void scanType(String typeName, boolean polymorphic, DocumentCallback callback) {

      }

      @Override
      public void scanType(String typeName, boolean polymorphic, DocumentCallback callback,
          ErrorRecordCallback errorRecordCallback) {

      }

      @Override
      public void scanBucket(String bucketName, RecordCallback callback) {

      }

      @Override
      public void scanBucket(String bucketName, RecordCallback callback, ErrorRecordCallback errorRecordCallback) {

      }

      @Override
      public IndexCursor lookupByKey(String type, String keyName, Object keyValue) {
        return null;
      }

      @Override
      public IndexCursor lookupByKey(String type, String[] keyNames, Object[] keyValues) {
        return null;
      }

      @Override
      public Iterator<Record> iterateType(String typeName, boolean polymorphic) {
        return null;
      }

      @Override
      public Iterator<Record> iterateBucket(String bucketName) {
        return null;
      }

      @Override
      public Edge newEdgeByKeys(String sourceVertexType, String[] sourceVertexKeyNames, Object[] sourceVertexKeyValues,
          String destinationVertexType,
          String[] destinationVertexKeyNames, Object[] destinationVertexKeyValues, boolean createVertexIfNotExist, String edgeType,
          boolean bidirectional,
          Object... properties) {
        return null;
      }

      @Override
      public Edge newEdgeByKeys(Vertex sourceVertex, String destinationVertexType, String[] destinationVertexKeyNames,
          Object[] destinationVertexKeyValues,
          boolean createVertexIfNotExist, String edgeType, boolean bidirectional, Object... properties) {
        return null;
      }

      @Override
      public QueryEngine getQueryEngine(String language) {
        return new SQLQueryEngine.SQLQueryEngineFactory().getInstance(null);
      }

      @Override
      public Schema getSchema() {
        return null;
      }

      @Override
      public RecordEvents getEvents() {
        return null;
      }

      @Override
      public <RET> RET executeInReadLock(Callable<RET> callable) {
        return null;
      }

      @Override
      public <RET> RET executeInWriteLock(Callable<RET> callable) {
        return null;
      }

      @Override
      public boolean isReadYourWrites() {
        return false;
      }

      @Override
      public Database setReadYourWrites(boolean value) {
        return null;
      }

      @Override
      public Database setTransactionIsolationLevel(TRANSACTION_ISOLATION_LEVEL level) {
        return null;
      }

      @Override
      public TRANSACTION_ISOLATION_LEVEL getTransactionIsolationLevel() {
        return null;
      }

      @Override
      public int getEdgeListSize() {
        return 0;
      }

      @Override
      public Database setEdgeListSize(int size) {
        return null;
      }

      @Override
      public Database setUseWAL(boolean useWAL) {
        return null;
      }

      @Override
      public Database setWALFlush(WALFile.FlushType flush) {
        return null;
      }

      @Override
      public boolean isAsyncFlush() {
        return false;
      }

      @Override
      public Database setAsyncFlush(boolean value) {
        return null;
      }

      @Override
      public String getName() {
        return null;
      }

      @Override
      public void close() {

      }

      @Override
      public void drop() {

      }

      @Override
      public MutableDocument newDocument(String typeName) {
        return null;
      }

      @Override
      public MutableVertex newVertex(String typeName) {
        return null;
      }

      @Override
      public boolean isTransactionActive() {
        return false;
      }

      @Override
      public int getNestedTransactions() {
        return 0;
      }

      @Override
      public LocalTransactionExplicitLock acquireLock() {
        return null;
      }

      @Override
      public void transaction(TransactionScope txBlock) {

      }

      @Override
      public boolean transaction(TransactionScope txBlock, boolean joinCurrentTx) {
        return false;
      }

      @Override
      public boolean transaction(TransactionScope txBlock, boolean joinCurrentTx, int retries) {
        return false;
      }

      @Override
      public boolean transaction(TransactionScope txBlock, boolean joinCurrentTx, int attempts, OkCallback ok,
          ErrorCallback error) {
        return false;
      }

      @Override
      public void begin() {

      }

      @Override
      public void begin(TRANSACTION_ISOLATION_LEVEL isolationLevel) {

      }

      @Override
      public void commit() {

      }

      @Override
      public void rollback() {

      }

      @Override
      public Record lookupByRID(RID rid, boolean loadContent) {
        return null;
      }

      @Override
      public boolean existsRecord(RID rid) {
        return false;
      }

      @Override
      public void deleteRecord(Record record) {

      }

      @Override
      public ResultSet command(String language, String query, ContextConfiguration configuration, Object... args) {
        return null;
      }

      @Override
      public ResultSet command(String language, String query, Object... args) {
        return null;
      }

      @Override
      public ResultSet query(String language, String query, Object... args) {
        return null;
      }

      @Override
      public ResultSet execute(String language, String script, Object... args) {
        return null;
      }

      @Override
      public long countType(String typeName, boolean polymorphic) {
        return 0;
      }

      @Override
      public long countBucket(String bucketName) {
        return 0;
      }

      @Override
      public Map<String, Object> getStats() {
        return null;
      }
    });
    return context;
  }
}
