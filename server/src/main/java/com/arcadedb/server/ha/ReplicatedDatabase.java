/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server.ha;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.*;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.database.async.OkCallback;
import com.arcadedb.engine.*;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.message.TxForwardRequest;
import com.arcadedb.server.ha.message.TxRequest;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class ReplicatedDatabase implements DatabaseInternal {
  private final ArcadeDBServer   server;
  private final EmbeddedDatabase proxied;
  private       HAServer.QUORUM  quorum;
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

    proxied.executeInReadLock(new Callable<Object>() {
      @Override
      public Object call() {
        proxied.checkTransactionIsActive(false);

        final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
        final TransactionContext tx = current.getLastTransaction();
        try {

          final Pair<Binary, List<MutablePage>> changes = tx.commit1stPhase(isLeader);

          try {
            if (changes != null) {
              final Binary bufferChanges = changes.getFirst();

              if (isLeader)
                replicateTx(tx, changes, bufferChanges);
              else {
                // USE A BIGGER TIMEOUT CONSIDERING THE DOUBLE LATENCY
                final TxForwardRequest command = new TxForwardRequest(ReplicatedDatabase.this, bufferChanges, tx.getIndexChanges().toMap());
                server.getHA().forwardCommandToLeader(command, timeout * 2);
                tx.reset();
              }
            } else {
              tx.reset();
            }
          } catch (NeedRetryException e) {
            rollback();
            throw e;
          } catch (TransactionException e) {
            rollback();
            throw e;
          } catch (Exception e) {
            rollback();
            throw new TransactionException("Error on commit distributed transaction", e);
          }

        } finally {
          current.popIfNotLastTransaction();
        }

        return null;
      }
    });
  }

  public void replicateTx(final TransactionContext tx, final Pair<Binary, List<MutablePage>> changes, final Binary bufferChanges) {
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

    server.getHA().sendCommandToReplicasWithQuorum(new TxRequest(getName(), bufferChanges, reqQuorum > 1), reqQuorum, timeout);

    // COMMIT 2ND PHASE ONLY IF THE QUORUM HAS BEEN REACHED
    tx.commit2ndPhase(changes);
  }

  @Override
  public DatabaseInternal getWrappedDatabaseInstance() {
    return this;
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
  public void createRecordNoLock(final Record record, final String bucketName) {
    proxied.createRecordNoLock(record, bucketName);
  }

  @Override
  public void updateRecord(final Record record) {
    proxied.updateRecord(record);
  }

  @Override
  public void updateRecordNoLock(final Record record) {
    proxied.updateRecordNoLock(record);
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
  public int getEdgeListSize(int previousSize) {
    return proxied.getEdgeListSize(previousSize);
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
  public DatabaseAsyncExecutorImpl async() {
    return proxied.async();
  }

  @Override
  public String getDatabasePath() {
    return proxied.getDatabasePath();
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
  public void scanBucket(final String bucketName, final RecordCallback callback) {
    proxied.scanBucket(bucketName, callback);
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
  public Edge newEdgeByKeys(final String sourceVertexType, final String[] sourceVertexKeyNames, final Object[] sourceVertexKeyValues, final String destinationVertexType,
      final String[] destinationVertexKeyNames, final Object[] destinationVertexKeyValues, final boolean createVertexIfNotExist, final String edgeType,
      final boolean bidirectional, final Object... properties) {

    return proxied.newEdgeByKeys(sourceVertexType, sourceVertexKeyNames, sourceVertexKeyValues, destinationVertexType, destinationVertexKeyNames,
        destinationVertexKeyValues,
        createVertexIfNotExist, edgeType, bidirectional, properties);
  }

  @Override
  public Schema getSchema() {
    return proxied.getSchema();
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

  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final ReplicatedDatabase pDatabase = (ReplicatedDatabase) o;

    return getDatabasePath() != null ? getDatabasePath().equals(pDatabase.getDatabasePath()) : pDatabase.getDatabasePath() == null;
  }

  @Override
  public ResultSet command(final String language, final String query, final Object... args) {
    return proxied.command(language, query, args);
  }

  @Override
  public ResultSet command(final String language, final String query, final Map<String, Object> args) {
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
  public ResultSet execute(String language, String script, Map<Object, Object> args) {
    return proxied.execute(language, script, args);
  }

  @Override
  public <RET extends Object> RET executeInReadLock(final Callable<RET> callable) {
    return proxied.executeInReadLock(callable);
  }

  @Override
  public <RET extends Object> RET executeInWriteLock(final Callable<RET> callable) {
    return proxied.executeInWriteLock(callable);
  }

  @Override
  public boolean isReadYourWrites() {
    return proxied.isReadYourWrites();
  }

  @Override
  public void setReadYourWrites(final boolean value) {
    proxied.setReadYourWrites(value);
  }

  @Override
  public void setEdgeListSize(final int size) {
    proxied.setEdgeListSize(size);
  }

  @Override
  public boolean isOpen() {
    return proxied.isOpen();
  }

  @Override
  public String toString() {
    return proxied.toString();
  }
}
