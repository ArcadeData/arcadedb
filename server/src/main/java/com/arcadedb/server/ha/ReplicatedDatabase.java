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
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ha.message.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class ReplicatedDatabase implements DatabaseInternal {
  private final ArcadeDBServer server;
  private final LocalDatabase proxied;
  private final HAServer.QUORUM quorum;
  private final long timeout;

  public ReplicatedDatabase(final ArcadeDBServer server, final LocalDatabase proxied) {
    if (!server.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL))
      throw new ConfigurationException("Cannot use replicated database if transaction WAL is disabled");

    this.server = server;
    this.proxied = proxied;
    this.timeout = proxied.getConfiguration().getValueAsLong(GlobalConfiguration.HA_QUORUM_TIMEOUT);
    this.proxied.setWrappedDatabaseInstance(this);

    HAServer.QUORUM quorum;
    final String quorumValue = proxied.getConfiguration().getValueAsString(GlobalConfiguration.HA_QUORUM)
            .toUpperCase(Locale.ENGLISH);
    try {
      quorum = HAServer.QUORUM.valueOf(quorumValue);
    } catch (Exception e) {
      LogManager.instance()
              .log(this, Level.SEVERE, "Error on setting quorum to '%s' for database '%s'. Setting it to MAJORITY", e, quorumValue,
                      getName());
      quorum = HAServer.QUORUM.MAJORITY;
    }
    this.quorum = quorum;
  }

  @Override
  public void commit() {
    proxied.incrementStatsTxCommits();

    final boolean isLeader = isLeader();

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
              final TxForwardRequest command = new TxForwardRequest(ReplicatedDatabase.this, getTransactionIsolationLevel(),
                      tx.getBucketRecordDelta(), bufferChanges, tx.getIndexChanges().toMap());
              server.getHA().forwardCommandToLeader(command, timeout * 2);
              tx.reset();
            }
          } else {
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

  public void replicateTx(final TransactionContext tx, final TransactionContext.TransactionPhase1 phase1,
                          final Binary bufferChanges) {
    final int configuredServers = server.getHA().getConfiguredServers();

    final int reqQuorum = quorum.quorum(configuredServers);

    final TxRequest req = new TxRequest(getName(), tx.getBucketRecordDelta(), bufferChanges, reqQuorum > 1);

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
  public int getNewEdgeListSize(final int previousSize) {
    return proxied.getNewEdgeListSize(previousSize);
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
      final QueryEngine queryEngine = proxied.getQueryEngineManager().getInstance(language, this);
      if (queryEngine.isExecutedByTheLeader() || queryEngine.analyze(query).isDDL()) {
        // USE A BIGGER TIMEOUT CONSIDERING THE DOUBLE LATENCY
        final CommandForwardRequest command = new CommandForwardRequest(ReplicatedDatabase.this, language, query, null, args);
        return (ResultSet) server.getHA().forwardCommandToLeader(command, timeout * 2);
      }
      return proxied.command(language, query, configuration, args);
    }

    return proxied.command(language, query, configuration, args);
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
      final QueryEngine queryEngine = proxied.getQueryEngineManager().getInstance(language, this);
      if (queryEngine.isExecutedByTheLeader() || queryEngine.analyze(query).isDDL()) {
        // USE A BIGGER TIMEOUT CONSIDERING THE DOUBLE LATENCY
        final CommandForwardRequest command = new CommandForwardRequest(ReplicatedDatabase.this, language, query, args, null);
        return (ResultSet) server.getHA().forwardCommandToLeader(command, timeout * 2);
      }
    }

    return proxied.command(language, query, configuration, args);
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
    final HAServer ha = server.getHA();

    final AtomicReference<Object> result = new AtomicReference<>();

    // ACQUIRE A DATABASE WRITE LOCK. THE LOCK IS REENTRANT, SO THE ACQUISITION DOWN THE LINE IS GOING TO PASS BECAUSE ALREADY ACQUIRED HERE
    final AtomicReference<DatabaseChangeStructureRequest> command = new AtomicReference<>();

    try {
      proxied.executeInWriteLock(() -> {
        if (!ha.isLeader()) {
          // NOT THE LEADER: NOT RESPONSIBLE TO SEND CHANGES TO OTHER SERVERS
          // TODO: Issue #118SchemaException
          throw new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server",
                  ha.getLeaderName());
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

          return null;

        } finally {
          // EVEN IN CASE OF EXCEPTION PROPAGATE THE CHANGE OF STRUCTURE IF ANY.
          // THIS IS TYPICAL ON INDEX CREATION THAT FAIL (DUPLICATED KEYS)
          command.set(getChangeStructure(schemaVersionBefore));
          proxied.getFileManager().stopRecordingChanges();
        }
      });

    } finally {
      if (command.get() != null) {
        // SEND THE COMMAND OUTSIDE THE EXCLUSIVE LOCK
        final int quorum = ha.getConfiguredServers();
        ha.sendCommandToReplicasWithQuorum(command.get(), quorum, timeout);
      }
    }

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

  public HAServer.QUORUM getQuorum() {
    return quorum;
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
      proxied.getPageManager().suspendFlushAndExecute(this, () -> {
        final List<ComponentFile> files = proxied.getFileManager().getFiles();

        for (final ComponentFile file : files)
          if (file != null) {
            final long fileChecksum = file.calculateChecksum();
            fileChecksums.put(file.getFileId(), fileChecksum);
            fileSizes.put(file.getFileId(), file.getSize());
          }

        final DatabaseAlignRequest request = new DatabaseAlignRequest(getName(), getSchema().getEmbedded().toJSON().toString(),
                fileChecksums, fileSizes);
        final List<Object> responsePayloads = ha.sendCommandToReplicasWithQuorum(request, quorum, 120_000);

        if (responsePayloads != null) {
          for (final Object o : responsePayloads) {
            final DatabaseAlignResponse response = (DatabaseAlignResponse) o;
            result.put(response.getRemoteServerName(), response.getAlignedPages());
          }
        }
      });

      return null;
    });

    return result;
  }

  /**
   * Creates the new database to all the replicas by executing a full sync backup of the database.
   */
  public void createInReplicas() {
    final HAServer ha = server.getHA();
    if (!ha.isLeader())
      // NOT THE LEADER
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", ha.getLeaderName());

    final int quorum = ha.getConfiguredServers();
    if (quorum == 1)
      // NO ACTIVE NODES
      return;

    final InstallDatabaseRequest request = new InstallDatabaseRequest(getName());
    ha.sendCommandToReplicasWithQuorum(request, quorum, 30_000);
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

  private boolean isLeader() {
    return server.getHA() != null && server.getHA().isLeader();
  }
}
