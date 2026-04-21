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
import com.arcadedb.database.DataEncryption;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.EmbeddedModifier;
import com.arcadedb.database.LocalDatabase;
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
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.WALFileFactory;
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
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
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
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.HAServerPlugin;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
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
  ) {
  }

  // Thread-local buffers used to accumulate WAL data when commit() is called inside
  // a recordFileChanges() callback. The buffered entries are then embedded in the
  // SCHEMA_ENTRY so replicas receive them atomically with the file-creation step.
  private static final ThreadLocal<List<byte[]>>                schemaWalBuffer         = ThreadLocal.withInitial(ArrayList::new);
  private static final ThreadLocal<List<Map<Integer, Integer>>> schemaBucketDeltaBuffer = ThreadLocal.withInitial(ArrayList::new);
  // Set to TRUE only on the thread executing a recordFileChanges() DDL callback, so that
  // commit() knows to buffer WAL to schemaWalBuffer rather than replicate via Raft.
  // This MUST be thread-local: using the shared getRecordedChanges() != null check would
  // cause concurrent user-transaction threads to buffer their WAL and lose it, producing
  // WALVersionGapException on followers (version gap from unreplicated writes).
  private static final ThreadLocal<Boolean>                     isSchemaCommitThread    = new ThreadLocal<>();

  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  /**
   * Test-only fault-injection hook. Fires after Raft replication succeeds but BEFORE phase-2
   * commit runs. Set to a non-null Consumer to simulate leader crash in this narrow window.
   * Always null in production.
   */
  static volatile Consumer<String> TEST_POST_REPLICATION_HOOK = null;

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

  /**
   * Commits the current transaction through Raft consensus.
   * <p>
   * <b>Two-phase flow with lock release during replication:</b>
   * <ol>
   *   <li><b>Phase 1 (read lock held):</b> Capture WAL bytes and bucket record deltas
   *       via {@code commit1stPhase}. The read lock ensures a consistent snapshot of the
   *       transaction's page changes.</li>
   *   <li><b>Replication (no lock held):</b> Submit the WAL entry to Raft via
   *       {@link RaftTransactionBroker#replicateTransaction} and wait for quorum. Releasing the lock
   *       here allows concurrent transactions to proceed through Phase 1 while this
   *       transaction waits for Raft consensus, significantly improving throughput.</li>
   *   <li><b>Phase 2 (read lock held on leader):</b> Apply pages locally via
   *       {@code commit2ndPhase}. On replicas, the state machine applies the pages via
   *       {@link ArcadeStateMachine#applyTransaction}, so Phase 2 is skipped here.</li>
   * </ol>
   * <p>
   * <b>Phase 2 failure handling:</b> If local apply fails after Raft has committed the entry,
   * the entry is already in the log and other replicas will apply it. The leader logs SEVERE
   * and should step down rather than continue with diverged state.
   * <p>
   * <b>Schema WAL buffering:</b> When {@code commit()} is called from inside a
   * {@code recordFileChanges()} callback, the files being created do not yet exist on replicas.
   * Instead of sending a {@code TX_ENTRY} that would fail, the WAL data is buffered in
   * {@link #schemaWalBuffer} and embedded in the {@code SCHEMA_ENTRY} sent after the callback.
   */
  @Override
  public void commit() {
    proxied.incrementStatsWriteTx();

    final boolean leader = isLeader();

    // When commit() is called from inside a recordFileChanges() DDL callback on the leader,
    // the files being created do not yet exist on replicas. Sending a TX_ENTRY now would
    // fail on replicas because the target files are missing. Instead, buffer the WAL data
    // here and embed it in the SCHEMA_ENTRY that recordFileChanges() sends after the callback.
    // NOTE: we check the thread-local flag (not the shared getRecordedChanges() != null) so that
    // concurrent user transactions on OTHER threads are never affected by an active recording
    // session (e.g. compaction) and continue to replicate normally via TX_ENTRY.
    if (leader && Boolean.TRUE.equals(isSchemaCommitThread.get())) {
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
        if (leader && getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
        current.popIfNotLastTransaction();
        return null;
      } catch (final ArcadeDBException e) {
        rollback();
        throw e;
      } catch (final Exception e) {
        rollback();
        throw new TransactionException("Error on commit distributed transaction (phase 1)", e);
      }
    });

    // Read-only transaction: nothing more to do.
    if (payload == null)
      return;

    // --- REPLICATION (no lock held): send WAL to Raft and wait for quorum ---
    try {
      final RaftHAServer raft = requireRaftServer();
      raft.getTransactionBroker().replicateTransaction(getName(), payload.walData(), payload.bucketDeltas());
    } catch (final MajorityCommittedAllFailedException e) {
      // MAJORITY committed (applyTransaction fired with origin-skip, lastAppliedIndex advanced)
      // but ALL-quorum watch failed. We MUST apply locally to prevent permanent divergence.
      HALog.log(this, HALog.BASIC,
          "ALL quorum watch failed after MAJORITY commit; applying locally to prevent leader divergence: db=%s", getName());
      applyLocallyAfterMajorityCommit(payload);
      throw e;
    } catch (final ArcadeDBException e) {
      rollback();
      throw e;
    } catch (final Exception e) {
      rollback();
      throw new TransactionException("Error on commit distributed transaction (replication)", e);
    }

    // Test-only fault injection: simulate leader crash between replication and phase-2
    final Consumer<String> postReplicationHook = TEST_POST_REPLICATION_HOOK;
    if (postReplicationHook != null)
      postReplicationHook.accept(getName());

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
        if (e instanceof java.util.ConcurrentModificationException)
          LogManager.instance().log(this, Level.SEVERE,
              "Phase 2 commit failed AFTER successful Raft replication with a page version conflict (db=%s, txId=%s). "
                  + "A page was concurrently modified under file lock - this may indicate a locking bug. "
                  + "Followers have applied this transaction but the leader has not. "
                  + "Stepping down to prevent stale reads. Error: %s",
              getName(), payload.tx(), e.getMessage());
        else
          LogManager.instance().log(this, Level.SEVERE,
              "Phase 2 commit failed AFTER successful Raft replication (db=%s, txId=%s). "
                  + "Followers have applied this transaction but the leader has not. "
                  + "Stepping down to prevent stale reads. Error: %s",
              getName(), payload.tx(), e.getMessage());
        recoverLeadershipAfterPhase2Failure(payload.tx().toString());
        throw e;
      } finally {
        current.popIfNotLastTransaction();
      }
      return null;
    });
  }

  /**
   * Applies phase 2 locally when ALL-quorum watch fails after MAJORITY commit.
   * The Raft entry is durably committed (MAJORITY applied it, including origin-skip on the leader),
   * so we must write the local pages to prevent permanent divergence.
   */
  private void applyLocallyAfterMajorityCommit(final ReplicationPayload payload) {
    proxied.executeInReadLock(() -> {
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      try {
        payload.tx().commit2ndPhase(payload.phase1());
        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Phase 2 commit failed during ALL-quorum recovery (db=%s, txId=%s). "
                + "Leader database may be inconsistent. Stepping down so a node with correct state takes over. Error: %s",
            getName(), payload.tx(), e.getMessage());
        recoverLeadershipAfterPhase2Failure(payload.tx().toString());
      } finally {
        current.popIfNotLastTransaction();
      }
      return null;
    });
  }

  private static final int  STEP_DOWN_MAX_RETRIES    = 3;
  private static final long STEP_DOWN_RETRY_DELAY_MS = 500;

  private void recoverLeadershipAfterPhase2Failure(final String txDescription) {
    if (raftHAServer == null || !raftHAServer.isLeader())
      return;

    for (int attempt = 1; attempt <= STEP_DOWN_MAX_RETRIES; attempt++) {
      try {
        raftHAServer.stepDown();
        LogManager.instance().log(this, Level.WARNING,
            "Step-down succeeded on attempt %d/%d after phase-2 failure (db=%s, tx=%s)",
            attempt, STEP_DOWN_MAX_RETRIES, getName(), txDescription);
        return;
      } catch (final Exception stepDownEx) {
        LogManager.instance().log(this, Level.SEVERE,
            "Step-down attempt %d/%d failed after phase-2 failure (db=%s, tx=%s): %s",
            attempt, STEP_DOWN_MAX_RETRIES, getName(), txDescription, stepDownEx.getMessage());
        if (attempt < STEP_DOWN_MAX_RETRIES) {
          try {
            Thread.sleep(STEP_DOWN_RETRY_DELAY_MS);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }

    final boolean stopServer = server.getConfiguration()
        .getValueAsBoolean(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE);
    if (stopServer) {
      LogManager.instance().log(this, Level.SEVERE,
          "CRITICAL: All %d step-down attempts failed (db=%s, tx=%s). "
              + "Forcing server stop to prevent leader-follower divergence.",
          STEP_DOWN_MAX_RETRIES, getName(), txDescription);
      final Thread stopThread = new Thread(() -> {
        try {
          server.stop();
        } catch (final Throwable t) {
          LogManager.instance().log(this, Level.SEVERE,
              "Server stop also failed (db=%s). Manual intervention required: %s",
              getName(), t.getMessage());
        }
      }, "arcadedb-emergency-stop");
      stopThread.setDaemon(true);
      stopThread.start();
    } else {
      LogManager.instance().log(this, Level.SEVERE,
          "CRITICAL: All %d step-down attempts failed (db=%s, tx=%s). "
              + "HA_STOP_SERVER_ON_REPLICATION_FAILURE=false, server continues in degraded state.",
          STEP_DOWN_MAX_RETRIES, getName(), txDescription);
    }
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
    if (raftHAServer == null)
      return;

    final ReadConsistencyContext ctx = READ_CONSISTENCY_CONTEXT.get();
    if (ctx == null)
      return;

    final Database.READ_CONSISTENCY consistency = ctx.consistency();
    if (consistency == null || consistency == Database.READ_CONSISTENCY.EVENTUAL)
      return;

    if (consistency == Database.READ_CONSISTENCY.READ_YOUR_WRITES) {
      if (!isLeader() && ctx.readAfterIndex() >= 0)
        raftHAServer.waitForAppliedIndex(ctx.readAfterIndex());
    } else if (consistency == Database.READ_CONSISTENCY.LINEARIZABLE) {
      if (isLeader())
        raftHAServer.ensureLinearizableRead();
      else
        raftHAServer.ensureLinearizableFollowerRead();
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

    // Clear thread-local WAL buffers so any commits inside the callback are captured fresh,
    // and mark THIS thread as the schema-commit thread so commit() buffers rather than replicates.
    schemaWalBuffer.get().clear();
    schemaBucketDeltaBuffer.get().clear();
    isSchemaCommitThread.set(Boolean.TRUE);

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
        final RaftHAServer raft = requireRaftServer();
        raft.getTransactionBroker().replicateSchema(getName(), serializedSchema, addFiles, removeFiles, walEntries, bucketDeltas);
        HALog.log(this, HALog.DETAILED,
            "Schema changes replicated via Raft: addFiles=%d, removeFiles=%d, schemaChanged=%s, embeddedWalEntries=%d",
            addFiles.size(), removeFiles.size(), schemaChanged, walEntries.size());
      }

      return result;
    } finally {
      isSchemaCommitThread.remove();
      schemaWalBuffer.get().clear();
      schemaBucketDeltaBuffer.get().clear();
      proxied.getFileManager().stopRecordingChanges();
    }
  }

  @Override
  public boolean runWithCompactionReplication(final Callable<Boolean> compaction) throws IOException, InterruptedException {
    if (!isLeader()) {
      // Followers receive compacted state from the leader; running compaction independently
      // would create different file IDs and diverge from the leader's replication stream.
      return false;
    }

    if (!proxied.getFileManager().startRecordingChanges()) {
      // Already recording (nested schema change): run compaction locally only
      return invokeCompaction(compaction);
    }

    try {
      final boolean result = invokeCompaction(compaction);
      if (!result)
        return false;

      final List<FileManager.FileChange> changes = proxied.getFileManager().getRecordedChanges();
      if (changes == null)
        return result;

      final Map<Integer, String> addFiles = new HashMap<>();
      final Map<Integer, String> removeFiles = new HashMap<>();
      for (final FileManager.FileChange change : changes) {
        if (change.create)
          addFiles.put(change.fileId, change.fileName);
        else
          removeFiles.put(change.fileId, change.fileName);
      }

      if (addFiles.isEmpty() && removeFiles.isEmpty())
        return result;

      // Serialize all pages of each newly created compaction file as synthetic WAL.
      // txId=-1 on followers signals forceApply to bypass version-gap checks when writing
      // pages whose version exceeds 1 (which is common after repeated page updates in compaction).
      final List<byte[]> walEntries = new ArrayList<>();
      for (final int fileId : addFiles.keySet()) {
        final byte[] wal = serializeFilePagesAsWal(fileId);
        if (wal != null)
          walEntries.add(wal);
      }

      final String serializedSchema = proxied.getSchema().getEmbedded().toJSON().toString();
      requireRaftServer().getTransactionBroker()
          .replicateSchema(getName(), serializedSchema, addFiles, removeFiles, walEntries, Collections.emptyList());

      HALog.log(this, HALog.DETAILED,
          "Compaction for database '%s' replicated via Raft: addFiles=%d, removeFiles=%d, walEntries=%d",
          getName(), addFiles.size(), removeFiles.size(), walEntries.size());

      return result;
    } finally {
      proxied.getFileManager().stopRecordingChanges();
    }
  }

  private static boolean invokeCompaction(final Callable<Boolean> compaction) throws IOException, InterruptedException {
    try {
      return compaction.call();
    } catch (final IOException | InterruptedException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new IOException("Compaction failed", e);
    }
  }

  private byte[] serializeFilePagesAsWal(final int fileId) throws IOException {
    final PaginatedComponentFile file = (PaginatedComponentFile) proxied.getFileManager().getFile(fileId);
    final int pageSize = file.getPageSize();
    final int totalPages = (int) file.getTotalPages();

    if (totalPages == 0)
      return null;

    final int deltaSize = pageSize - BasePage.PAGE_HEADER_SIZE;
    // Per-page WAL record: fileId(4) + pageNum(4) + changesFrom(4) + changesTo(4) + version(4) + contentSize(4) + delta
    final int perPageWalSize = 6 * Integer.BYTES + deltaSize;
    final int segmentSize = totalPages * perPageWalSize;
    // Full buffer: txId(8) + timestamp(8) + pageCount(4) + segmentSize(4) + pages + segmentSize(4) + MAGIC(8)
    final int totalSize = 2 * Long.BYTES + 2 * Integer.BYTES + segmentSize + Integer.BYTES + Long.BYTES;

    final ByteBuffer walBuf = ByteBuffer.allocate(totalSize);
    walBuf.putLong(-1L); // txId=-1 → forceApply on followers
    walBuf.putLong(System.currentTimeMillis());
    walBuf.putInt(totalPages);
    walBuf.putInt(segmentSize);

    final ByteBuffer pageBuf = ByteBuffer.allocate(pageSize);
    for (int pageNum = 0; pageNum < totalPages; pageNum++) {
      file.readPage(pageNum, pageBuf);

      final int version = pageBuf.getInt(0);                 // PAGE_VERSION_OFFSET
      final int rawContentSize = pageBuf.getInt(Integer.BYTES); // PAGE_CONTENTSIZE_OFFSET (stored as full size)

      walBuf.putInt(fileId);
      walBuf.putInt(pageNum);
      walBuf.putInt(BasePage.PAGE_HEADER_SIZE);  // changesFrom = 8
      walBuf.putInt(pageSize - 1);               // changesTo
      walBuf.putInt(version);
      walBuf.putInt(rawContentSize - BasePage.PAGE_HEADER_SIZE); // contentSize in WAL = stored minus header

      walBuf.put(pageBuf.array(), BasePage.PAGE_HEADER_SIZE, deltaSize);
    }

    walBuf.putInt(segmentSize);
    walBuf.putLong(WALFile.MAGIC_NUMBER);

    return walBuf.array();
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
    try {
      final RaftHAServer raft = requireRaftServer();
      raft.getTransactionBroker().replicateInstallDatabase(getName(), false);
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending install-database entry via Raft for database '" + getName() + "'", e);
    }
    LogManager.instance().log(this, Level.INFO, "Database '%s' install-database entry committed via Raft", getName());
  }

  @Override
  public void createInReplicas(final boolean forceSnapshot) {
    try {
      final RaftHAServer raft = requireRaftServer();
      raft.getTransactionBroker().replicateInstallDatabase(getName(), forceSnapshot);
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending install-database entry via Raft for database '" + getName() + "'", e);
    }
    LogManager.instance()
        .log(this, Level.INFO, "Database '%s' install-database (forceSnapshot=%s) entry committed via Raft", getName(),
            forceSnapshot);
  }

  @Override
  public void dropInReplicas() {
    try {
      final RaftHAServer raft = requireRaftServer();
      raft.getTransactionBroker().replicateDropDatabase(getName());
    } catch (final TransactionException e) {
      throw e;
    } catch (final Exception e) {
      throw new TransactionException("Error sending drop-database entry via Raft for database '" + getName() + "'", e);
    }
    LogManager.instance().log(this, Level.INFO, "Database '%s' drop-database entry committed via Raft", getName());
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

    final String clusterToken = raftHAServer.getClusterToken();
    if (clusterToken != null && !clusterToken.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);

    final String proxiedUser = proxied.getCurrentUserName();
    if (proxiedUser == null || proxiedUser.isBlank())
      throw new SecurityException(
          "Cannot forward command to leader: no authenticated user in the current security context");
    builder.header("X-ArcadeDB-Forwarded-User", proxiedUser);

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
