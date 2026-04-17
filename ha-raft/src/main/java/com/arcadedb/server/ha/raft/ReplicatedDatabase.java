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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.WALFile;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.exception.ConcurrentModificationException;
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
import com.arcadedb.server.HAPlugin;
import com.arcadedb.server.ReadConsistencyContext;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Level;

public class ReplicatedDatabase implements DatabaseInternal {
  /**
   * Test-only fault-injection hook. When non-null, invoked on the leader AFTER
   * {@code replicateTransaction()} succeeds and BEFORE {@code commit2ndPhase()} runs.
   * A hook that throws simulates a leader crash in the narrow window where the Raft
   * entry is durably committed (and followers applied it) but the leader did not yet
   * write pages locally. Always {@code null} in production; set only from integration
   * tests that need to exercise this crash window.
   */
  static volatile Consumer<String> TEST_POST_REPLICATION_HOOK = null;

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

  /**
   * Commits the current transaction with Raft replication.
   *
   * <p>For {@link Quorum#MAJORITY}, the transaction is committed once a majority of peers acknowledge.
   * For {@link Quorum#ALL}, an additional Watch(ALL_COMMITTED) is issued after the majority ack.
   *
   * <p><strong>ALL-quorum contract:</strong> success means all nodes confirmed the entry. If the ALL
   * watch fails (leader step-down, follower stall, timeout), a {@link MajorityCommittedAllFailedException}
   * is thrown. In this case the entry is majority-committed and durable - the leader still applies
   * locally via {@code commit2ndPhase()} to prevent divergence. The caller receives the exception
   * but the transaction will eventually be visible on all nodes.
   *
   * @throws MajorityCommittedAllFailedException if ALL quorum watch failed after majority commit
   * @throws com.arcadedb.network.binary.QuorumNotReachedException if majority quorum was not reached (rollback occurred)
   * @throws TransactionException on phase 1 or phase 2 commit failure
   */
  @Override
  public void commit() {
    final boolean leader = isLeader();
    HALog.log(this, HALog.TRACE, "commit() called: db=%s, isLeader=%s", getName(), leader);

    // PHASE 1 (under read lock): prepare the transaction, capture all data needed for replication.
    // After this phase, all WAL bytes, delta, and schema changes are in local variables.
    final ReplicationPayload payload = proxied.executeInReadLock(() -> {
      proxied.checkTransactionIsActive(false);

      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      final TransactionContext tx = current.getLastTransaction();

      try {
        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(leader);

        if (phase1 != null) {
          proxied.incrementStatsWriteTx();

          if (!leader) {
            tx.reset();
            throw new ServerIsNotTheLeaderException("Write operations must be executed on the leader server",
                getLeaderHTTPAddress());
          }

          return captureReplicationPayload(tx, phase1);
        } else {
          proxied.incrementStatsReadTx();
          tx.reset();
          return null;
        }
      } catch (final NeedRetryException | TransactionException e) {
        rollback();
        throw e;
      } catch (final Exception e) {
        rollback();
        throw new TransactionException("Error on commit distributed transaction (phase 1)", e);
      }
    });

    // Read-only transaction or follower rejection: nothing more to do.
    if (payload == null)
      return;

    // REPLICATION (no lock held): send WAL to Ratis and wait for quorum.
    // No database lock is needed - we only send captured bytes over gRPC.
    //
    // Three outcomes:
    //  - Success: MAJORITY (or ALL) ack'd. Ratis called applyTransaction() on the leader's
    //    state machine (origin-skip fired), then returned the client reply. Proceed to phase 2.
    //  - MajorityCommittedAllFailedException: MAJORITY committed (applyTransaction fired with
    //    origin-skip), but ALL watch failed. Must still call commit2ndPhase() to write pages
    //    locally - otherwise lastAppliedIndex advanced but database is stale. Re-throw so the
    //    client knows ALL quorum was not reached.
    //  - Any other exception: entry was NOT committed by Raft; phase 2 must not run. Rollback.
    //
    // Safety: server.getHA() is guaranteed non-null here because ReplicatedDatabase is only created
    // when HA is enabled, and the RaftHAPlugin instance is set before databases are loaded.
    // The isLeader() check above already verified that the Raft server is started and this node
    // is the leader - if not, we would have thrown ServerIsNotTheLeaderException in phase 1.
    try {
      final RaftHAServer raftServer = ((RaftHAPlugin) server.getHA()).getRaftServer();
      HALog.log(this, HALog.DETAILED, "Replicating WAL via Ratis: db=%s, walSize=%d, deltaSize=%d, schema=%s",
          getName(), payload.bufferChanges.size(), payload.delta.size(), payload.schemaJson != null);
      raftServer.replicateTransaction(getName(), payload.delta, payload.bufferChanges,
          payload.schemaJson, payload.filesToAdd, payload.filesToRemove);
      HALog.log(this, HALog.TRACE, "WAL replication completed: db=%s", getName());
    } catch (final MajorityCommittedAllFailedException e) {
      // MAJORITY quorum committed the entry - Ratis already called applyTransaction() on this
      // leader with the origin-skip. We must apply locally to prevent permanent divergence
      // between lastAppliedIndex and the actual database state. Re-throw after applying so the
      // client receives the ALL-quorum failure and can decide whether to retry.
      HALog.log(this, HALog.BASIC,
          "ALL quorum watch failed after MAJORITY commit; applying locally to prevent leader divergence: db=%s", getName());
      applyLocallyAfterMajorityCommit(payload);
      throw e;
    } catch (final NeedRetryException | TransactionException e) {
      rollback();
      throw e;
    } catch (final Exception e) {
      rollback();
      throw new TransactionException("Error on commit distributed transaction (replication)", e);
    }

    // Test-only fault-injection point: replication succeeded, phase 2 not yet run.
    // Null-checked to keep the production hot path branch-free on the common case.
    final Consumer<String> postReplicationHook = TEST_POST_REPLICATION_HOOK;
    if (postReplicationHook != null)
      postReplicationHook.accept(getName());

    // PHASE 2 (under read lock): quorum reached, commit locally.
    // If this fails, followers have already applied the changes but the leader has not.
    // Rollback cannot undo replicated changes, so we log the inconsistency for diagnosis.
    proxied.executeInReadLock(() -> {
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      try {
        payload.tx.commit2ndPhase(payload.phase1);

        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
      } catch (final Exception e) {
        if (e instanceof ConcurrentModificationException)
          LogManager.instance().log(this, Level.SEVERE,
              "Phase 2 commit failed AFTER successful Raft replication with a page version conflict (db=%s, txId=%s). "
                  + "A page was concurrently modified under file lock - this may indicate a locking bug. "
                  + "Followers have applied this transaction but the leader has not. "
                  + "Stepping down to prevent stale reads. Error: %s",
              getName(), payload.tx, e.getMessage());
        else
          LogManager.instance().log(this, Level.SEVERE,
              "Phase 2 commit failed AFTER successful Raft replication (db=%s, txId=%s). "
                  + "Followers have applied this transaction but the leader has not. "
                  + "Stepping down to prevent stale reads. Error: %s",
              getName(), payload.tx, e.getMessage());
        // Step down so a follower with correct state takes over. This node will self-heal on
        // restart via Raft log replay. The stop-server fallback lives in recoverLeadershipAfterPhase2Failure,
        // runs on a background thread (executeInReadLock is still active here), and is gated by
        // HA_STOP_SERVER_ON_REPLICATION_FAILURE so a single transient CME cannot kill the JVM.
        recoverLeadershipAfterPhase2Failure(payload.tx.toString());
        throw e;
      } finally {
        current.popIfNotLastTransaction();
      }
      return null;
    });
  }

  /**
   * Background supervisor for post-replication phase-2 failures. Called while the caller still
   * holds {@code executeInReadLock}, so all potentially-blocking work (step-down retries, optional
   * server stop) runs on a dedicated thread to avoid deadlocking against the database write lock
   * that {@link ArcadeDBServer#stop()} acquires.
   * <p>
   * Recovery policy:
   * <ol>
   *   <li>Attempt {@code stepDown()} up to {@value #STEP_DOWN_MAX_ATTEMPTS} times with a
   *       {@value #STEP_DOWN_RETRY_DELAY_MS} ms delay between tries. Raft will elect a follower
   *       as new leader; when this node rejoins, log replay reconciles its divergent state.</li>
   *   <li>If every attempt fails, log CRITICAL. The server stays up by default so an operator
   *       can inspect it - killing the JVM on a transient {@link ConcurrentModificationException}
   *       (or any other step-down failure) is disproportionate and was an overly aggressive
   *       default in earlier versions.</li>
   *   <li>Operators who want fail-stop semantics (e.g., Kubernetes with a CrashLoopBackOff
   *       policy that leans on log replay) can opt in via
   *       {@link GlobalConfiguration#HA_STOP_SERVER_ON_REPLICATION_FAILURE}, which schedules
   *       {@code server.stop()} after the retries are exhausted.</li>
   * </ol>
   */
  private void recoverLeadershipAfterPhase2Failure(final String txId) {
    recoverLeadershipAfterPhase2Failure(server, getName(), txId);
  }

  /**
   * Package-private static variant. Exposed for unit testing so the phase-2 recovery policy
   * (step-down retries, optional server stop) can be exercised without standing up a full
   * {@link ReplicatedDatabase} around a real {@link LocalDatabase}. Returns the recovery thread
   * so callers can join on it; production callers ignore the return value.
   */
  static Thread recoverLeadershipAfterPhase2Failure(final ArcadeDBServer server, final String databaseName,
      final String txId) {
    final Thread recoveryThread = new Thread(() -> {
      boolean steppedDown = false;
      Exception lastError = null;
      for (int attempt = 1; attempt <= STEP_DOWN_MAX_ATTEMPTS; attempt++) {
        try {
          final HAPlugin raftHA = server.getHA();
          if (raftHA == null || !raftHA.isLeader()) {
            steppedDown = true; // Nothing to do - we are no longer the leader.
            break;
          }
          raftHA.stepDown();
          steppedDown = true;
          break;
        } catch (final Exception stepDownEx) {
          lastError = stepDownEx;
          LogManager.instance().log(ReplicatedDatabase.class, Level.WARNING,
              "Step-down attempt %d/%d failed after phase 2 failure (db=%s, txId=%s): %s",
              attempt, STEP_DOWN_MAX_ATTEMPTS, databaseName, txId, stepDownEx.getMessage());
          if (attempt < STEP_DOWN_MAX_ATTEMPTS) {
            try {
              Thread.sleep(STEP_DOWN_RETRY_DELAY_MS);
            } catch (final InterruptedException ie) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }
      }

      if (steppedDown) {
        LogManager.instance().log(ReplicatedDatabase.class, Level.INFO,
            "Stepped down after phase 2 failure; Raft log replay will reconcile this node on next leadership change (db=%s, txId=%s)",
            databaseName, txId);
        return;
      }

      final boolean stopOnFailure = server.getConfiguration()
          .getValueAsBoolean(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE);
      if (!stopOnFailure) {
        LogManager.instance().log(ReplicatedDatabase.class, Level.SEVERE,
            "CRITICAL: Failed to step down after %d attempts following phase 2 failure (db=%s, txId=%s). "
                + "This leader has diverged from the Raft quorum and may serve stale reads until another "
                + "election fires. Manual intervention required (restart this node or set %s=true to "
                + "automate restart). Last error: %s",
            STEP_DOWN_MAX_ATTEMPTS, databaseName, txId,
            GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE.getKey(),
            lastError != null ? lastError.getMessage() : "unknown");
        return;
      }

      LogManager.instance().log(ReplicatedDatabase.class, Level.SEVERE,
          "Stop-server-on-replication-failure is enabled and all %d step-down attempts failed; stopping the server "
              + "so Raft log replay corrects this node's state on restart (db=%s, txId=%s)",
          STEP_DOWN_MAX_ATTEMPTS, databaseName, txId);
      try {
        server.stop();
      } catch (final Throwable t) {
        LogManager.instance().log(ReplicatedDatabase.class, Level.SEVERE,
            "Server stop also failed (db=%s): %s. Manual intervention required.",
            databaseName, t.getMessage());
      }
    }, "arcadedb-replication-recovery");
    recoveryThread.setDaemon(true);
    recoveryThread.start();
    return recoveryThread;
  }

  private static final int  STEP_DOWN_MAX_ATTEMPTS   = 3;
  private static final long STEP_DOWN_RETRY_DELAY_MS = 250L;

  /**
   * Applies the transaction locally after MAJORITY quorum was committed but the ALL watch failed.
   * Ratis already called {@code applyTransaction()} with the origin-skip on this leader, so
   * {@code lastAppliedIndex} was advanced but the page writes never happened. Without this call,
   * the leader's database permanently diverges from its own Raft log.
   */
  private void applyLocallyAfterMajorityCommit(final ReplicationPayload payload) {
    proxied.executeInReadLock(() -> {
      final DatabaseContext.DatabaseContextTL current = DatabaseContext.INSTANCE.getContext(proxied.getDatabasePath());
      try {
        payload.tx.commit2ndPhase(payload.phase1);
        if (getSchema().getEmbedded().isDirty())
          getSchema().getEmbedded().saveConfiguration();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Phase 2 commit failed during ALL-quorum recovery (db=%s, txId=%s). "
                + "Leader database may be inconsistent. Stepping down so a node with correct state takes over. Error: %s",
            getName(), payload.tx, e.getMessage());
        recoverLeadershipAfterPhase2Failure(payload.tx.toString());
      } finally {
        current.popIfNotLastTransaction();
      }
      return null;
    });
  }

  /** Holds schema/file structure change information for replication via Ratis. */
  private record ChangeStructure(String schemaJson, Map<Integer, String> filesToAdd, Map<Integer, String> filesToRemove) {
  }

  /** Holds all data captured in phase 1 needed for replication and local commit. */
  private record ReplicationPayload(TransactionContext tx, TransactionContext.TransactionPhase1 phase1,
      Binary bufferChanges, Map<Integer, Integer> delta, String schemaJson,
      Map<Integer, String> filesToAdd, Map<Integer, String> filesToRemove) {
  }

  /**
   * Captures everything needed for replication from the current transaction state.
   * Called under read lock during phase 1. All returned data is immutable/captured - safe to use
   * after releasing the lock.
   */
  private ReplicationPayload captureReplicationPayload(final TransactionContext tx,
      final TransactionContext.TransactionPhase1 phase1) {
    final Binary bufferChanges = phase1.result;

    String schemaJson = null;
    Map<Integer, String> filesToAdd = null;
    Map<Integer, String> filesToRemove = null;

    final ChangeStructure changeStructure = getChangeStructure(-1);
    if (changeStructure != null) {
      proxied.getFileManager().stopRecordingChanges();
      proxied.getFileManager().startRecordingChanges();

      schemaJson = changeStructure.schemaJson();
      filesToAdd = changeStructure.filesToAdd();
      filesToRemove = changeStructure.filesToRemove();
    }

    final Map<Integer, Integer> delta = tx.getBucketRecordDelta();
    HALog.log(this, HALog.TRACE, "Captured replication payload: delta=%s", delta);

    return new ReplicationPayload(tx, phase1, bufferChanges, delta, schemaJson, filesToAdd, filesToRemove);
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
        throw new ServerIsNotTheLeaderException("Write commands must be executed on the leader server",
            getLeaderHTTPAddress());
      waitForReadConsistency();
      return proxied.command(language, query, configuration, args);
    }
    waitForReadConsistency();
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
            getLeaderHTTPAddress());
      waitForReadConsistency();
    } else {
      waitForReadConsistency();
    }
    return proxied.command(language, query, configuration, args);
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
   * Waits for read consistency before executing a read.
   * <p>
   * Behavior depends on the configured consistency level:
   * <ul>
   *   <li><b>EVENTUAL</b>: No waiting. Reads from local state (fastest, may be stale on followers).</li>
   *   <li><b>READ_YOUR_WRITES</b>: Followers wait until they've applied the client's last write (bookmark).
   *       Leader waits for lastAppliedIndex >= commitIndex (fast no-op in steady state).</li>
   *   <li><b>LINEARIZABLE</b>:
   *     <ul>
   *       <li><i>On the leader</i>: verifies it still holds the Raft lease via {@code sendReadOnly()}
   *           (Raft paper Section 6.4). If the lease is valid, returns immediately (no round-trip).
   *           If the lease expired (e.g., after SIGSTOP/SIGCONT), sends heartbeats to a majority
   *           before serving the read. This path is strictly linearizable.</li>
   *       <li><i>On a follower with a bookmark</i>: waits for the follower's local apply to reach
   *           {@code ctx.readAfterIndex}. This is linearizable with respect to the caller's own
   *           prior writes (since the bookmark names a committed index) but is NOT globally
   *           linearizable across other clients' concurrent writes.</li>
   *       <li><i>On a follower without a bookmark</i>: issues a Ratis ReadIndex RPC to the
   *           leader (which verifies it still holds a quorum) and waits for the local state
   *           machine to catch up to the returned read index. This is globally linearizable:
   *           any read served afterwards reflects every write committed before the call. Cost
   *           is one follower-to-leader RTT plus apply-lag catch-up; concurrent ReadIndex
   *           calls are amortized by Ratis onto a single leader heartbeat.</li>
   *     </ul>
   *   </li>
   * </ul>
   * The consistency level comes from the per-request HTTP header ({@code X-ArcadeDB-Read-Consistency})
   * or the global config ({@code arcadedb.ha.readConsistency}).
   */
  private void waitForReadConsistency() {
    final HAPlugin raftHA = server.getHA();
    if (raftHA == null)
      return;
    applyReadConsistencyBarrier(raftHA, ReadConsistencyContext.get(), isLeader());
  }

  /**
   * Package-private for direct unit testing of the consistency-to-barrier mapping without
   * spinning up a full server. Exactly one barrier method (or none, for EVENTUAL) is invoked
   * per call.
   */
  static void applyReadConsistencyBarrier(final HAPlugin raftHA, final ReadConsistencyContext ctx,
      final boolean isLeader) {
    final Database.READ_CONSISTENCY consistency = ctx != null ? ctx.consistency : null;

    if (isLeader) {
      if (consistency == Database.READ_CONSISTENCY.LINEARIZABLE) {
        // Full Raft read protocol: verify lease or send heartbeats to majority.
        // This guarantees linearizability even after SIGSTOP/SIGCONT.
        raftHA.ensureLinearizableRead();
      } else {
        // Default leader barrier: wait for lastAppliedIndex >= commitIndex.
        // Handles normal leadership transitions but not SIGSTOP (deposed leader).
        raftHA.waitForLocalApply();
      }
      return;
    }

    // Follower reads
    if (consistency == null || consistency == Database.READ_CONSISTENCY.EVENTUAL)
      return;

    if (consistency == Database.READ_CONSISTENCY.READ_YOUR_WRITES) {
      if (ctx.readAfterIndex >= 0)
        raftHA.waitForAppliedIndex(ctx.readAfterIndex);
      else
        // No bookmark: ensure the follower has applied all committed entries before serving the read.
        // Without this, a catching-up follower would silently degrade to EVENTUAL consistency.
        raftHA.waitForLocalApply();
    } else if (consistency == Database.READ_CONSISTENCY.LINEARIZABLE) {
      if (ctx.readAfterIndex >= 0)
        // A bookmark already names the committed index the reader wants to observe, so the
        // cheaper local-apply wait suffices. This gives read-your-writes relative to the caller
        // (linearizability for the caller's own sequence of operations) without a leader RTT.
        raftHA.waitForAppliedIndex(ctx.readAfterIndex);
      else
        // No bookmark: issue a ReadIndex RPC to the leader to learn the current global commit
        // index, then wait for local apply to reach it. This is what makes LINEARIZABLE honest
        // on a follower - without the round-trip the follower could serve data older than some
        // other client's already-committed write.
        raftHA.ensureLinearizableFollowerRead();
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
    final HAPlugin raftHA = server.getHA();

    final AtomicReference<Object> result = new AtomicReference<>();
    final AtomicReference<ChangeStructure> replicationCommand = new AtomicReference<>();
    final AtomicReference<RuntimeException> callbackException = new AtomicReference<>();

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
      } catch (final RuntimeException e) {
        // Capture the exception but don't rethrow yet - we need to send the replication
        // command first. Multi-bucket index creation may have already replicated partial
        // file additions (via intermediate commits). The finally block captures file
        // removals that must be sent to followers to prevent orphan files.
        callbackException.set(e);
      } finally {
        replicationCommand.set(getChangeStructure(schemaVersionBefore));
        proxied.getFileManager().stopRecordingChanges();
      }
      return null;
    });

    // SEND THE REPLICATION COMMAND OUTSIDE THE WRITE LOCK to avoid blocking all readers/writers
    // during the Raft quorum round-trip (network I/O).
    // This runs even when the callback failed - cleanup commands (file removals) must reach
    // followers to prevent orphan files from partially-replicated multi-step schema changes.
    final ChangeStructure command = replicationCommand.get();
    if (command != null)
      ((RaftHAPlugin) raftHA).getRaftServer().replicateTransaction(getName(), Map.of(), new Binary(0), command.schemaJson(),
          command.filesToAdd(), command.filesToRemove());

    if (callbackException.get() != null)
      throw callbackException.get();

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

  public Quorum getQuorum() {
    final HAPlugin ha = server.getHA();
    return ha instanceof RaftHAPlugin p ? p.getQuorum() : Quorum.MAJORITY;
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

  protected ChangeStructure getChangeStructure(final long schemaVersionBefore) {
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

    return new ChangeStructure(serializedSchema, addFiles, removeFiles);
  }

  protected boolean isLeader() {
    final HAPlugin raftHA = server.getHA();
    return raftHA != null && raftHA.isLeader();
  }

  private String getLeaderHTTPAddress() {
    final HAPlugin raftHA = server.getHA();
    return raftHA != null ? raftHA.getLeaderHTTPAddress() : null;
  }
}
