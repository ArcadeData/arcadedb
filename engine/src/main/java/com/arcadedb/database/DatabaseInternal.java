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
package com.arcadedb.database;

import com.arcadedb.database.async.AsyncQuiesce;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.WALFileFactory;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.query.opencypher.optimizer.statistics.GraphStatisticsCache;
import com.arcadedb.query.opencypher.query.CypherPlanCache;
import com.arcadedb.query.opencypher.query.CypherStatementCache;
import com.arcadedb.query.sql.parser.ExecutionPlanCache;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Internal API, do not use as an end user.
 */
@ExcludeFromJacocoGeneratedReport
public interface DatabaseInternal extends Database {
  enum CALLBACK_EVENT {
    TX_AFTER_WAL_WRITE, DB_NOT_CLOSED, DB_AFTER_OPEN
  }

  default TransactionContext getTransaction() {
    final TransactionContext tx = getTransactionIfExists();
    if (tx == null)
      throw new TransactionException("Transaction not started on current thread");
    return tx;
  }

  long getSize();

  TransactionContext getTransactionIfExists();

  MutableEmbeddedDocument newEmbeddedDocument(EmbeddedModifier modifier, String typeName);

  DatabaseInternal getEmbedded();

  /**
   * Closes the database with drop semantics but without removing its files, leaving their deletion to the
   * caller. Only the embedded instance supports it: reach it through {@link #getEmbedded()}, as the wrappers
   * reject it the way they reject {@code drop()} and {@code close()}.
   */
  default void closeForDrop() {
    throw new UnsupportedOperationException("Only an embedded database instance can be closed for drop");
  }

  DatabaseContext.DatabaseContextTL getContext();

  FileManager getFileManager();

  RecordFactory getRecordFactory();

  BinarySerializer getSerializer();

  PageManager getPageManager();

  DatabaseInternal getWrappedDatabaseInstance();

  /**
   * Unwraps a database to its underlying instance (e.g., ServerDatabase → LocalDatabase).
   * Ensures consistent identity regardless of wrapper layers.
   */
  static Database unwrap(final Database database) {
    if (database instanceof DatabaseInternal) {
      final DatabaseInternal internal = ((DatabaseInternal) database).getWrappedDatabaseInstance();
      if (internal != null && internal != database)
        return unwrap(internal);
    }
    return database;
  }

  Map<String, Object> getWrappers();

  void setWrapper(final String name, final Object instance);

  void checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS access);

  void checkPermissionsOnFile(int fileId, SecurityDatabaseUser.ACCESS access);

  /**
   * Per-type equivalent of {@link #checkPermissionsOnFile(int, SecurityDatabaseUser.ACCESS)} for a type that owns
   * no bucket to check a file id against (a TimeSeries type: its data lives in its own engine, not a
   * {@code LocalBucket}).
   */
  void checkPermissionsOnType(String typeName, SecurityDatabaseUser.ACCESS access);

  boolean checkTransactionIsActive(boolean createTx);

  boolean isAsyncProcessing();

  /**
   * Barrier against the asynchronous executor: returns only once every task already submitted has run <b>and</b> the
   * batch transaction each worker keeps open across {@link com.arcadedb.GlobalConfiguration#ASYNC_TX_BATCH_SIZE} tasks
   * has been committed. Does nothing - and in particular does not start the executor - on a database that never used
   * it.
   * <p>
   * This is what a caller that needs to <i>read</i> what the async side wrote has to use. Polling
   * {@link #isAsyncProcessing()} instead is not equivalent, and that was the bug of issue #6281: the predicate answers
   * about TASKS, and the tasks of a batch are all long finished while their records are still uncommitted, so an index
   * built on the strength of a {@code false} there was built over a bucket that did not contain them yet.
   *
   * @throws com.arcadedb.exception.NeedRetryException when called from one of the executor's OWN worker threads - a
   *     command dispatched with {@code awaitResponse=false}, for instance. The wait cannot be satisfied from inside
   *     the thing it waits for, so it is refused rather than deadlocked on; run the command synchronously instead.
   */
  void waitForAsyncCompletion();

  /**
   * The barrier above <b>held open</b>: returns once every async worker has committed its batch <i>and parked</i>, and
   * keeps them parked until the returned handle is closed (issue #6303, item 2).
   * <p>
   * {@link #waitForAsyncCompletion()} answers about the past and nothing else - the instant it returns a worker may
   * take the next task and write a record. That is enough for a caller that only has to READ what the async side
   * wrote; it is not enough for one that SCANS while it builds something out of the scan, because a record written
   * during the scan is missed by it and, having been saved before the index existed, staged no entry for it either.
   * Every scan-based index build therefore holds this instead.
   * <p>
   * <b>The cost, because it is not small and callers hold this for their whole build.</b> While the quiescence is
   * held every worker of the database is parked, so a task queued behind the park - a plain
   * {@code async().createRecord(...)} from an unrelated caller - waits for the build, and once that worker's bounded
   * queue fills, the backpressure in {@code scheduleTask} reaches the SUBMITTING thread too. On a long build (a
   * sorted build that spills to disk, say) that is the database's whole asynchronous ingestion path stalled for the
   * duration. It is the right trade and not an oversight: the alternative is the silently incomplete index of
   * #6281, and releasing the workers between the index's registration and the scan does not help - a record written
   * in that window would then be indexed twice, once by its own staged operation and once by the scan. A build is
   * also not something to run on a database at peak write load, which this makes true rather than merely advisable.
   *
   * @return a handle to close when the scan is done. Never null; a database that never used the async API gets one
   *     that does nothing.
   *
   * @throws com.arcadedb.exception.NeedRetryException when called from one of the executor's own worker threads, or
   *     when a worker does not park in time.
   */
  AsyncQuiesce quiesceAsync();

  long getResultSetLimit();

  long getReadTimeout();

  void registerCallback(CALLBACK_EVENT event, Callable<Void> callback);

  void unregisterCallback(CALLBACK_EVENT event, Callable<Void> callback);

  void executeCallbacks(CALLBACK_EVENT event) throws IOException;

  GraphEngine getGraphEngine();

  TransactionManager getTransactionManager();

  void createRecord(MutableDocument record);

  void createRecord(Record record, String bucketName);

  void createRecordNoLock(Record record, String bucketName, boolean discardRecordAfter);

  /**
   * Emergency repair: recreates {@code record} at {@code position} in {@code bucket}, i.e. at the exact RID a
   * deleted record used to hold, so existing references to that RID stay valid. Behind {@code RESTORE
   * DOCUMENT/VERTEX/EDGE} and {@link GraphEngine#restoreVertexAt}.
   * <p>
   * {@link com.arcadedb.engine.LocalBucket#restoreRecordAtPosition} performs only the physical page write, exactly
   * like bucket-level create/delete. Everything a create folds on top of that page write - the transaction's cached
   * bucket record-count delta that {@code count(*)} reads (#6069), the transaction record cache, and the record's
   * index entries (#6120) - is the caller's job, and having each RESTORE call site do it by hand is how both of
   * those were missed. It lives here, next to {@code createRecordNoLock}, so there is one place to forget.
   * <p>
   * Follows {@code createRecordNoLock}'s transaction contract: it joins the caller's transaction, and on a database
   * with {@code setAutoTransaction(true)} it wraps itself in an implicit one rather than throwing - RESTORE must not
   * be the one statement that behaves differently from an INSERT for those callers. {@code record} must not already
   * be persistent.
   * <p>
   * Since #6127 the parity is complete: default values, {@code validate()} and the create events (database-level and
   * per-type) all apply, so a restored record is indistinguishable from one an INSERT at the same RID would have
   * produced. Restoring past a type's constraints used to be allowed on the grounds that an emergency repair must
   * never be blocked, but the record it produced could not be UPDATEd afterwards ({@code updateRecord} validates too)
   * and no consistency pass ever flagged it - {@code CHECK DATABASE} is structural and does not read schema
   * constraints. The one intentional difference from {@code createRecordNoLock}: a {@code beforeCreate} listener that
   * vetoes raises here instead of returning quietly, because a repair that reports success without writing the record
   * is the one outcome this call must never produce. Everything above runs inside the same read lock as the write, so
   * a listener - arbitrary user code - cannot run against a database an {@code executeInWriteLock} caller
   * ({@code drop()}, {@code close()}) is free to close underneath it, and the occupied-slot check comes first so that
   * the likeliest mistake with this call is not masked by the record's own constraints.
   *
   * @return the RID the record was restored at, always {@code bucket}'s file id at {@code position}
   *
   * @throws ValidationException        if {@code record} violates its type's constraints
   * @throws DatabaseOperationException if a {@code beforeCreate} listener vetoed the restore
   */
  RID restoreRecord(Record record, LocalBucket bucket, long position);

  void updateRecord(Record record);

  void updateRecordNoLock(Record record, boolean discardRecordAfter);

  void deleteRecordNoLock(Record record);

  /**
   * Deletes an edge record the way {@code edge.delete()} does - index cleanup, external values, delete events and
   * the graph disconnection - except that it does NOT disconnect the edge from the endpoint vertex named by
   * {@code skipEndpoint}.
   * <p>
   * #5760: for {@code GraphEngine.deleteVertex}, which passes the vertex it is deleting. That vertex's edge lists
   * are dropped in their entirety a moment later, so removing each edge from them one at a time - a chain walk, a
   * chunk anchor, a compaction and a write-back per edge - is pure waste. See
   * {@link com.arcadedb.graph.GraphEngine#deleteEdge(Edge, RID)} for the contract, including why a self-loop skips
   * both sides and why the self-side READ is untouched.
   */
  void deleteEdgeSkippingEndpoint(Edge edge, RID skipEndpoint);

  Record invokeAfterReadEvents(Record record);

  void kill();

  DocumentIndexer getIndexer();

  WALFileFactory getWALFileFactory();

  StatementCache getStatementCache();

  ExecutionPlanCache getExecutionPlanCache();

  CypherStatementCache getCypherStatementCache();

  CypherPlanCache getCypherPlanCache();

  GraphStatisticsCache getGraphStatisticsCache();

  <RET> RET recordFileChanges(final Callable<Object> callback);

  long getLastUpdatedOn();

  long getLastUsedOn();

  long getOpenedOn();

  void saveConfiguration() throws IOException;

  Map<String, Object> alignToReplicas();

  SecurityManager getSecurity();

  /**
   * Executes an operation after having locked files.
   */
  <RET> RET executeLockingFiles(Collection<Integer> fileIds, Callable<RET> callable);

  @Override
  default void setDataEncryption(DataEncryption encryption) {
    getSerializer().setDataEncryption(encryption);
  }

  /**
   * Returns the {@link DataEncryption} configured via {@link #setDataEncryption}, or null if none is configured.
   * Used by components that persist data outside the normal record/page path (e.g. {@code
   * GraphAnalyticalViewCSRPersistence}, issue #6583) so that opting into encryption for a database's records also
   * covers side files derived from that data, rather than only the pages the serializer itself writes.
   */
  default DataEncryption getDataEncryption() {
    return getSerializer().getDataEncryption();
  }

  /**
   * Returns true if writes against this database are forwarded to a replication layer
   * (e.g. Raft). Components that capture WAL bytes for replication (like {@link com.arcadedb.graph.GraphBatch})
   * must keep the WAL on for committed transactions when this is true, otherwise the
   * replica nodes will silently miss the changes.
   */
  default boolean isReplicated() {
    return false;
  }

  /**
   * Runs index compaction, wrapping it in HA replication if this is a Raft leader.
   * The default (standalone) implementation just calls the compaction directly.
   * The HA override captures new files and page content, then replicates them to followers.
   */
  default boolean runWithCompactionReplication(final Callable<Boolean> compaction) throws IOException, InterruptedException {
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

  /**
   * Returns true if this node is the current leader (or always true when not replicated, i.e. standalone).
   * Used by engine-internal background work (e.g. the TimeSeries maintenance scheduler) to keep
   * destructive maintenance leader-only without a compile dependency on the HA module.
   */
  default boolean isLeader() {
    return true;
  }

  /**
   * Registers the post-mutation bytes of a TimeSeries sealed-store file so the HA layer can ship them
   * to followers as part of the current compaction/maintenance replication unit. No-op when standalone:
   * the sealed store is a node-local derived artifact that does not need replication outside HA.
   * Only meaningful while a {@link #runWithCompactionReplication(Callable)} session is active on the
   * calling thread; the buffered blobs are drained and embedded in the SCHEMA_ENTRY shipped at the end
   * of that session.
   *
   * @param typeName     the TimeSeries type owning the shard
   * @param shardIndex   the shard index whose sealed store changed
   * @param sealedFileName the sealed-store file name (relative to the database directory)
   * @param sealedBytes  the full content of the sealed-store file after the mutation
   */
  default void recordTimeSeriesSealedChange(final String typeName, final int shardIndex, final String sealedFileName,
      final byte[] sealedBytes) {
    // no-op: standalone databases do not replicate the sealed store
  }

  /**
   * Gets a global variable value by name.
   * <p>
   * <b>Not replicated in an HA cluster.</b> Global variables are per-node state: under Raft replication
   * ({@code RaftReplicatedDatabase}) every accessor here - this one included - delegates straight to the local
   * node's own {@link LocalDatabase}, with no Raft consensus or replication involved. A value set on one node is
   * invisible to every other node, including after a failover (issue #6560).
   * @param name Variable name (with or without $ prefix)
   * @return The variable value, or null if not set
   */
  Object getGlobalVariable(String name);

  /**
   * Sets a global variable. Setting to null removes the variable.
   * <p>
   * <b>Not replicated in an HA cluster</b> - see {@link #getGlobalVariable(String)}.
   * @param name Variable name (with or without $ prefix)
   * @param value The value to set, or null to remove
   * @return The previous value, or null
   */
  Object setGlobalVariable(String name, Object value);

  /**
   * Atomically sets a global variable only if it is not already set. Unlike calling
   * {@link #getGlobalVariable(String)} followed by {@link #setGlobalVariable(String, Object)}, this check-and-set
   * happens as one operation, so two callers racing on the same name on the SAME node cannot both observe "absent"
   * and both write.
   * <p>
   * <b>Not a cluster-wide distributed lock.</b> The atomicity above is per-node only - see
   * {@link #getGlobalVariable(String)} for why. A caller reaching different nodes of an HA cluster (e.g. two
   * Redis-wire clients connected to different nodes, or the same client across a failover) can each observe
   * "absent" and each believe they won a lock acquired with e.g. Redis {@code SET k v NX}: every node keeps its
   * own independent copy, so two nodes granting the same "lock" simultaneously is expected behavior today, not a
   * bug in this method. Do not rely on this as a real distributed lock in an HA deployment; that would need global
   * variables replicated through Raft, which this method does not do (issue #6560).
   * <p>
   * The default falls back to a non-atomic get-then-set for implementations (e.g. test doubles) that do not need
   * the atomicity guarantee; {@link LocalDatabase} overrides it with a genuinely atomic (per-node) implementation.
   * @param name Variable name (with or without $ prefix)
   * @param value The value to set
   * @return The existing value if the variable was already set (value left untouched), or null if it was absent
   * (value was set)
   */
  default Object setGlobalVariableIfAbsent(final String name, final Object value) {
    final Object existing = getGlobalVariable(name);
    if (existing != null)
      return existing;
    setGlobalVariable(name, value);
    return null;
  }

  /**
   * Atomically sets a global variable only if it is already set. Unlike calling
   * {@link #getGlobalVariable(String)} followed by {@link #setGlobalVariable(String, Object)}, this check-and-set
   * happens as one operation - the counterpart to {@link #setGlobalVariableIfAbsent(String, Object)} (e.g. Redis
   * {@code SET k v XX}).
   * <p>
   * <b>Not replicated in an HA cluster</b> - see {@link #setGlobalVariableIfAbsent(String, Object)}: the atomicity
   * this method gives is per-node only, and different nodes of an HA cluster keep independent copies of the
   * variable.
   * <p>
   * The default falls back to a non-atomic get-then-set for implementations (e.g. test doubles) that do not need
   * the atomicity guarantee; {@link LocalDatabase} overrides it with a genuinely atomic (per-node) implementation.
   * @param name Variable name (with or without $ prefix)
   * @param value The value to set
   * @return The previous value if the variable was set (value was replaced), or null if it was absent (value left
   * untouched)
   */
  default Object setGlobalVariableIfPresent(final String name, final Object value) {
    final Object existing = getGlobalVariable(name);
    if (existing == null)
      return null;
    setGlobalVariable(name, value);
    return existing;
  }

  /**
   * Gets all global variables as an unmodifiable map.
   * <p>
   * <b>Not replicated in an HA cluster</b> - see {@link #getGlobalVariable(String)}. Each entry is this node's
   * own local value, not a cluster-wide view.
   * @return Map of variable name to value
   */
  Map<String, Object> getGlobalVariables();
}
