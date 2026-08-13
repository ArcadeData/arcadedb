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

  boolean checkTransactionIsActive(boolean createTx);

  boolean isAsyncProcessing();

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
   * @param name Variable name (with or without $ prefix)
   * @return The variable value, or null if not set
   */
  Object getGlobalVariable(String name);

  /**
   * Sets a global variable. Setting to null removes the variable.
   * @param name Variable name (with or without $ prefix)
   * @param value The value to set, or null to remove
   * @return The previous value, or null
   */
  Object setGlobalVariable(String name, Object value);

  /**
   * Gets all global variables as an unmodifiable map.
   * @return Map of variable name to value
   */
  Map<String, Object> getGlobalVariables();
}
