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
package com.arcadedb.index;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Component;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Internal Index interface.
 */
@ExcludeFromJacocoGeneratedReport
public interface IndexInternal extends Index {
  enum INDEX_STATUS {UNAVAILABLE, AVAILABLE, COMPACTION_SCHEDULED, COMPACTION_IN_PROGRESS}

  /**
   * Populates the index by SCANNING the records it covers.
   *
   * @param buildIndexBatchSize    records per chunked commit, for the builds that chunk by record count. Never a
   *                               permission: see {@code sharesCallerTransaction}, which is the one that says whether
   *                               committing is allowed at all
   * @param sharesCallerTransaction whether this build is running INSIDE a transaction somebody else opened, which is
   *                               what lets its scan see records that transaction has written and not committed
   *                               (issue #6324, item 1). A build that shares a transaction may not commit it, and has
   *                               to ask that transaction for its own written copy of each scanned record
   * @param callback               per-record progress callback, or null
   */
  long build(int buildIndexBatchSize, boolean sharesCallerTransaction, BuildIndexCallback callback);

  /**
   * A build that OWNS the transaction it runs in, which is every build reached other than through
   * {@code IndexBuilder#buildCreatedIndex}: it may commit, and there is no caller's uncommitted work for it to see.
   */
  default long build(final int buildIndexBatchSize, final BuildIndexCallback callback) {
    return build(buildIndexBatchSize, false, callback);
  }

  /**
   * The chunked commit every scan-based {@link #build(int, boolean, BuildIndexCallback)} performs: once every
   * {@code buildIndexBatchSize} records the build's transaction is committed and reopened, so indexing a large bucket
   * does not accumulate every entry of it in one transaction.
   * <p>
   * Written once and shared by all four scan-based builds because the condition under which it must NOT run is the
   * subtle half, and four copies of it were four chances to get it wrong. It must not run when the build is sharing a
   * transaction opened by a caller - a {@code CREATE INDEX} inside an open transaction, which is where the records
   * being indexed are the caller's own uncommitted writes (issue #6324, item 1). Committing there would publish
   * whatever else that transaction has done, halfway through a DDL statement the caller has not finished, and would
   * leave the caller holding a transaction object that has already been popped.
   * <p>
   * The permission is a PARAMETER OF ITS OWN and never a value of {@code buildIndexBatchSize}. Spelling it as
   * "batch size 0" would collide with a user-supplied {@code REBUILD INDEX ... WITH batchSize = 0} for any index
   * family that does not chunk by record count - the vector builds chunk by bytes and would read that literal zero as
   * the permission. A non-positive batch size is still treated as "no chunking" rather than as a modulo by zero,
   * which is what the four copies did with it.
   *
   * @param database                the database whose build transaction is being chunked
   * @param processedRecords        how many records the build has indexed so far
   * @param buildIndexBatchSize     records per chunk; non-positive means no chunking
   * @param sharesCallerTransaction when true the build owns nothing and commits nothing
   */
  static void commitBuildBatch(final DatabaseInternal database, final long processedRecords,
      final int buildIndexBatchSize, final boolean sharesCallerTransaction) {
    if (sharesCallerTransaction || buildIndexBatchSize <= 0 || processedRecords % buildIndexBatchSize != 0)
      return;
    database.getWrappedDatabaseInstance().commit();
    database.getWrappedDatabaseInstance().begin();
  }

  /**
   * The version of {@code scanned} a scan-based build must index: the transaction's own written copy when it has one,
   * the page image the scan handed over otherwise.
   * <p>
   * A bucket scan reads bytes off pages, and an UPDATE inside a transaction does not touch the page until commit -
   * it is parked as a deferred update and serialized in the first commit phase, precisely so that repeatedly updating
   * the same record does not rewrite the page each time. So a build sharing the caller's transaction (issue #6324,
   * item 1) sees the record's OLD content and would index a key the record no longer has, while the commit-time
   * serialization removes that same key directly on the index and the build's staged entry is replayed on top of it -
   * leaving the stale key behind for good. Asking the transaction closes the gap at the only place the two views can
   * disagree.
   * <p>
   * A build that owns its transaction is answered without asking anything: it is on the per-record hot path of every
   * index build and rebuild in the engine, and there is nothing for a transaction it opened itself to correct.
   *
   * @param database                the database whose transaction may hold a newer copy
   * @param scanned                 the record as the bucket scan produced it
   * @param sharesCallerTransaction whether there is a caller's transaction that could hold a newer copy at all
   */
  static Document buildSourceRecord(final DatabaseInternal database, final Record scanned,
      final boolean sharesCallerTransaction) {
    if (!sharesCallerTransaction)
      return (Document) scanned;

    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null) {
      final Record written = tx.getWrittenRecord(scanned.getIdentity());
      if (written instanceof Document document)
        return document;
    }
    return (Document) scanned;
  }

  boolean compact() throws IOException, InterruptedException;

  IndexMetadata getMetadata();

  void setMetadata(IndexMetadata metadata);

  void setMetadata(JSONObject indexJSON);

  boolean setStatus(INDEX_STATUS[] expectedStatuses, INDEX_STATUS newStatus);

  default void flush() {
  }

  /**
   * Recomputes any persisted per-index statistics (e.g. BM25 corpus counters) by rescanning the live data, repairing drift that
   * incremental maintenance cannot reverse (rolled-back transactions, analyzer changes). Most indexes keep no such statistics and
   * this is a no-op. Exposed to SQL via {@code REBUILD INDEX <name> {statsOnly: true}} so operators can repair drift without a
   * full (and far more expensive) index rebuild.
   *
   * @return true if statistics were recomputed (the index keeps such statistics), false otherwise
   */
  default boolean recomputeStatistics() {
    return false;
  }

  /**
   * Performs a lightweight structural/metadata integrity check of the index, independent of record content, and
   * returns a list of human-readable problem descriptions. An empty list means the index metadata is healthy.
   * Used by CHECK DATABASE to surface on-disk corruption (e.g. a damaged hash index metadata page, issue #352)
   * proactively, before it manifests as a cryptic failure during a query.
   */
  default List<String> checkIntegrity() {
    return Collections.emptyList();
  }

  /**
   * Stops everything this index keeps running in the background - timers, thread pools, pooled per-query state -
   * WITHOUT touching its files or its durability state (issue #5418). Most indexes run nothing of the sort and
   * this is a no-op; the LSM vector index is the notable exception, with an inactivity rebuild timer, a graph
   * build pool and a pool of graph searchers.
   * <p>
   * Invoked by {@code LocalDatabase} on close and on drop, right after the index has been flushed. It is
   * deliberately NOT {@link #close()}: that one also closes the index files, which on the database close path
   * must stay open until the pending pages have been flushed and {@code FileManager.close()} closes them.
   */
  default void releaseBackgroundResources() {
  }

  /**
   * Closes the index files. The caller must guarantee that every page committed against this index has already
   * reached disk (e.g. {@code PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(database)}) before calling
   * this: a page still queued for asynchronous flush (the default, see {@code TransactionContext.asyncFlush})
   * finds its file closed underneath it, is left unflushed, and turns the WAL-preserving recovery that follows
   * into a torn page instead of a clean replay (issue #5971). {@code LocalDatabase}'s own close path satisfies
   * this by construction - it never calls this method directly, closing index files only via
   * {@code FileManager.close()} once the database-wide flush has completed.
   */
  void close();

  void drop();

  Map<String, Long> getStats();

  int getFileId();

  Component getComponent();

  Type[] getKeyTypes();

  byte[] getBinaryKeyTypes();

  List<Integer> getFileIds();

  void setTypeIndex(TypeIndex typeIndex);

  TypeIndex getTypeIndex();

  int getPageSize();

  /**
   * Page size to use when this index's configuration is carried over into a NEW index file - a rebuild, or a
   * propagation to a freshly added bucket or sub type. Defaults to the current one, which is what "carry the
   * configuration over" means, but an index whose CURRENT page size is not one it would accept at creation has to
   * answer with a legal one instead (issue #5713).
   * <p>
   * Distinct from {@link #getPageSize()} because the value goes back through the creation path, which validates.
   * A rebuild in particular DROPS the index before recreating it ({@code RebuildIndexStatement.buildIndex}), so
   * handing back a page size creation refuses would delete the index and then fail to build the replacement -
   * turning the documented repair for a damaged index into the thing that loses it.
   */
  default int getPageSizeForNewFile() {
    return getPageSize();
  }

  /**
   * Configuration to replay when this index's definition is carried over into a NEW index file - a rebuild, a truncate,
   * a propagation to a freshly added bucket or sub type, a {@code copyType()}. The companion of
   * {@link #getPageSizeForNewFile()} for everything that is not the page size, and the value to feed
   * {@link IndexMetadata#copy(String, String[], int)} before handing it to a builder.
   * <p>
   * Distinct from {@link #getMetadata()} because that one answers whatever the index stores internally, which for the
   * wrapper index types is the UNDERLYING LSM-Tree's plain {@link IndexMetadata}: a full-text index keeps its analyzers
   * and BM25 configuration in its own {@code FullTextIndexMetadata}, a geospatial one keeps its resolution and layout
   * in its own fields, and neither is reachable through the underlying index. A carry-over site reading
   * {@code getMetadata()} therefore silently recreates the index with the default configuration (issue #5723) - which
   * for a full-text index means a different analyzer and a different ranking.
   */
  default IndexMetadata getMetadataForNewFile() {
    return getMetadata();
  }

  boolean isCompacting();

  boolean isValid();

  boolean scheduleCompaction();

  String getMostRecentFileName();

  JSONObject toJSON();

  IndexInternal getAssociatedIndex();

  void updateTypeName(String newTypeName);

  /**
   * Replay entry point invoked by {@code TransactionIndexContext.applyChanges} at commit time.
   * Default implementation forwards to {@link Index#put(Object[], RID[])}. Wrapper indexes that
   * re-shape keys on the original call (e.g. {@code LSMTreeFullTextIndex} which tokenizes raw
   * text into one posting per analyzed term, or {@code LSMTreeGeoIndex} which tokenizes a WKT
   * shape into GeoHash cells) override this to skip the wrapping logic on replay, since the
   * keys queued onto the transaction are already the storage form (issue #4073).
   */
  default void putReplay(final Object[] keys, final RID[] rids) {
    put(keys, rids);
  }

  /**
   * Replay entry point invoked by {@code TransactionIndexContext.applyChanges} at commit time
   * for REMOVE / REPLACE operations. Default implementation forwards to
   * {@link Index#remove(Object[], Identifiable)}. See {@link #putReplay} for the rationale.
   */
  default void removeReplay(final Object[] keys, final Identifiable rid) {
    remove(keys, rid);
  }

  /**
   * Whether the per-transaction changes of this index must be held in {@code TransactionIndexContext}'s
   * key-ordered map (the default) or can ride an append-only lane replayed in insertion order.
   * <p>
   * The ordered map exists to serve three needs of a classic LSM-Tree index: in-transaction cursor
   * navigation over uncommitted keys, immediate duplicated-key detection on unique indexes, and
   * collapsing repeated operations on the same key so commit replays each key once. Its cost is one
   * {@code O(log n)} comparison chain plus a per-key {@code HashMap} for every queued entry.
   * <p>
   * That trade is wrong for an index whose single record produces many entries. A sparse vector
   * queues one posting per non-zero dimension - hundreds per record on learned-sparse corpora
   * (issue #5411) - none of which is unique, cursor-navigable, or worth deduplicating, since the
   * receiving structure is itself last-write-wins. Returning {@code false} routes those entries to
   * an append-only list that replays in the exact order they were queued, which preserves
   * "the last operation on a posting wins" without any key bookkeeping.
   * <p>
   * An index may only return {@code false} when it is non-unique <b>and</b> exposes no
   * in-transaction read path, since both features read the ordered map back.
   */
  default boolean isTransactionKeyOrderRequired() {
    return true;
  }

  /**
   * Returns {@code true} when {@link Index#get} answers with a SUPERSET of the matching records that the caller must
   * re-check with the real predicate - a spatial grid approximates a shape with cells, so a cell hit is a candidate,
   * not a match.
   * <p>
   * The consequence is that a row {@code limit} cannot be applied to this index's output: truncating candidates
   * BEFORE the predicate runs drops rows that would have survived it, and does so silently. Both
   * {@link Index#get(Object[], int)} on such an index and {@link TypeIndex#get(Object[], int)} over it therefore
   * ignore a positive limit and return every candidate; the caller applies the limit after filtering
   * ({@code IndexableSQLFunction.shouldExecuteAfterSearch}).
   */
  default boolean isResultApproximate() {
    return false;
  }

  /**
   * Returns a human-readable reason why this index should be rebuilt, or {@code null} when there is none.
   * <p>
   * Two shapes reach this. Most often an index whose on-disk layout predates a change the engine cannot apply in
   * place keeps working with the old layout - correctness first - but does not get whatever the new one buys. The
   * other is an index the current build can no longer read correctly at all, such as one whose physical key order
   * predates the string comparison fix of #5321 and whose lookups therefore return fewer records than a scan
   * (#5802). Either way this is how it says so: the schema load logs it once per database open, and it is exposed as
   * {@code upgradeWarning} on {@code schema:indexes} and {@code schema:index:<name>}, which is what Studio renders.
   * The remedy is always {@code REBUILD INDEX &lt;name&gt;}, so say what is lost and why, not what to type - and,
   * since the two shapes cost the reader very different things, say which one this is.
   * <p>
   * Implementations must keep this cheap and side-effect free: it is called per index on every listing.
   */
  default String getUpgradeWarning() {
    return null;
  }
}
