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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
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

  long build(int buildIndexBatchSize, BuildIndexCallback callback);

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
   * An index whose on-disk layout predates a change the engine cannot apply in place keeps working with the old
   * layout - correctness first - but does not get whatever the new one buys. This is how it says so: the schema
   * load logs it once per database open, and it is exposed as {@code upgradeWarning} on {@code schema:indexes} and
   * {@code schema:index:<name>}, which is what Studio renders. The remedy is always
   * {@code REBUILD INDEX &lt;name&gt;}, so say what is lost and why, not what to type.
   * <p>
   * Implementations must keep this cheap and side-effect free: it is called per index on every listing.
   */
  default String getUpgradeWarning() {
    return null;
  }
}
