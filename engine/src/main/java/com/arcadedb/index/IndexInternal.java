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
    return List.of();
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
}
