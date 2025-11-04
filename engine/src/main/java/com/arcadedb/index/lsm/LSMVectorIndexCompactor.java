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
package com.arcadedb.index.lsm;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * LSMVectorIndexCompactor - Orchestrates K-way merge of mutable vector pages.
 *
 * <p>Responsible for:
 * <ul>
 *   <li>Merging vectors from multiple mutable pages</li>
 *   <li>Deduplicating vectors (keeping latest RID for each vector)</li>
 *   <li>Building compacted index with merged results</li>
 *   <li>Coordinating atomic swap of mutable → compacted transition</li>
 * </ul>
 *
 * <p><b>K-way Merge Algorithm:</b>
 * 1. Collect all vectors from current mutable index
 * 2. For each unique vector (by VectorKey), aggregate all RIDs
 * 3. Create new compacted index with merged vectors
 * 4. Create new mutable index for fresh writes
 * 5. Atomically swap old mutable/compacted with new instances
 *
 * @author Arcade Data
 * @since 24.12.0
 */
public class LSMVectorIndexCompactor {

  private final LSMVectorIndex                     mainIndex;
  private final DatabaseInternal                   database;
  private final String                             indexName;
  private final String                             filePath;
  private final int                                dimensions;
  private final String                             similarityFunction;
  private final int                                maxConnections;
  private final int                                beamWidth;
  private final float                              alpha;
  private final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy;

  /**
   * Creates a new compactor instance.
   *
   * @param mainIndex          the parent LSMVectorIndex
   * @param database           the database instance
   * @param indexName          the index name
   * @param filePath           the index file path
   * @param dimensions         vector dimensionality
   * @param similarityFunction similarity metric
   * @param maxConnections     HNSW max connections
   * @param beamWidth          HNSW beam width
   * @param alpha              HNSW alpha parameter
   * @param nullStrategy       null value strategy
   */
  public LSMVectorIndexCompactor(
      final LSMVectorIndex mainIndex,
      final DatabaseInternal database,
      final String indexName,
      final String filePath,
      final int dimensions,
      final String similarityFunction,
      final int maxConnections,
      final int beamWidth,
      final float alpha,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    this.mainIndex = mainIndex;
    this.database = database;
    this.indexName = indexName;
    this.filePath = filePath;
    this.dimensions = dimensions;
    this.similarityFunction = similarityFunction;
    this.maxConnections = maxConnections;
    this.beamWidth = beamWidth;
    this.alpha = alpha;
    this.nullStrategy = nullStrategy;
  }

  /**
   * Execute K-way merge compaction.
   *
   * <p>This method:
   * 1. Collects all vectors from current mutable index
   * 2. Deduplicates vectors by VectorKey
   * 3. Creates new compacted index with merged data
   * 4. Creates new mutable index for future writes
   * 5. Atomically swaps components in mainIndex
   *
   * @return true if compaction succeeded, false if scheduling conditions not met
   *
   * @throws IOException          if file I/O error occurs
   * @throws InterruptedException if thread is interrupted
   */
  public boolean executeCompaction() throws IOException, InterruptedException {
    // Phase 1: Collect all vectors from current mutable index
    final LSMVectorIndexMutable currentMutable = mainIndex.getMutableIndex();
    if (currentMutable == null)
      return false;

    final Map<LSMVectorIndexMutable.VectorKey, Set<RID>> allVectors =
        currentMutable.getAllVectors();

    if (allVectors.isEmpty()) {
      // Nothing to compact
      return true;
    }

    // Phase 2: Create new compacted index with merged vectors
    final LSMVectorIndexCompacted newCompacted = createCompactedIndex();

    // Phase 3: Merge vectors into compacted index
    mergeVectorsIntoCompacted(allVectors, newCompacted);

    // Phase 4: Create new mutable index for future writes
    final LSMVectorIndexMutable newMutable = createNewMutableIndex();

    // Phase 5: Atomically swap components
    swapComponents(currentMutable, newCompacted, newMutable);

    return true;
  }

  /**
   * Create new compacted index for merged vectors.
   *
   * <p>Phase 4: Creates HNSW-accelerated compacted index for efficient KNN search.
   *
   * @return new LSMVectorIndexCompactedWithHNSW instance with HNSW support
   *
   * @throws IOException if file I/O error occurs
   */
  private LSMVectorIndexCompacted createCompactedIndex() throws IOException {
    // Phase 4: Use HNSW-accelerated compacted index for fast KNN
    return new LSMVectorIndexCompactedWithHNSW(
        mainIndex,
        database,
        indexName + "_compacted",
        filePath + "_compacted",
        ComponentFile.MODE.READ_WRITE,
        LSMVectorIndexMutable.DEFAULT_PAGE_SIZE,
        dimensions,
        similarityFunction,
        maxConnections,
        beamWidth,
        alpha,
        nullStrategy);
  }

  /**
   * Merge all vectors from mutable pages into compacted index.
   *
   * <p>Phase 3: K-way merge. Phase 4: Also finalizes HNSW build.
   *
   * @param vectors   map of vector keys to RID sets
   * @param compacted target compacted index
   */
  private void mergeVectorsIntoCompacted(
      final Map<LSMVectorIndexMutable.VectorKey, Set<RID>> vectors,
      final LSMVectorIndexCompacted compacted) {

    // Phase 3: K-way merge implementation
    // For each unique vector, append to compacted index
    for (final Map.Entry<LSMVectorIndexMutable.VectorKey, Set<RID>> entry : vectors.entrySet()) {
      final LSMVectorIndexMutable.VectorKey key = entry.getKey();
      final Set<RID> rids = entry.getValue();

      // Append deduplicated vector to compacted index
      compacted.appendDuringCompaction(key.vector, new HashSet<>(rids));
    }

    // Phase 4: Finalize HNSW index if using HNSW compacted
    if (compacted instanceof LSMVectorIndexCompactedWithHNSW) {
      final LSMVectorIndexCompactedWithHNSW hnswCompacted =
          (LSMVectorIndexCompactedWithHNSW) compacted;
      hnswCompacted.finalizeHNSWBuild();
    }
  }

  /**
   * Create new mutable index for future writes.
   *
   * @return new LSMVectorIndexMutable instance
   *
   * @throws IOException if file I/O error occurs
   */
  private LSMVectorIndexMutable createNewMutableIndex() throws IOException {
    return new LSMVectorIndexMutable(
        mainIndex,
        database,
        indexName,
        filePath,
        ComponentFile.MODE.READ_WRITE,
        LSMVectorIndexMutable.DEFAULT_PAGE_SIZE,
        dimensions,
        similarityFunction,
        maxConnections,
        beamWidth,
        alpha,
        nullStrategy);
  }

  /**
   * Atomically swap mutable and compacted components in main index.
   *
   * <p>This ensures thread-safe transition where:
   * - New compacted index replaces old one (or becomes new)
   * - Old mutable index is replaced with new one
   * - Old compacted index resources are released
   *
   * @param oldMutable   the old mutable index being replaced
   * @param newCompacted the new compacted index from merge
   * @param newMutable   the new mutable index for future writes
   */
  private void swapComponents(
      final LSMVectorIndexMutable oldMutable,
      final LSMVectorIndexCompacted newCompacted,
      final LSMVectorIndexMutable newMutable) {

    // Close old mutable index
    if (oldMutable != null) {
      oldMutable.close();
    }

    // Close old compacted index if it exists
    final LSMVectorIndexCompacted oldCompacted = mainIndex.getCompactedIndex();
    if (oldCompacted != null) {
      oldCompacted.close();
    }

    // Swap components (handled by synchronized setters in LSMVectorIndex)
    mainIndex.setMutableIndex(newMutable);
    mainIndex.setCompactedIndex(newCompacted);
  }
}
