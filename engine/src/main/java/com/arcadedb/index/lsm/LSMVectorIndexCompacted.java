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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LSMVectorIndexCompacted - Standalone compacted component for LSM Vector Index.
 *
 * <p>Manages immutable merged storage of vector embeddings created during K-way merge compaction.
 *
 * <p>Key Characteristics:
 * <ul>
 *   <li>All pages are immutable after creation</li>
 *   <li>Created during K-way merge compaction from mutable pages</li>
 *   <li>Optimized for read-heavy workloads</li>
 *   <li>Contains built JVector HNSW indexes (Phase 3)</li>
 *   <li>Deduplicates vectors (keeps latest RID for each vector)</li>
 * </ul>
 *
 * @author Arcade Data
 * @since 24.12.0
 */
public class LSMVectorIndexCompacted {

  private final LSMVectorIndex mainIndex;
  private final DatabaseInternal database;
  private final String name;
  private final String filePath;
  private final ComponentFile.MODE fileMode;
  private final int dimensions;
  private final String similarityFunction;
  private final int maxConnections;
  private final int beamWidth;
  private final float alpha;
  private final int pageSize;
  private final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy;

  // In-memory vector storage (Phase 3: persist to disk with JVector HNSW)
  private final Map<LSMVectorIndexMutable.VectorKey, Set<RID>> vectorToRIDs = new HashMap<>();
  private long totalEntries = 0;

  /**
   * Creates a new LSMVectorIndexCompacted component.
   *
   * @param mainIndex the parent LSMVectorIndex
   * @param database the database instance
   * @param name the index name
   * @param filePath the path to the index file
   * @param fileMode the file mode
   * @param pageSize the page size
   * @param dimensions vector dimensionality
   * @param similarityFunction similarity metric (COSINE, EUCLIDEAN, DOT_PRODUCT)
   * @param maxConnections HNSW max connections
   * @param beamWidth HNSW beam width
   * @param alpha HNSW alpha diversity parameter
   * @param nullStrategy the null value strategy
   * @throws IOException if file I/O error occurs
   */
  public LSMVectorIndexCompacted(
      final LSMVectorIndex mainIndex,
      final DatabaseInternal database,
      final String name,
      final String filePath,
      final ComponentFile.MODE fileMode,
      final int pageSize,
      final int dimensions,
      final String similarityFunction,
      final int maxConnections,
      final int beamWidth,
      final float alpha,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) throws IOException {
    this.mainIndex = mainIndex;
    this.database = database;
    this.name = name;
    this.filePath = filePath;
    this.fileMode = fileMode;
    this.pageSize = pageSize;
    this.dimensions = dimensions;
    this.similarityFunction = similarityFunction;
    this.maxConnections = maxConnections;
    this.beamWidth = beamWidth;
    this.alpha = alpha;
    this.nullStrategy = nullStrategy != null ? nullStrategy : LSMTreeIndexAbstract.NULL_STRATEGY.SKIP;
  }

  /**
   * Insert merged vectors during compaction (K-way merge result).
   *
   * <p>Called by compactor to add deduplicated vectors.
   *
   * @param vector the vector embedding
   * @param rids the RIDs associated with this vector
   */
  public void appendDuringCompaction(final float[] vector, final Set<RID> rids) {
    if (vector == null || vector.length != dimensions)
      throw new IllegalArgumentException("Invalid vector");
    if (rids == null || rids.isEmpty())
      throw new IllegalArgumentException("RIDs cannot be empty");

    final LSMVectorIndexMutable.VectorKey key = new LSMVectorIndexMutable.VectorKey(vector);
    vectorToRIDs.put(key, new HashSet<>(rids));
    totalEntries++;
  }

  /**
   * Search for a vector and return all matching RIDs.
   *
   * @param queryVector the query vector
   * @return set of RIDs matching this vector
   */
  public Set<RID> search(final float[] queryVector) {
    if (queryVector == null || queryVector.length != dimensions)
      return Collections.emptySet();

    final LSMVectorIndexMutable.VectorKey key = new LSMVectorIndexMutable.VectorKey(queryVector);
    final Set<RID> result = vectorToRIDs.get(key);
    return result != null ? new HashSet<>(result) : Collections.emptySet();
  }

  /**
   * Find K nearest neighbors using brute force (mutable does this too).
   *
   * <p>Phase 3: JVector will provide efficient KNN via HNSW.
   *
   * @param queryVector the query vector
   * @param k number of neighbors to return
   * @return list of K nearest vectors with their RIDs and distances
   */
  public List<LSMVectorIndexMutable.VectorSearchResult> knnSearch(final float[] queryVector, final int k) {
    return knnSearch(queryVector, k, null);
  }

  /**
   * Find K nearest neighbors using brute force with optional filtering.
   *
   * <p>Implements Option B: integrated filtering during search.
   * Filters results in-place while iterating, skipping ignored RIDs.
   *
   * <p>Phase 3: JVector will provide efficient KNN via HNSW with integrated filtering.
   *
   * @param queryVector the query vector
   * @param k number of neighbors to return
   * @param ignoreCallback optional callback to filter results during search
   * @return list of K nearest vectors with their RIDs and distances
   */
  public List<LSMVectorIndexMutable.VectorSearchResult> knnSearch(final float[] queryVector, final int k,
      final com.arcadedb.index.lsm.LSMVectorIndex.IgnoreVertexCallback ignoreCallback) {
    if (queryVector == null || k <= 0)
      return Collections.emptyList();

    final List<LSMVectorIndexMutable.VectorSearchResult> results = new ArrayList<>();

    for (final Map.Entry<LSMVectorIndexMutable.VectorKey, Set<RID>> entry : vectorToRIDs.entrySet()) {
      final float distance = computeDistance(queryVector, entry.getKey().vector);

      // Filter RIDs in-place if callback provided
      final Set<RID> filteredRids;
      if (ignoreCallback != null) {
        filteredRids = new HashSet<>();
        for (final RID rid : entry.getValue()) {
          if (!ignoreCallback.ignoreVertex(rid)) {
            filteredRids.add(rid);
          }
        }
      } else {
        filteredRids = entry.getValue();
      }

      // Only add result if there are non-ignored RIDs
      if (!filteredRids.isEmpty()) {
        results.add(new LSMVectorIndexMutable.VectorSearchResult(
            entry.getKey().vector, distance, filteredRids));
      }
    }

    // Sort by distance/similarity based on metric type
    // EUCLIDEAN: lower distance is better → ascending sort
    // COSINE/DOT_PRODUCT: higher similarity is better → descending sort
    if ("EUCLIDEAN".equals(similarityFunction)) {
      results.sort((a, b) -> Float.compare(a.distance, b.distance)); // Ascending
    } else {
      // COSINE and DOT_PRODUCT are similarity metrics (higher is better)
      results.sort((a, b) -> Float.compare(b.distance, a.distance)); // Descending
    }

    return results.subList(0, Math.min(k, results.size()));
  }

  /**
   * Compute distance between two vectors.
   */
  private float computeDistance(final float[] vector1, final float[] vector2) {
    if (vector1.length != vector2.length)
      throw new IllegalArgumentException("Vector dimensions mismatch");

    return switch (similarityFunction) {
      case "COSINE" -> cosineSimilarity(vector1, vector2);
      case "EUCLIDEAN" -> euclideanDistance(vector1, vector2);
      case "DOT_PRODUCT" -> dotProduct(vector1, vector2);
      default -> throw new IllegalArgumentException("Unknown similarity function: " + similarityFunction);
    };
  }

  private float cosineSimilarity(final float[] v1, final float[] v2) {
    float dotProduct = 0;
    float norm1 = 0;
    float norm2 = 0;

    for (int i = 0; i < v1.length; i++) {
      dotProduct += v1[i] * v2[i];
      norm1 += v1[i] * v1[i];
      norm2 += v2[i] * v2[i];
    }

    if (norm1 == 0 || norm2 == 0)
      return 0;

    return (float) (dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2)));
  }

  private float euclideanDistance(final float[] v1, final float[] v2) {
    float sum = 0;
    for (int i = 0; i < v1.length; i++) {
      final float diff = v1[i] - v2[i];
      sum += diff * diff;
    }
    return (float) Math.sqrt(sum);
  }

  private float dotProduct(final float[] v1, final float[] v2) {
    float result = 0;
    for (int i = 0; i < v1.length; i++) {
      result += v1[i] * v2[i];
    }
    return result;
  }

  /**
   * Get number of vectors in compacted index.
   *
   * @return vector count
   */
  public long countEntries() {
    return totalEntries;
  }

  /**
   * Close compacted index and cleanup resources.
   */
  public void close() {
    // Phase 3: Flush JVector HNSW index to disk
  }

  // Getters for vector parameters
  public int getDimensions() {
    return dimensions;
  }

  public String getSimilarityFunction() {
    return similarityFunction;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public int getBeamWidth() {
    return beamWidth;
  }

  public float getAlpha() {
    return alpha;
  }
}
