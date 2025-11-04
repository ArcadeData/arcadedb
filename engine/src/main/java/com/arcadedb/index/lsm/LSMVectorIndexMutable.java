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
 * LSMVectorIndexMutable - Standalone mutable component for LSM Vector Index.
 *
 * <p>Manages writable pages containing vector embeddings and their associated RIDs.
 * Implements custom page management optimized for vector storage.
 *
 * <p>Key Responsibilities:
 * <ul>
 *   <li>Store vector embeddings in mutable pages (fast writes)</li>
 *   <li>Search vectors with exact and similarity queries</li>
 *   <li>Manage page lifecycle (creation, full detection, immutability marking)</li>
 *   <li>Schedule compaction when page threshold exceeded</li>
 *   <li>Track vector entries for K-way merge during compaction</li>
 * </ul>
 *
 * <p><b>Vector Storage Format:</b>
 * Each vector entry stores: `[dimensions(int)][float_values][RID_count(int)][RID_values]`
 *
 * <p><b>Page Structure:</b>
 * - Mutable pages grow from both ends (index array and data area)
 * - When full, page is marked immutable and new page created
 * - Multiple pages enable fast writes without compaction pause
 *
 * @author Arcade Data
 * @since 24.12.0
 */
public class LSMVectorIndexMutable {

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

  // In-memory vector storage (Phase 3: persist to disk)
  private final Map<VectorKey, Set<RID>> vectorToRIDs = new HashMap<>();
  private final List<MutablePage> mutablePages = new ArrayList<>();
  private int totalEntries = 0;

  // Threshold for scheduling compaction
  private static final int DEFAULT_MIN_PAGES_TO_SCHEDULE_COMPACTION = 100;
  public static final int DEFAULT_PAGE_SIZE = 65536; // 64KB default page size
  private int minPagesToScheduleACompaction = DEFAULT_MIN_PAGES_TO_SCHEDULE_COMPACTION;

  /**
   * Creates a new LSMVectorIndexMutable component.
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
  public LSMVectorIndexMutable(
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

    // Initialize with first mutable page
    mutablePages.add(new MutablePage(0, pageSize));
  }

  /**
   * Insert a vector into the mutable index.
   *
   * @param vector the vector embedding (float array)
   * @param rid the record ID associated with this vector
   */
  public void put(final float[] vector, final RID rid) {
    if (vector == null)
      throw new IllegalArgumentException("Vector cannot be null");
    if (vector.length != dimensions)
      throw new IllegalArgumentException(
          "Vector dimension mismatch: expected " + dimensions + ", got " + vector.length);
    if (rid == null)
      throw new IllegalArgumentException("RID cannot be null");

    final VectorKey key = new VectorKey(vector);
    vectorToRIDs.computeIfAbsent(key, k -> new HashSet<>()).add(rid);
    totalEntries++;

    // Phase 3: Persist to page storage
    // For now, keep in memory
  }

  /**
   * Remove a vector from the mutable index.
   *
   * @param vector the vector embedding
   * @param rid the record ID to remove
   */
  public void remove(final float[] vector, final RID rid) {
    if (vector == null)
      throw new IllegalArgumentException("Vector cannot be null");

    final VectorKey key = new VectorKey(vector);
    final Set<RID> rids = vectorToRIDs.get(key);
    if (rids != null && rids.remove(rid)) {
      totalEntries--;
      if (rids.isEmpty()) {
        vectorToRIDs.remove(key);
      }
    }
  }

  /**
   * Search for a vector and return all matching RIDs.
   *
   * @param queryVector the query vector
   * @return set of RIDs matching this vector
   */
  public Set<RID> search(final float[] queryVector) {
    if (queryVector == null)
      return Collections.emptySet();
    if (queryVector.length != dimensions)
      return Collections.emptySet();

    final VectorKey key = new VectorKey(queryVector);
    final Set<RID> result = vectorToRIDs.get(key);
    return result != null ? new HashSet<>(result) : Collections.emptySet();
  }

  /**
   * Find K nearest neighbors to the query vector using brute force.
   *
   * <p>Phase 3: This is basic brute force search. JVector will be used
   * in compacted index for efficient KNN via HNSW.
   *
   * @param queryVector the query vector
   * @param k number of neighbors to return
   * @return list of K nearest vectors with their RIDs and distances
   */
  public List<VectorSearchResult> knnSearch(final float[] queryVector, final int k) {
    return knnSearch(queryVector, k, null);
  }

  /**
   * Find K nearest neighbors to the query vector using brute force with optional filtering.
   *
   * <p>Implements Option B: integrated filtering during search.
   * Filters results in-place while iterating, skipping ignored RIDs.
   *
   * @param queryVector the query vector
   * @param k number of neighbors to return
   * @param ignoreCallback optional callback to filter results during search
   * @return list of K nearest vectors with their RIDs and distances
   */
  public List<VectorSearchResult> knnSearch(final float[] queryVector, final int k,
      final LSMVectorIndex.IgnoreVertexCallback ignoreCallback) {
    if (queryVector == null || k <= 0)
      return Collections.emptyList();

    final List<VectorSearchResult> results = new ArrayList<>();

    // Iterate through all vectors and compute distances
    for (final Map.Entry<VectorKey, Set<RID>> entry : vectorToRIDs.entrySet()) {
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
        results.add(new VectorSearchResult(entry.getKey().vector, distance, filteredRids));
      }
    }

    // Sort by distance/similarity
    // IMPORTANT: Different metrics have different "better" directions:
    // - EUCLIDEAN: lower distance is better → ascending sort
    // - COSINE: higher similarity is better (1.0 = identical) → descending sort
    // - DOT_PRODUCT: higher product is better → descending sort
    if ("EUCLIDEAN".equals(similarityFunction)) {
      results.sort((a, b) -> Float.compare(a.distance, b.distance)); // Ascending
    } else {
      // COSINE and DOT_PRODUCT are similarity metrics (higher is better)
      results.sort((a, b) -> Float.compare(b.distance, a.distance)); // Descending
    }

    // Return top K
    return results.subList(0, Math.min(k, results.size()));
  }

  /**
   * Compute distance between two vectors using configured similarity function.
   *
   * @param vector1 first vector
   * @param vector2 second vector
   * @return distance value
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

  /**
   * Compute cosine similarity between two vectors.
   */
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

  /**
   * Compute Euclidean distance between two vectors.
   */
  private float euclideanDistance(final float[] v1, final float[] v2) {
    float sum = 0;
    for (int i = 0; i < v1.length; i++) {
      final float diff = v1[i] - v2[i];
      sum += diff * diff;
    }
    return (float) Math.sqrt(sum);
  }

  /**
   * Compute dot product of two vectors.
   */
  private float dotProduct(final float[] v1, final float[] v2) {
    float result = 0;
    for (int i = 0; i < v1.length; i++) {
      result += v1[i] * v2[i];
    }
    return result;
  }

  /**
   * Get all vectors stored in this mutable index.
   *
   * <p>Used by compactor for K-way merge.
   *
   * @return map of vectors to their RIDs
   */
  public Map<VectorKey, Set<RID>> getAllVectors() {
    return new HashMap<>(vectorToRIDs);
  }

  /**
   * Clear the mutable index (called after compaction).
   */
  public void clear() {
    vectorToRIDs.clear();
    mutablePages.clear();
    totalEntries = 0;
    mutablePages.add(new MutablePage(0, pageSize));
  }

  /**
   * Get number of vectors in this mutable index.
   *
   * @return vector count
   */
  public long countEntries() {
    return totalEntries;
  }

  /**
   * Check if compaction should be scheduled.
   *
   * @return true if mutable pages exceed threshold
   */
  public boolean shouldScheduleCompaction() {
    return mutablePages.size() >= minPagesToScheduleACompaction;
  }

  /**
   * Get minimum pages threshold for compaction scheduling.
   *
   * @return pages threshold
   */
  public int getMinPagesToScheduleACompaction() {
    return minPagesToScheduleACompaction;
  }

  /**
   * Set minimum pages threshold for compaction scheduling.
   *
   * @param minPages pages threshold
   */
  public void setMinPagesToScheduleACompaction(final int minPages) {
    if (minPages <= 0)
      throw new IllegalArgumentException("minPagesToScheduleACompaction must be > 0");
    this.minPagesToScheduleACompaction = minPages;
  }

  /**
   * Get vector dimensions.
   *
   * @return dimensions
   */
  public int getDimensions() {
    return dimensions;
  }

  /**
   * Get similarity function.
   *
   * @return similarity function name
   */
  public String getSimilarityFunction() {
    return similarityFunction;
  }

  /**
   * Get HNSW max connections.
   *
   * @return max connections
   */
  public int getMaxConnections() {
    return maxConnections;
  }

  /**
   * Get HNSW beam width.
   *
   * @return beam width
   */
  public int getBeamWidth() {
    return beamWidth;
  }

  /**
   * Get HNSW alpha parameter.
   *
   * @return alpha
   */
  public float getAlpha() {
    return alpha;
  }

  /**
   * Close the mutable index (cleanup resources).
   */
  public void close() {
    mutablePages.forEach(MutablePage::close);
  }

  // ==================== Inner Classes ====================

  /**
   * Represents a mutable page in memory.
   */
  static class MutablePage {
    private final int pageNumber;
    private final int pageSize;
    private boolean mutable = true;

    MutablePage(final int pageNumber, final int pageSize) {
      this.pageNumber = pageNumber;
      this.pageSize = pageSize;
    }

    void close() {
      // Phase 3: Persist page to disk
    }

    boolean isMutable() {
      return mutable;
    }

    void markImmutable() {
      this.mutable = false;
    }
  }

  /**
   * Wrapper for vector with proper equality and hashing.
   */
  static class VectorKey {
    final float[] vector;
    private final int hash;

    VectorKey(final float[] vector) {
      this.vector = vector.clone();
      this.hash = computeHash(vector);
    }

    private static int computeHash(final float[] vector) {
      int hash = 1;
      for (final float v : vector) {
        hash = 31 * hash + Float.floatToIntBits(v);
      }
      return hash;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof VectorKey))
        return false;
      final VectorKey other = (VectorKey) obj;
      if (vector.length != other.vector.length)
        return false;
      for (int i = 0; i < vector.length; i++) {
        if (Float.floatToIntBits(vector[i]) != Float.floatToIntBits(other.vector[i]))
          return false;
      }
      return true;
    }
  }

  /**
   * Result of a KNN search.
   */
  public static class VectorSearchResult {
    public final float[] vector;
    public final float distance;
    public final Set<RID> rids;

    public VectorSearchResult(final float[] vector, final float distance, final Set<RID> rids) {
      this.vector = vector;
      this.distance = distance;
      this.rids = new HashSet<>(rids);
    }
  }
}
