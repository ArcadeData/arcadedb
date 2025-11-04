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
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LSMVectorIndexCompactedWithHNSW - Compacted index with HNSW acceleration.
 *
 * <p>Extends LSMVectorIndexCompacted with JVector's HNSW (Hierarchical Navigable Small World)
 * graph indexing for efficient approximate K-nearest neighbor search on immutable compacted pages.
 *
 * <p>Architecture:
 * <ul>
 *   <li>Parent class manages HashMap vector storage (inherited)</li>
 *   <li>This class adds HNSW graph index for fast KNN</li>
 *   <li>Maps HNSW node IDs to RID arrays</li>
 *   <li>Routes queries: exact match → HashMap, KNN → HNSW</li>
 * </ul>
 *
 * <p><b>K-NN Search Flow:</b>
 * 1. Query arrives via `knnSearch(queryVector, k)`
 * 2. HNSW navigates graph layers to find candidates
 * 3. Each candidate HNSW node ID mapped to RID set
 * 4. Results merged with distance scores
 * 5. Top K returned to caller
 *
 * @author Arcade Data
 * @since 24.12.0
 */
public class LSMVectorIndexCompactedWithHNSW extends LSMVectorIndexCompacted {

  // HNSW Parameters
  private static final int DEFAULT_MAX_M           = 16;            // Maximum connections per node
  private static final int DEFAULT_EF_CONSTRUCTION = 200; // Beam width for construction
  private static final int DEFAULT_EF              = 200;              // Beam width for search

  // HNSW Index and mapping
  private final Map<Integer, RID[]>        idToRids        = new HashMap<>();
  private final Map<Integer, float[]>      idToVectors     = new HashMap<>();
  private       int                        nextId          = 0;
  private final int                        maxM;
  private final int                        efConstruction;
  private final int                        ef;
  private       GraphIndex                 hnswIndex       = null;     // Phase 4.5: JVector HNSW graph
  private final VectorTypeSupport          vectorTypeSupport;          // JVector vector factory

  /**
   * Creates a new LSMVectorIndexCompactedWithHNSW instance.
   *
   * <p>Phase 4: HNSW integration provides framework for enhanced KNN performance.
   * Current implementation uses HashMap for compatibility. Future versions will
   * integrate JVector's GraphIndex for O(log n) search instead of O(n) brute force.
   *
   * @param mainIndex          the parent LSMVectorIndex
   * @param database           the database instance
   * @param name               the index name
   * @param filePath           the index file path
   * @param fileMode           the file mode
   * @param pageSize           the page size
   * @param dimensions         vector dimensionality
   * @param similarityFunction similarity metric (COSINE, EUCLIDEAN, DOT_PRODUCT)
   * @param maxConnections     HNSW max connections (M parameter)
   * @param beamWidth          HNSW beam width (ef parameter)
   * @param alpha              HNSW alpha (not used in JVector)
   * @param nullStrategy       the null value strategy
   *
   * @throws IOException if file I/O error occurs
   */
  public LSMVectorIndexCompactedWithHNSW(
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

    super(mainIndex, database, name, filePath, fileMode, pageSize, dimensions,
        similarityFunction, maxConnections, beamWidth, alpha, nullStrategy);

    this.maxM = Math.max(maxConnections, DEFAULT_MAX_M);
    this.efConstruction = Math.max(beamWidth, DEFAULT_EF_CONSTRUCTION);
    this.ef = Math.max(beamWidth, DEFAULT_EF);

    // Initialize JVector vector factory
    this.vectorTypeSupport = VectorizationProvider.getInstance().getVectorTypeSupport();
  }

  @Override
  public void appendDuringCompaction(final float[] vector, final Set<RID> rids) {
    if (vector == null || vector.length != getDimensions())
      throw new IllegalArgumentException("Invalid vector");
    if (rids == null || rids.isEmpty())
      throw new IllegalArgumentException("RIDs cannot be empty");

    // Store in parent's HashMap
    super.appendDuringCompaction(vector, rids);

    // Store vector and RIDs with ID for future HNSW integration
    final int id = nextId++;
    idToVectors.put(id, vector.clone());
    idToRids.put(id, rids.toArray(new RID[0]));
  }

  /**
   * Finalize HNSW index construction after all vectors added.
   *
   * <p>Phase 4.5: Builds JVector HNSW graph from collected vectors.
   * Called by compactor after K-way merge completes.
   *
   * <p>Constructs hierarchical navigable small world graph for O(log n) KNN search.
   */
  public void finalizeHNSWBuild() {
    if (idToVectors.isEmpty()) {
      return; // No vectors to build HNSW from
    }

    try {
      // Create adapter for JVector's RandomAccessVectorValues interface
      final VectorValuesAdapter vectorValues = new VectorValuesAdapter();

      // Map similarity function name to JVector enum
      final VectorSimilarityFunction similarityFunc = mapSimilarityFunction(getSimilarityFunction());

      // Build HNSW graph with configured parameters
      // GraphIndexBuilder(RandomAccessVectorValues, VectorSimilarityFunction, int M, int efConstruction, float alpha, float ml)
      // ml (level multiplier) = 1.0f / ln(2.0f) ≈ 1.4427f
      final GraphIndexBuilder builder = new GraphIndexBuilder(
          vectorValues,
          similarityFunc,
          maxM,                    // Maximum connections per node (M parameter)
          efConstruction,           // Beam width for construction
          1.2f,                     // Alpha: diversity parameter (typically 1.2)
          1.4427f                   // ml: level multiplier (1/ln(2))
      );

      // Build the graph - requires vectorValues as parameter
      this.hnswIndex = builder.build(vectorValues);

    } catch (final Exception e) {
      // If HNSW build fails, log but continue with brute-force fallback
      System.err.println("HNSW graph construction failed: " + e.getMessage());
      this.hnswIndex = null;
    }
  }

  /**
   * Find K nearest neighbors using HNSW graph or brute-force fallback.
   *
   * <p>Phase 4.5: Uses JVector HNSW for O(log n) search if graph built.
   * Falls back to O(n) brute-force if HNSW unavailable.
   *
   * @param queryVector the query vector
   * @param k           number of neighbors to return
   *
   * @return list of K nearest vectors with distances and RIDs
   */
  @Override
  public List<LSMVectorIndexMutable.VectorSearchResult> knnSearch(
      final float[] queryVector, final int k) {

    if (queryVector == null || queryVector.length != getDimensions())
      return Collections.emptyList();

    if (k <= 0)
      return Collections.emptyList();

    // Phase 4.5: Use HNSW if graph was built
    if (hnswIndex != null) {
      try {
        // Perform HNSW-accelerated search O(log n) using GraphSearcher
        final List<LSMVectorIndexMutable.VectorSearchResult> results = new ArrayList<>();

        // Wrap the query vector using official JVector API
        final VectorFloat<?> queryVectorFloat = vectorTypeSupport.createFloatVector(queryVector);

        // Create GraphSearcher and perform search
        // GraphSearcher.search() is a static method that takes multiple parameters
        final SearchResult searchResult = GraphSearcher.search(
            queryVectorFloat,         // query vector (as VectorFloat)
            k,                        // number of results
            new VectorValuesAdapter(), // vector values
            mapSimilarityFunction(getSimilarityFunction()), // similarity function
            hnswIndex,                // graph index
            new Bits.MatchAllBits()   // bits (all nodes accepted)
        );

        // Convert HNSW results (NodeScore array) to VectorSearchResult (with RIDs)
        for (final var nodeScore : searchResult.getNodes()) {
          final int nodeId = nodeScore.node;
          final float[] vector = idToVectors.get(nodeId);
          final RID[] rids = idToRids.get(nodeId);

          if (vector != null && rids != null) {
            results.add(new LSMVectorIndexMutable.VectorSearchResult(
                vector, nodeScore.score, Set.of(rids)));
          }
        }

        return results;
      } catch (final Exception e) {
        // If HNSW search fails, fall back to brute-force
        System.err.println("HNSW search failed, falling back to brute-force: " + e.getMessage());
        e.printStackTrace();
      }
    }

    // Fallback: Use brute-force KNN from parent class
    return super.knnSearch(queryVector, k);
  }

  /**
   * Search for exact vector match (inherited from parent).
   *
   * @param queryVector the query vector
   *
   * @return set of RIDs matching this vector
   */
  @Override
  public Set<RID> search(final float[] queryVector) {
    // Use parent's HashMap-based exact match
    return super.search(queryVector);
  }

  /**
   * Get total entry count.
   *
   * @return number of vectors in compacted index
   */
  @Override
  public long countEntries() {
    return super.countEntries();
  }

  /**
   * Close and cleanup resources including HNSW graph.
   */
  @Override
  public void close() {
    idToRids.clear();
    idToVectors.clear();
    if (hnswIndex != null) {
      hnswIndex = null;  // Release HNSW graph reference
    }
    super.close();
  }

  /**
   * Map similarity function string to JVector's VectorSimilarityFunction enum.
   *
   * @param similarityFunction the function name (COSINE, EUCLIDEAN, DOT_PRODUCT)
   * @return the corresponding VectorSimilarityFunction
   * @throws IllegalArgumentException if function is unknown
   */
  private VectorSimilarityFunction mapSimilarityFunction(final String similarityFunction) {
    return switch (similarityFunction) {
      case "COSINE" -> VectorSimilarityFunction.COSINE;
      case "EUCLIDEAN" -> VectorSimilarityFunction.EUCLIDEAN;
      case "DOT_PRODUCT" -> VectorSimilarityFunction.DOT_PRODUCT;
      default -> throw new IllegalArgumentException("Unknown similarity function: " + similarityFunction);
    };
  }

  /**
   * Adapter for JVector's RandomAccessVectorValues interface.
   *
   * <p>Wraps HNSW vectors map to provide JVector with vector access interface.
   */
  private class VectorValuesAdapter implements RandomAccessVectorValues {

    @Override
    public int size() {
      return idToVectors.size();
    }

    @Override
    public int dimension() {
      return getDimensions();
    }

    /**
     * Returns the vector at the given ordinal.
     * Uses JVector's official VectorTypeSupport API to create VectorFloat instances.
     *
     * @param targetOrd the ordinal (node ID)
     * @return the vector wrapped as VectorFloat
     */
    @Override
    public VectorFloat<?> getVector(final int targetOrd) {
      final float[] vector = idToVectors.get(targetOrd);

      if (vector == null) {
        // Create zero vector
        return vectorTypeSupport.createFloatVector(getDimensions());
      }

      // Wrap the float[] as VectorFloat using official API
      return vectorTypeSupport.createFloatVector(vector);
    }

    @Override
    public RandomAccessVectorValues copy() {
      // Return a copy of this adapter (stateless, so can return self or new instance)
      return new VectorValuesAdapter();
    }

    @Override
    public boolean isValueShared() {
      // Vectors are stored separately for each node, not shared
      return false;
    }
  }
}
