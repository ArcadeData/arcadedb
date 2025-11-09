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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.LSMVectorIndexBuilder;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSMVectorIndex - Public interface and coordinator for LSM Vector indexes.
 *
 * <p>Manages the lifecycle and operations of vector indexes using LSM-Tree architecture.
 *
 * @author Arcade Data
 * @since 24.12.0
 */
public class LSMVectorIndex implements Index, IndexInternal, RangeIndex {

  /**
   * Callback to ignore/filter certain results during vector search.
   */
  public interface IgnoreVertexCallback {
    /**
     * Determine if a vertex should be ignored during search.
     *
     * @param rid the record ID of the candidate result
     *
     * @return true if this result should be ignored, false to include it
     */
    boolean ignoreVertex(RID rid);
  }

  public enum STATUS {
    AVAILABLE, COMPACTION_SCHEDULED, COMPACTION_IN_PROGRESS
  }

  public static class IndexFactoryHandler implements com.arcadedb.index.IndexFactoryHandler {
    @Override
    public IndexInternal create(final IndexBuilder builder) {
      final LSMVectorIndexBuilder vectorBuilder = (LSMVectorIndexBuilder) builder;
      return new LSMVectorIndex(
          vectorBuilder.getDatabase(),        // database
          vectorBuilder.getTypeName(),        // docType (was getIndexName - FIXED)
          vectorBuilder.getPropertyName(),    // property (was getFilePath - FIXED)
          vectorBuilder.getIndexName(),       // name (was getTypeName - FIXED)
          vectorBuilder.getFilePath(),        // filePath (was getPropertyName - FIXED)
          ComponentFile.MODE.READ_WRITE,      // mode
          vectorBuilder.getDimensions(),      // dimensions
          vectorBuilder.getSimilarityFunction(),  // similarityFunction
          vectorBuilder.getMaxConnections(),  // maxConnections
          vectorBuilder.getBeamWidth(),       // beamWidth
          vectorBuilder.getAlpha(),           // alpha
          vectorBuilder.getNullStrategy());   // nullStrategy
    }
  }

  private final String           name;
  private final int              dimensions;
  private final String           similarityFunction;
  private final int              maxConnections;
  private final int              beamWidth;
  private final float            alpha;
  private final DatabaseInternal database;
  private       String           docType;
  private       String           vectorPropertyName;

  // Components
  protected LSMVectorIndexMutable   mutableIndex;
  private   LSMVectorIndexCompacted compactedIndex;

  // Status
  private volatile STATUS                 status = STATUS.AVAILABLE;
  private final    ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  /**
   * Creates a new LSM Vector Index.
   */
  public LSMVectorIndex(final DatabaseInternal database, final String docType, final String property, final String name,
      final String filePath,
      final ComponentFile.MODE mode, final int dimensions, final String similarityFunction,
      final int maxConnections, final int beamWidth, final float alpha,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    try {
      this.name = name;
      this.docType = docType;
      this.vectorPropertyName = property;
      this.database = database;
      this.dimensions = dimensions;
      this.similarityFunction = similarityFunction;
      this.maxConnections = maxConnections;
      this.beamWidth = beamWidth;
      this.alpha = alpha;

      // Initialize mutable component
      this.mutableIndex = new LSMVectorIndexMutable(
          this, database, name, filePath, mode, LSMVectorIndexMutable.DEFAULT_PAGE_SIZE,
          dimensions, similarityFunction, maxConnections, beamWidth, alpha, nullStrategy);
    } catch (final IOException e) {
      throw new RuntimeException("Error creating LSM vector index '" + name + "'", e);
    }
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    rwLock.readLock().lock();
    try {
      if (mutableIndex == null)
        throw new IllegalStateException("Index not initialized");

      if (keys == null || keys.length == 0)
        return new EmptyIndexCursor();

      final Object key = keys[0];
      if (!(key instanceof float[]))
        throw new IllegalArgumentException("Vector key must be float[]");

      final float[] vector = (float[]) key;

      // Search mutable first, then compacted
      final Set<RID> rids = new HashSet<>();
      rids.addAll(mutableIndex.search(vector));

      if (compactedIndex != null) {
        rids.addAll(compactedIndex.search(vector));
      }

      return rids.isEmpty() ? new EmptyIndexCursor() : new SimpleIndexCursor(rids);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    return get(keys);
  }

  @Override
  public void put(final Object[] keys, final RID[] rids) {
    rwLock.readLock().lock();
    try {
      if (mutableIndex == null)
        throw new IllegalStateException("Index not initialized");

      if (keys == null || keys.length == 0)
        throw new IllegalArgumentException("Vector key required");

      final Object key = keys[0];
      if (!(key instanceof float[]))
        throw new IllegalArgumentException("Vector key must be float[]");

      final float[] vector = (float[]) key;

      // Insert into mutable for each RID
      if (rids != null) {
        for (final RID rid : rids) {
          mutableIndex.put(vector, rid);
        }
      }

      // Check if compaction should be scheduled
      if (mutableIndex.shouldScheduleCompaction()) {
        scheduleCompaction();
      }
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public void remove(final Object[] keys) {
    if (keys == null || keys.length == 0)
      return;

    final Object key = keys[0];
    if (!(key instanceof float[]))
      return;

    final float[] vector = (float[]) key;

    rwLock.readLock().lock();
    try {
      if (mutableIndex != null) {
        // Phase 3: Mark for deletion, cleanup during compaction
        // For now, we don't support removal from vector index
      }
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    // Phase 3: Implement removal
  }

  @Override
  public long countEntries() {
    rwLock.readLock().lock();
    try {
      long count = 0;
      if (mutableIndex != null)
        count += mutableIndex.countEntries();
      if (compactedIndex != null)
        count += compactedIndex.countEntries();
      return count;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.LSM_VECTOR;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getTypeName() {
    return "LSM_VECTOR";
  }

  @Override
  public List<String> getPropertyNames() {
    return Collections.emptyList();
  }

  @Override
  public int getAssociatedBucketId() {
    return -1;
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return LSMTreeIndexAbstract.NULL_STRATEGY.SKIP;
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    // Vector indexes don't support null values
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean isAutomatic() {
    return false;
  }

  @Override
  public void updateTypeName(final String newName) {
    // Vector indexes don't support type name updates
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("name", name);
    json.put("type", "LSM_VECTOR");
    json.put("dimensions", dimensions);
    json.put("similarity", similarityFunction);
    json.put("maxConnections", maxConnections);
    json.put("beamWidth", beamWidth);
    json.put("alpha", alpha);
    return json;
  }

  @Override
  public boolean isValid() {
    return mutableIndex != null;
  }

  @Override
  public boolean isCompacting() {
    return status == STATUS.COMPACTION_IN_PROGRESS;
  }

  @Override
  public boolean scheduleCompaction() {
    rwLock.writeLock().lock();
    try {
      if (status == STATUS.AVAILABLE) {
        status = STATUS.COMPACTION_SCHEDULED;
        return true;
      }
      return false;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public Map<String, Long> getStats() {
    return Collections.emptyMap();
  }

  @Override
  public int getFileId() {
    return 0;
  }

  @Override
  public Component getComponent() {
    return null;
  }

  @Override
  public Type[] getKeyTypes() {
    return new Type[0];
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return new byte[0];
  }

  @Override
  public List<Integer> getFileIds() {
    return new ArrayList<>();
  }

  @Override
  public TypeIndex getTypeIndex() {
    return null;
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    // Vector indexes don't use TypeIndex
  }

  @Override
  public void drop() {
    close();
  }

  @Override
  public void close() {
    rwLock.writeLock().lock();
    try {
      if (mutableIndex != null) {
        mutableIndex.close();
        mutableIndex = null;
      }
      if (compactedIndex != null) {
        compactedIndex.close();
        compactedIndex = null;
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  public boolean setStatus(final IndexInternal.INDEX_STATUS[] expectedStatuses,
      final IndexInternal.INDEX_STATUS newStatus) {
    // Vector indexes don't use status management from IndexInternal
    return false;
  }

  @Override
  public void setMetadata(final String name, final String[] propertyNames, final int associatedBucketId) {
    // Vector indexes don't use metadata management from IndexInternal
  }

  @Override
  public long build(final int buildIndexBatchSize, final Index.BuildIndexCallback callback) {
    // Vector indexes are built incrementally via put() operations
    return 0;
  }

  @Override
  public int getPageSize() {
    return LSMVectorIndexMutable.DEFAULT_PAGE_SIZE;
  }

  @Override
  public String getMostRecentFileName() {
    return name;
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return this;
  }

  @Override
  public IndexCursor range(final boolean fromInclusive, final Object[] fromKeys,
      final boolean toInclusive, final Object[] toKeys, final boolean ascendingOrder) {
    // Range queries not supported for vector indexes
    return new EmptyIndexCursor();
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] keys, final boolean inclusive) {
    // Iterator queries not supported for vector indexes
    return new EmptyIndexCursor();
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) {
    // Iterator queries not supported for vector indexes
    return new EmptyIndexCursor();
  }

  /**
   * Execute compaction (K-way merge).
   */
  @Override
  public boolean compact() throws IOException, InterruptedException {
    rwLock.writeLock().lock();
    try {
      if (status != STATUS.COMPACTION_SCHEDULED)
        return false;

      status = STATUS.COMPACTION_IN_PROGRESS;

      try {
        // Execute K-way merge compaction
        final LSMVectorIndexCompactor compactor = new LSMVectorIndexCompactor(
            this,
            null, // database will be obtained from mutableIndex if needed
            name,
            "", // filePath - can be derived from mutableIndex if needed
            dimensions,
            similarityFunction,
            maxConnections,
            beamWidth,
            alpha,
            LSMTreeIndexAbstract.NULL_STRATEGY.SKIP);

        return compactor.executeCompaction();
      } finally {
        status = STATUS.AVAILABLE;
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  public STATUS getStatus() {
    return status;
  }

  public LSMVectorIndexMutable getMutableIndex() {
    return mutableIndex;
  }

  public void setMutableIndex(final LSMVectorIndexMutable newMutableIndex) {
    this.mutableIndex = newMutableIndex;
  }

  public LSMVectorIndexCompacted getCompactedIndex() {
    return compactedIndex;
  }

  public void setCompactedIndex(final LSMVectorIndexCompacted newCompactedIndex) {
    this.compactedIndex = newCompactedIndex;
  }

  /**
   * Find K nearest neighbors to the query vector.
   *
   * <p>Performs hybrid search across mutable and compacted components:
   * - Mutable: Brute-force O(n) search on fresh writes
   * - Compacted: HNSW O(log n) search on compacted data
   *
   * <p>Results from both components are merged and sorted by distance,
   * returning the top K nearest neighbors.
   *
   * @param queryVector the query vector embedding
   * @param k           number of nearest neighbors to return
   *
   * @return list of K nearest vectors with their distances and RIDs
   */
  public List<LSMVectorIndexMutable.VectorSearchResult> knnSearch(
      final float[] queryVector, final int k) {
    return knnSearch(queryVector, k, null);
  }

  /**
   * Find K nearest neighbors to the query vector with optional filtering.
   *
   * <p>Performs hybrid search across mutable and compacted components with
   * integrated filtering during search (Option B implementation).
   *
   * @param queryVector    the query vector embedding
   * @param k              number of nearest neighbors to return
   * @param ignoreCallback optional callback to filter results during search
   *
   * @return list of K nearest vectors with their distances and RIDs
   */
  public List<LSMVectorIndexMutable.VectorSearchResult> knnSearch(
      final float[] queryVector, final int k, final IgnoreVertexCallback ignoreCallback) {

    rwLock.readLock().lock();
    try {
      if (queryVector == null || queryVector.length != dimensions)
        throw new IllegalArgumentException("Invalid query vector");

      if (k <= 0)
        throw new IllegalArgumentException("k must be positive");

      final List<LSMVectorIndexMutable.VectorSearchResult> results = new ArrayList<>();

      // Search mutable component (brute force) with optional filtering
      if (mutableIndex != null) {
        results.addAll(mutableIndex.knnSearch(queryVector, k, ignoreCallback));
      }

      // Search compacted component (HNSW accelerated if available) with optional filtering
      if (compactedIndex != null) {
        results.addAll(compactedIndex.knnSearch(queryVector, k, ignoreCallback));
      }

      // If no results found
      if (results.isEmpty())
        return Collections.emptyList();

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
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Find K nearest neighbors from the given query vector.
   *
   * <p>Convenience method that returns results in Pair format (RID, distance)
   * compatible with HnswVectorIndex API.
   *
   * @param vector the query vector embedding
   * @param k      number of nearest neighbors to return
   *
   * @return list of (RID, distance) pairs
   */
  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromVector(final float[] vector, final int k) {
    return findNeighborsFromVector(vector, k, null);
  }

  /**
   * Find K nearest neighbors from the given query vector with optional filtering.
   *
   * <p>Convenience method that returns results in Pair format (RID, distance)
   * compatible with HnswVectorIndex API. Implements Option B with integrated filtering.
   *
   * @param vector         the query vector embedding
   * @param k              number of nearest neighbors to return
   * @param ignoreCallback optional callback to filter results during search
   *
   * @return list of (RID, distance) pairs
   */
  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromVector(final float[] vector, final int k,
      final IgnoreVertexCallback ignoreCallback) {
    rwLock.readLock().lock();
    try {
      final List<LSMVectorIndexMutable.VectorSearchResult> searchResults = knnSearch(vector, k, ignoreCallback);

      if (searchResults.isEmpty())
        return Collections.emptyList();

      // Convert VectorSearchResult to Pair<RID, distance>
      // Each result may have multiple RIDs, so we create one pair per RID
      final List<Pair<Identifiable, ? extends Number>> results = new ArrayList<>();
      for (final LSMVectorIndexMutable.VectorSearchResult result : searchResults) {
        for (final RID rid : result.rids) {
          results.add(new Pair<>(rid, result.distance));
        }
        // Stop if we have enough results
        if (results.size() >= k)
          break;
      }

      // Return top K pairs
      return results.subList(0, Math.min(k, results.size()));
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Find K nearest neighbors from a record ID with optional filtering.
   *
   * <p>Looks up the record by ID, extracts the vector property,
   * and searches for similar vectors with integrated filtering.
   * Compatible with HnswVectorIndex API.
   *
   * @param id             the record ID value (matches idPropertyName)
   * @param k              number of nearest neighbors to return
   * @param ignoreCallback optional callback to filter results during search
   *
   * @return list of (RID, distance) pairs
   */
  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromId(final Identifiable id, final int k,
      final IgnoreVertexCallback ignoreCallback) {
    if (id == null)
      return Collections.emptyList();

    rwLock.readLock().lock();
    try {
      // Query the database to find the record with the matching ID property
      final String sql = "SELECT * FROM " + docType + " WHERE @rid = ?";
      final ResultSet resultSet = database.query("sql", sql, id.toString());

      if (!resultSet.hasNext()) {
        return Collections.emptyList();
      }

      final Result row = resultSet.next();
      final Vertex vertex = row.getVertex().orElse(null);
      if (vertex == null) {
        return Collections.emptyList();
      }

      // Extract vector from the vertex
      final Object vectorObj = vertex.get(vectorPropertyName);
      if (vectorObj == null) {
        return Collections.emptyList();
      }

      // Convert to float[] if needed
      final float[] vector;
      if (vectorObj instanceof float[]) {
        vector = (float[]) vectorObj;
      } else if (vectorObj instanceof Float[]) {
        final Float[] floatArray = (Float[]) vectorObj;
        vector = new float[floatArray.length];
        for (int i = 0; i < floatArray.length; i++) {
          vector[i] = floatArray[i] != null ? floatArray[i] : 0f;
        }
      } else {
        return Collections.emptyList();
      }

      // Search for neighbors
      return findNeighborsFromVector(vector, k, ignoreCallback);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Find K nearest neighbors from a vertex.
   *
   * <p>Extracts the vector from the vertex and searches for similar vectors.
   * Compatible with HnswVectorIndex API.
   *
   * @param vertex the source vertex
   * @param k      number of nearest neighbors to return
   *
   * @return list of (RID, distance) pairs
   */
  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromVertex(final Vertex vertex, final int k) {
    return findNeighborsFromVertex(vertex, k, null);
  }

  /**
   * Find K nearest neighbors from a vertex with optional filtering.
   *
   * <p>Extracts the vector from the vertex and searches for similar vectors
   * with integrated filtering. Compatible with HnswVectorIndex API.
   *
   * @param vertex         the source vertex
   * @param k              number of nearest neighbors to return
   * @param ignoreCallback optional callback to filter results during search
   *
   * @return list of (RID, distance) pairs
   */
  public List<Pair<Identifiable, ? extends Number>> findNeighborsFromVertex(final Vertex vertex, final int k,
      final IgnoreVertexCallback ignoreCallback) {
    if (vertex == null)
      return Collections.emptyList();

    rwLock.readLock().lock();
    try {
      // Extract vector from the vertex
      final Object vectorObj = vertex.get(vectorPropertyName);
      if (vectorObj == null) {
        return Collections.emptyList();
      }

      // Convert to float[] if needed
      final float[] vector;
      if (vectorObj instanceof float[]) {
        vector = (float[]) vectorObj;
      } else if (vectorObj instanceof Float[]) {
        final Float[] floatArray = (Float[]) vectorObj;
        vector = new float[floatArray.length];
        for (int i = 0; i < floatArray.length; i++) {
          vector[i] = floatArray[i] != null ? floatArray[i] : 0f;
        }
      } else {
        return Collections.emptyList();
      }

      // Search for neighbors
      return findNeighborsFromVector(vector, k, ignoreCallback);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  // Empty IndexCursor implementation
  private static class EmptyIndexCursor implements IndexCursor {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Identifiable next() {
      throw new IllegalStateException("No more records");
    }

    @Override
    public byte[] getBinaryKeyTypes() {
      return new byte[0];
    }

    @Override
    public BinaryComparator getComparator() {
      return null;
    }

    @Override
    public Object[] getKeys() {
      return new Object[0];
    }

    @Override
    public Identifiable getRecord() {
      return null;
    }

    @Override
    public long estimateSize() {
      return 0;
    }

    @Override
    public java.util.Iterator<Identifiable> iterator() {
      return this;
    }
  }

  // Simple IndexCursor implementation
  private static class SimpleIndexCursor implements IndexCursor {
    private final Set<RID>     rids;
    private       Identifiable currentRecord;

    SimpleIndexCursor(final Set<RID> rids) {
      this.rids = rids;
    }

    @Override
    public boolean hasNext() {
      return !rids.isEmpty();
    }

    @Override
    public Identifiable next() {
      if (rids.isEmpty())
        throw new IllegalStateException("No more records");
      final RID rid = rids.iterator().next();
      rids.remove(rid);
      this.currentRecord = rid;
      return rid;
    }

    @Override
    public byte[] getBinaryKeyTypes() {
      return new byte[0];
    }

    @Override
    public BinaryComparator getComparator() {
      return null;
    }

    @Override
    public Object[] getKeys() {
      return new Object[0];
    }

    @Override
    public Identifiable getRecord() {
      return currentRecord;
    }

    @Override
    public long estimateSize() {
      return rids.size();
    }

    @Override
    public java.util.Iterator<Identifiable> iterator() {
      return this;
    }
  }
}
