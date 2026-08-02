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
package com.arcadedb.schema;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.vector.VectorEncoding;
import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.serializer.json.JSONObject;

/**
 * Builder class for lsm vector indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeLSMVectorIndexBuilder extends TypeIndexBuilder {
  protected TypeLSMVectorIndexBuilder(final TypeIndexBuilder copyFrom) {
    super(copyFrom.database, copyFrom.metadata.typeName, copyFrom.metadata.propertyNames.toArray(new String[0]));

    this.metadata = new LSMVectorIndexMetadata(
        copyFrom.metadata.typeName,
        copyFrom.metadata.propertyNames.toArray(new String[0]),
        copyFrom.metadata.associatedBucketId);

    this.indexType = Schema.INDEX_TYPE.LSM_VECTOR;
    this.unique = copyFrom.unique;
    this.pageSize = copyFrom.pageSize;
    this.nullStrategy = copyFrom.nullStrategy;
    this.callback = copyFrom.callback;
    this.ignoreIfExists = copyFrom.ignoreIfExists;
    this.indexName = copyFrom.indexName;
    this.filePath = copyFrom.filePath;
    this.keyTypes = copyFrom.keyTypes;
    this.batchSize = copyFrom.batchSize;
    this.maxAttempts = copyFrom.maxAttempts;
  }

  protected TypeLSMVectorIndexBuilder(final DatabaseInternal database, final String typeName, final String[] propertyNames) {
    super(database, typeName, propertyNames);
    this.indexType = Schema.INDEX_TYPE.LSM_VECTOR;
    // Install the vector metadata here too: the superclass constructor leaves a plain IndexMetadata, and every setter
    // on this builder writes into the vector one.
    this.metadata = new LSMVectorIndexMetadata(typeName, propertyNames, -1);
  }

  /**
   * Sets the number of dimensions for the vectors.
   *
   * @param dimensions the number of dimensions
   */
  public TypeLSMVectorIndexBuilder withDimensions(final int dimensions) {
    vectorMetadata().dimensions = dimensions;
    return this;
  }

  /**
   * Sets the similarity function to use for vector comparison.
   * Supported values: COSINE, DOT_PRODUCT, EUCLIDEAN
   *
   * @param similarity the similarity function name
   */
  public TypeLSMVectorIndexBuilder withSimilarity(final String similarity) {
    vectorMetadata().setSimilarity(similarity);
    return this;
  }

  /**
   * Sets the Vamana per-layer graph degree (JVector {@code M}): the maximum number of connections kept
   * per node on every layer, base layer included. Unlike hnswlib, this value is <b>not</b> doubled at
   * the base layer, so to match an hnswlib {@code M} set {@code maxConnections = 2 * M}. Higher values
   * improve recall but increase memory usage and build time. Typical range: 16-64, default: 32.
   *
   * @param maxConnections the per-layer graph degree
   * @see LSMVectorIndexMetadata#maxConnections
   */
  public TypeLSMVectorIndexBuilder withMaxConnections(final int maxConnections) {
    vectorMetadata().setMaxConnections(maxConnections);
    return this;
  }

  /**
   * Sets the beam width for search operations.
   * Higher values improve recall but increase search time.
   * Typical range: 50-500, default: 100
   *
   * @param beamWidth the beam width
   */
  public TypeLSMVectorIndexBuilder withBeamWidth(final int beamWidth) {
    vectorMetadata().setBeamWidth(beamWidth);
    return this;
  }

  /**
   * Sets the search-time beam width: the recall/latency knob of every query that does not override it per call.
   * Higher values improve recall at the cost of latency. Typical range: 50-500, default: 100.
   *
   * @param efSearch the search beam width
   */
  public TypeLSMVectorIndexBuilder withEfSearch(final int efSearch) {
    vectorMetadata().setEfSearch(efSearch);
    return this;
  }

  /**
   * Sets how long the index waits, with pending mutations and no further write, before rebuilding its graph in the
   * background. {@code 0} disables the timer; {@code -1} (default) defers to
   * {@code arcadedb.vectorIndex.inactivityRebuildTimeoutMs}.
   *
   * @param inactivityRebuildTimeoutMs the inactivity window in milliseconds
   */
  public TypeLSMVectorIndexBuilder withInactivityRebuildTimeout(final int inactivityRebuildTimeoutMs) {
    vectorMetadata().inactivityRebuildTimeoutMs = inactivityRebuildTimeoutMs;
    return this;
  }

  /**
   * Sets the number of pending mutations that trigger a graph rebuild. {@code -1} (default) defers to the global
   * setting.
   *
   * @param mutationsBeforeRebuild the mutation threshold
   */
  public TypeLSMVectorIndexBuilder withMutationsBeforeRebuild(final int mutationsBeforeRebuild) {
    vectorMetadata().mutationsBeforeRebuild = mutationsBeforeRebuild;
    return this;
  }

  /**
   * Refuses a limit on the vector-location index, which cannot be capped.
   *
   * @param locationCacheSize the requested limit; anything positive raises
   *
   * @throws com.arcadedb.index.IndexException on any positive value
   * @deprecated since 26.8.1 (issues #5559 and #5568): the location index cannot be capped, because a vector
   * location is the only mapping from a vector id to its record and nothing on disk can rebuild an evicted one. The
   * index costs ~90 bytes per LIVE vector; size the heap for that instead. Only {@code -1}/{@code 0} ("no limit",
   * the default) are still accepted, so an existing call passing one of those keeps working.
   */
  @Deprecated
  public TypeLSMVectorIndexBuilder withLocationCacheSize(final int locationCacheSize) {
    vectorMetadata().setLocationCacheSize(locationCacheSize);
    return this;
  }

  /**
   * Sets the size of the cache used while building the graph. {@code -1} (default) defers to the global setting.
   *
   * @param graphBuildCacheSize the cache size in entries
   */
  public TypeLSMVectorIndexBuilder withGraphBuildCacheSize(final int graphBuildCacheSize) {
    vectorMetadata().graphBuildCacheSize = graphBuildCacheSize;
    return this;
  }

  /**
   * Sets whether the vectors are stored inline in the graph file rather than read from the index pages.
   *
   * @param storeVectorsInGraph true to store the vectors in the graph file
   */
  public TypeLSMVectorIndexBuilder withStoreVectorsInGraph(final boolean storeVectorsInGraph) {
    vectorMetadata().storeVectorsInGraph = storeVectorsInGraph;
    return this;
  }

  /**
   * Sets the neighbor overflow factor for graph construction.
   * This parameter controls how many extra candidate neighbors are considered during graph building.
   * Higher values can improve graph quality but increase build time.
   * Typical range: 1.0-1.5, default: 1.2
   *
   * @param neighborOverflowFactor the neighbor overflow factor
   */
  public TypeLSMVectorIndexBuilder withNeighborOverflowFactor(final float neighborOverflowFactor) {
    vectorMetadata().setNeighborOverflowFactor(neighborOverflowFactor);
    return this;
  }

  /**
   * Sets the alpha diversity relaxation factor for graph construction.
   * This parameter controls the trade-off between distance accuracy and diversity in the graph.
   * Higher values prioritize diversity, which can improve recall for complex queries.
   * Typical range: 1.0-1.5, default: 1.2
   *
   * @param alphaDiversityRelaxation the alpha diversity relaxation factor
   */
  public TypeLSMVectorIndexBuilder withAlphaDiversityRelaxation(final float alphaDiversityRelaxation) {
    vectorMetadata().setAlphaDiversityRelaxation(alphaDiversityRelaxation);
    return this;
  }

  /**
   * Sets the ID property name used to identify vertices.
   * This property is used when searching for vertices by ID.
   * Default is "id".
   *
   * @param idPropertyName the ID property name
   */
  public TypeLSMVectorIndexBuilder withIdProperty(final String idPropertyName) {
    vectorMetadata().idPropertyName = idPropertyName;
    return this;
  }

  /**
   * Sets the quantization type for vector compression.
   * NONE (default): No quantization, stores float32 vectors (4 bytes per dimension)
   * INT8: 4x compression using int8 quantization
   * BINARY: 32x compression using binary quantization
   *
   * @param quantizationType the quantization type
   */
  public TypeLSMVectorIndexBuilder withQuantization(final VectorQuantizationType quantizationType) {
    vectorMetadata().quantizationType = quantizationType;
    return this;
  }

  /**
   * Sets whether to add hierarchical layers to the HNSW graph.
   * Enabling hierarchy can improve search performance at the cost of increased index size and build time.
   * Default is true.
   *
   * @param addHierarchy true to add hierarchy, false otherwise
   */
  public TypeLSMVectorIndexBuilder withAddHierarchy(final boolean addHierarchy) {
    vectorMetadata().addHierarchy = addHierarchy;
    return this;
  }

  /**
   * Sets the quantization type for vector compression by string name.
   *
   * @param quantization the quantization type name (NONE, INT8, BINARY, PRODUCT)
   */
  public TypeLSMVectorIndexBuilder withQuantization(final String quantization) {
    vectorMetadata().setQuantization(quantization);
    return this;
  }

  /**
   * Sets the wire / storage encoding of the vector property. {@link VectorEncoding#FLOAT32} (default)
   * keeps the historical contract: documents store float32 in {@code ARRAY_OF_FLOATS} columns.
   * {@link VectorEncoding#INT8} accepts pre-quantized signed bytes (one byte per dim) end-to-end:
   * 4x smaller HTTP payloads, 4x smaller bucket storage, no client-side dequantize round trip.
   * The HNSW build/search still runs on {@code float32} internally
   * (<a href="https://github.com/datastax/jvector/issues/665">datastax/jvector#665</a>); bytes are
   * dequantized once on the read path via {@code value / 127.0f}.
   *
   * @param encoding the vector encoding
   */
  public TypeLSMVectorIndexBuilder withEncoding(final VectorEncoding encoding) {
    vectorMetadata().encoding = encoding;
    return this;
  }

  /**
   * Sets the wire / storage encoding by string name (FLOAT32, INT8). See
   * {@link #withEncoding(VectorEncoding)} for the trade-offs.
   *
   * @param encoding the encoding name
   */
  public TypeLSMVectorIndexBuilder withEncoding(final String encoding) {
    vectorMetadata().setEncoding(encoding);
    return this;
  }

  /**
   * Returns {@code metadata} narrowed to {@link LSMVectorIndexMetadata}. The constructor of this
   * builder always installs an LSMVectorIndexMetadata, but {@link #withMetadata(IndexMetadata)}
   * allows the caller to override it; the explicit guard turns "subclasser swapped a different
   * metadata in via {@code withMetadata}" into a clear error rather than a {@link ClassCastException}
   * blamed on an unrelated line.
   */
  private LSMVectorIndexMetadata vectorMetadata() {
    if (metadata instanceof LSMVectorIndexMetadata m)
      return m;
    throw new IndexException(
        "TypeLSMVectorIndexBuilder.metadata is not an LSMVectorIndexMetadata (got "
            + (metadata == null ? "null" : metadata.getClass().getSimpleName()) + ")");
  }

  /**
   * Sets the number of subspaces (M) for Product Quantization.
   * Only applicable when quantization type is PRODUCT.
   * The value must evenly divide the number of dimensions.
   * Default: min(dimensions/4, 512), adjusted to evenly divide dimensions
   *
   * @param pqSubspaces the number of subspaces (M)
   */
  public TypeLSMVectorIndexBuilder withPQSubspaces(final int pqSubspaces) {
    vectorMetadata().setPQSubspaces(pqSubspaces);
    return this;
  }

  /**
   * Sets the number of clusters per subspace (K) for Product Quantization.
   * Only applicable when quantization type is PRODUCT.
   * Typical values: 128 or 256 (for byte-sized codes)
   * Default: 256
   *
   * @param pqClusters the number of clusters per subspace (K)
   */
  public TypeLSMVectorIndexBuilder withPQClusters(final int pqClusters) {
    vectorMetadata().setPQClusters(pqClusters);
    return this;
  }

  /**
   * Sets whether to globally center vectors before PQ encoding.
   * Only applicable when quantization type is PRODUCT.
   * Global centering can improve recall by normalizing the data distribution.
   * Default: true
   *
   * @param pqCenterGlobally true to globally center vectors, false otherwise
   */
  public TypeLSMVectorIndexBuilder withPQCenterGlobally(final boolean pqCenterGlobally) {
    vectorMetadata().pqCenterGlobally = pqCenterGlobally;
    return this;
  }

  /**
   * Sets the maximum number of vectors to use for PQ training.
   * Only applicable when quantization type is PRODUCT.
   * Higher values improve codebook quality but increase training time.
   * Default: 128000 (JVector's recommended maximum)
   *
   * @param pqTrainingLimit the maximum number of training vectors
   */
  public TypeLSMVectorIndexBuilder withPQTrainingLimit(final int pqTrainingLimit) {
    vectorMetadata().setPQTrainingLimit(pqTrainingLimit);
    return this;
  }

  @Override
  public TypeLSMVectorIndexBuilder withMetadata(final IndexMetadata metadata) {
    // Same guard as the full-text and bucket-level vector builders: a raw cast here would raise the very
    // ClassCastException, blamed on an unrelated line, that vectorMetadata() exists to turn into an actionable error.
    if (metadata != null && !(metadata instanceof LSMVectorIndexMetadata))
      throw new IllegalArgumentException(
          "An LSM_VECTOR index requires LSMVectorIndexMetadata but got " + metadata.getClass().getName());
    this.metadata = metadata;
    return this;
  }

  /**
   * Configures the builder from the {@code METADATA} clause of {@code CREATE INDEX}. Unknown keys are rejected rather
   * than dropped: a typo such as {@code {"similarty": "EUCLIDEAN"}} used to yield a COSINE index and report success
   * (issue #5639).
   * <p>
   * Use {@link #withPersistedMetadata(JSONObject)} instead to restore an index from an exported definition, which
   * carries structural keys this method has no reason to accept.
   *
   * @param json the METADATA clause
   */
  public void withMetadata(final JSONObject json) {
    vectorMetadata().fromUserMetadata(json, Schema.INDEX_TYPE.LSM_VECTOR);
  }

  /**
   * Restores the builder from a PERSISTED index definition - the JSON {@code LSMVectorIndex.toJSON()} writes into
   * {@code schema.json} and the exporters copy verbatim. Unlike {@link #withMetadata(JSONObject)} this tolerates (and
   * ignores) the structural keys of such a definition: {@code type}, {@code bucket}, {@code indexName},
   * {@code typeName}, {@code properties}, {@code version} and the {@code buildState} marker.
   *
   * @param json the persisted index definition
   *
   * @return this builder for chaining
   */
  public TypeLSMVectorIndexBuilder withPersistedMetadata(final JSONObject json) {
    if (json != null)
      vectorMetadata().fromJSON(json);
    return this;
  }
}
