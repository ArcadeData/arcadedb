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
 * Builder class for bucket indexes of type lsm vector.
 * <p>
 * Every setting lives on the {@link LSMVectorIndexMetadata} this builder carries, and never on a field of its own: the
 * historical parallel field list silently dropped {@code efSearch}, {@code inactivityRebuildTimeoutMs},
 * {@code neighborOverflowFactor} and {@code alphaDiversityRelaxation} on the way from the type-level builder to the
 * index, because a setting added to the metadata had to be remembered here too (issue #5639).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BucketLSMVectorIndexBuilder extends BucketIndexBuilder {
  protected BucketLSMVectorIndexBuilder(final DatabaseInternal database, final String typeName, final String bucketName,
      final String[] propertyNames) {
    super(database, typeName, bucketName, propertyNames);
    this.indexType = Schema.INDEX_TYPE.LSM_VECTOR;
    this.metadata = new LSMVectorIndexMetadata(typeName, propertyNames, -1);
  }

  protected BucketLSMVectorIndexBuilder(final BucketIndexBuilder copyFrom) {
    super(copyFrom.database, copyFrom.typeName, copyFrom.bucketName, copyFrom.propertyNames);

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
    this.metadata = new LSMVectorIndexMetadata(copyFrom.typeName, copyFrom.propertyNames, -1);
  }

  /**
   * Returns the vector configuration this builder carries. The index factory reads every setting from here.
   */
  public LSMVectorIndexMetadata getVectorMetadata() {
    return vectorMetadata();
  }

  /**
   * Sets the number of dimensions for the vectors.
   *
   * @param dimensions the number of dimensions
   */
  public BucketLSMVectorIndexBuilder withDimensions(final int dimensions) {
    vectorMetadata().dimensions = dimensions;
    return this;
  }

  /**
   * Sets the similarity function to use for vector comparison.
   * Supported values: COSINE, DOT_PRODUCT, EUCLIDEAN
   *
   * @param similarity the similarity function name
   */
  public BucketLSMVectorIndexBuilder withSimilarity(final String similarity) {
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
  public BucketLSMVectorIndexBuilder withMaxConnections(final int maxConnections) {
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
  public BucketLSMVectorIndexBuilder withBeamWidth(final int beamWidth) {
    vectorMetadata().setBeamWidth(beamWidth);
    return this;
  }

  /**
   * Sets the search-time beam width. Higher values improve recall at the cost of latency. Default: 100.
   *
   * @param efSearch the search beam width
   */
  public BucketLSMVectorIndexBuilder withEfSearch(final int efSearch) {
    vectorMetadata().setEfSearch(efSearch);
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
  public BucketLSMVectorIndexBuilder withNeighborOverflowFactor(final float neighborOverflowFactor) {
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
  public BucketLSMVectorIndexBuilder withAlphaDiversityRelaxation(final float alphaDiversityRelaxation) {
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
  public BucketLSMVectorIndexBuilder withIdProperty(final String idPropertyName) {
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
  public BucketLSMVectorIndexBuilder withQuantization(final VectorQuantizationType quantizationType) {
    vectorMetadata().quantizationType = quantizationType;
    return this;
  }

  /**
   * Sets the quantization type for vector compression by string name.
   *
   * @param quantization the quantization type name (NONE, INT8, BINARY, PRODUCT)
   */
  public BucketLSMVectorIndexBuilder withQuantization(final String quantization) {
    vectorMetadata().setQuantization(quantization);
    return this;
  }

  /**
   * Sets the wire / storage encoding of the vector property. See
   * {@link TypeLSMVectorIndexBuilder#withEncoding(VectorEncoding)} for the trade-offs.
   *
   * @param encoding the vector encoding
   */
  public BucketLSMVectorIndexBuilder withEncoding(final VectorEncoding encoding) {
    vectorMetadata().encoding = encoding;
    return this;
  }

  /**
   * Sets the wire / storage encoding by string name (FLOAT32, INT8).
   *
   * @param encoding the encoding name
   */
  public BucketLSMVectorIndexBuilder withEncoding(final String encoding) {
    vectorMetadata().setEncoding(encoding);
    return this;
  }

  /**
   * Sets the number of subspaces (M) for Product Quantization.
   * Only applicable when quantization type is PRODUCT.
   * The value must evenly divide the number of dimensions.
   * Default: min(dimensions/4, 512), adjusted to evenly divide dimensions
   *
   * @param pqSubspaces the number of subspaces (M)
   */
  public BucketLSMVectorIndexBuilder withPQSubspaces(final int pqSubspaces) {
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
  public BucketLSMVectorIndexBuilder withPQClusters(final int pqClusters) {
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
  public BucketLSMVectorIndexBuilder withPQCenterGlobally(final boolean pqCenterGlobally) {
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
  public BucketLSMVectorIndexBuilder withPQTrainingLimit(final int pqTrainingLimit) {
    vectorMetadata().setPQTrainingLimit(pqTrainingLimit);
    return this;
  }

  @Override
  public BucketLSMVectorIndexBuilder withMetadata(final IndexMetadata metadata) {
    if (metadata instanceof LSMVectorIndexMetadata) {
      // Adopt the type-level instance instead of copying it field by field: that copy is what dropped four settings
      // on the floor (issue #5639). The index factory takes a copy() so the per-bucket index does not share the
      // type-level instance's per-index runtime state.
      super.withMetadata(metadata);
    } else if (metadata != null) {
      // A plain metadata carries no vector setting, only the shared bits: keep this builder's configuration and graft
      // those on, which is what the historical field-by-field copy did for a non-vector metadata.
      final LSMVectorIndexMetadata vectorMetadata = vectorMetadata();
      vectorMetadata.typeName = metadata.typeName;
      vectorMetadata.propertyNames = metadata.propertyNames;
      vectorMetadata.associatedBucketId = metadata.associatedBucketId;
      vectorMetadata.collations = metadata.collations;
      vectorMetadata.typeIndexName = metadata.typeIndexName;
    }
    return this;
  }

  /**
   * Configures the builder from the {@code METADATA} clause of {@code CREATE INDEX}. Unknown keys are rejected rather
   * than dropped (issue #5639).
   *
   * @param metadata the METADATA clause
   */
  public void withMetadata(final JSONObject metadata) {
    vectorMetadata().fromUserMetadata(metadata, Schema.INDEX_TYPE.LSM_VECTOR);
  }

  /**
   * Returns {@code metadata} narrowed to {@link LSMVectorIndexMetadata}. Both constructors install one, so the guard
   * only turns a metadata swapped in from outside into a clear error rather than a {@link ClassCastException} blamed on
   * an unrelated line.
   */
  private LSMVectorIndexMetadata vectorMetadata() {
    if (metadata instanceof LSMVectorIndexMetadata m)
      return m;
    throw new IndexException("BucketLSMVectorIndexBuilder.metadata is not an LSMVectorIndexMetadata (got "
        + (metadata == null ? "null" : metadata.getClass().getSimpleName()) + ")");
  }
}
