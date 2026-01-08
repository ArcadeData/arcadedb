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
import com.arcadedb.index.vector.VectorQuantizationType;
import com.arcadedb.serializer.json.JSONObject;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

/**
 * Builder class for lsm vector indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeLSMVectorIndexBuilder extends TypeIndexBuilder {
  protected TypeLSMVectorIndexBuilder(final TypeIndexBuilder copyFrom) {
    super(copyFrom.database, copyFrom.metadata.typeName, copyFrom.metadata.propertyNames.toArray(new String[0]));

    this.metadata = new LSMVectorIndexMetadata(copyFrom.metadata.typeName, copyFrom.metadata.propertyNames.toArray(new String[0]),
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
  }

  /**
   * Sets the number of dimensions for the vectors.
   *
   * @param dimensions the number of dimensions
   */
  public TypeLSMVectorIndexBuilder withDimensions(final int dimensions) {
    ((LSMVectorIndexMetadata) metadata).dimensions = dimensions;
    return this;
  }

  /**
   * Sets the similarity function to use for vector comparison.
   * Supported values: COSINE, DOT_PRODUCT, EUCLIDEAN
   *
   * @param similarity the similarity function name
   */
  public TypeLSMVectorIndexBuilder withSimilarity(final String similarity) {
    try {
      ((LSMVectorIndexMetadata) metadata).similarityFunction = VectorSimilarityFunction.valueOf(similarity.toUpperCase());
      return this;
    } catch (final IllegalArgumentException e) {
      throw new IndexException("Invalid similarity function: " + similarity + ". Supported values: COSINE, DOT_PRODUCT, EUCLIDEAN");
    }
  }

  /**
   * Sets the maximum number of connections per node in the HNSW graph.
   * Higher values improve recall but increase memory usage and build time.
   * Typical range: 8-64, default: 16
   *
   * @param maxConnections the maximum number of connections
   */
  public TypeLSMVectorIndexBuilder withMaxConnections(final int maxConnections) {
    if (maxConnections < 1)
      throw new IllegalArgumentException("maxConnections must be at least 1");
    ((LSMVectorIndexMetadata) metadata).maxConnections = maxConnections;
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
    if (beamWidth < 1)
      throw new IllegalArgumentException("beamWidth must be at least 1");
    ((LSMVectorIndexMetadata) metadata).beamWidth = beamWidth;
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
    if (neighborOverflowFactor < 1.0f)
      throw new IllegalArgumentException("neighborOverflowFactor must be at least 1.0");
    ((LSMVectorIndexMetadata) metadata).neighborOverflowFactor = neighborOverflowFactor;
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
    if (alphaDiversityRelaxation < 1.0f)
      throw new IllegalArgumentException("alphaDiversityRelaxation must be at least 1.0");
    ((LSMVectorIndexMetadata) metadata).alphaDiversityRelaxation = alphaDiversityRelaxation;
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
    ((LSMVectorIndexMetadata) metadata).idPropertyName = idPropertyName;
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
    ((LSMVectorIndexMetadata) metadata).quantizationType = quantizationType;
    return this;
  }

  /**
   * Sets the quantization type for vector compression by string name.
   *
   * @param quantization the quantization type name (NONE, INT8, BINARY)
   */
  public TypeLSMVectorIndexBuilder withQuantization(final String quantization) {
    try {
      ((LSMVectorIndexMetadata) metadata).quantizationType = VectorQuantizationType.valueOf(quantization.toUpperCase());
      return this;
    } catch (final IllegalArgumentException e) {
      throw new IndexException("Invalid quantization type: " + quantization + ". Supported values: NONE, INT8, BINARY");
    }
  }

  @Override
  public TypeLSMVectorIndexBuilder withMetadata(IndexMetadata metadata) {
    this.metadata = (LSMVectorIndexMetadata) metadata;
    return this;
  }

  public void withMetadata(final JSONObject json) {
    final LSMVectorIndexMetadata v = ((LSMVectorIndexMetadata) metadata);

    if (json.has("dimensions"))
      v.dimensions = json.getInt("dimensions");

    if (json.has("similarity"))
      withSimilarity(json.getString("similarity"));

    if (json.has("quantization"))
      withQuantization(json.getString("quantization"));

    if (json.has("maxConnections"))
      v.maxConnections = json.getInt("maxConnections");

    if (json.has("beamWidth"))
      v.beamWidth = json.getInt("beamWidth");

    if (json.has("neighborOverflowFactor"))
      v.neighborOverflowFactor = ((Number) json.get("neighborOverflowFactor")).floatValue();

    if (json.has("alphaDiversityRelaxation"))
      v.alphaDiversityRelaxation = ((Number) json.get("alphaDiversityRelaxation")).floatValue();

    if (json.has("idPropertyName"))
      v.idPropertyName = json.getString("idPropertyName");

    // Phase 2: New configuration options
    if (json.has("locationCacheSize"))
      v.locationCacheSize = json.getInt("locationCacheSize");

    if (json.has("graphBuildCacheSize"))
      v.graphBuildCacheSize = json.getInt("graphBuildCacheSize");

    if (json.has("mutationsBeforeRebuild"))
      v.mutationsBeforeRebuild = json.getInt("mutationsBeforeRebuild");

    if (json.has("storeVectorsInGraph"))
      v.storeVectorsInGraph = json.getBoolean("storeVectorsInGraph");
  }
}
