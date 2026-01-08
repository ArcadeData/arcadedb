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
 * Builder class for bucket indexes of type lsm vector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BucketLSMVectorIndexBuilder extends BucketIndexBuilder {
  public int                      dimensions;
  public VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.COSINE;
  public VectorQuantizationType   quantizationType   = VectorQuantizationType.NONE;
  public int                      maxConnections     = 16;
  public int                      beamWidth          = 100;
  public float                    neighborOverflowFactor = 1.2f;
  public float                    alphaDiversityRelaxation = 1.2f;
  public String                   idPropertyName     = "id";
  public int                      locationCacheSize  = -1;  // -1 = use global default
  public int                      graphBuildCacheSize = -1; // -1 = use global default
  public int                      mutationsBeforeRebuild = -1; // -1 = use global default
  public boolean                  storeVectorsInGraph = false; // Phase 2: Store vectors inline in graph file

  protected BucketLSMVectorIndexBuilder(DatabaseInternal database, String typeName, String bucketName,
      String[] propertyNames) {
    super(database, typeName, bucketName, propertyNames);
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
  }

  /**
   * Sets the number of dimensions for the vectors.
   *
   * @param dimensions the number of dimensions
   */
  public BucketLSMVectorIndexBuilder withDimensions(final int dimensions) {
    this.dimensions = dimensions;
    return this;
  }

  /**
   * Sets the similarity function to use for vector comparison.
   * Supported values: COSINE, DOT_PRODUCT, EUCLIDEAN
   *
   * @param similarity the similarity function name
   */
  public BucketLSMVectorIndexBuilder withSimilarity(final String similarity) {
    try {
      this.similarityFunction = VectorSimilarityFunction.valueOf(similarity.toUpperCase());
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
  public BucketLSMVectorIndexBuilder withMaxConnections(final int maxConnections) {
    if (maxConnections < 1)
      throw new IllegalArgumentException("maxConnections must be at least 1");
    this.maxConnections = maxConnections;
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
    if (beamWidth < 1)
      throw new IllegalArgumentException("beamWidth must be at least 1");
    this.beamWidth = beamWidth;
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
    if (neighborOverflowFactor < 1.0f)
      throw new IllegalArgumentException("neighborOverflowFactor must be at least 1.0");
    this.neighborOverflowFactor = neighborOverflowFactor;
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
    if (alphaDiversityRelaxation < 1.0f)
      throw new IllegalArgumentException("alphaDiversityRelaxation must be at least 1.0");
    this.alphaDiversityRelaxation = alphaDiversityRelaxation;
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
    this.idPropertyName = idPropertyName;
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
    this.quantizationType = quantizationType;
    return this;
  }

  /**
   * Sets the quantization type for vector compression by string name.
   *
   * @param quantization the quantization type name (NONE, INT8, BINARY)
   */
  public BucketLSMVectorIndexBuilder withQuantization(final String quantization) {
    try {
      this.quantizationType = VectorQuantizationType.valueOf(quantization.toUpperCase());
      return this;
    } catch (final IllegalArgumentException e) {
      throw new IndexException("Invalid quantization type: " + quantization + ". Supported values: NONE, INT8, BINARY");
    }
  }

  @Override
  public BucketLSMVectorIndexBuilder withMetadata(final IndexMetadata metadata) {
    if (metadata instanceof LSMVectorIndexMetadata v) {
      this.dimensions = v.dimensions;
      withSimilarity(v.similarityFunction.name());
      this.quantizationType = v.quantizationType;
      this.maxConnections = v.maxConnections;
      this.beamWidth = v.beamWidth;
      this.neighborOverflowFactor = v.neighborOverflowFactor;
      this.alphaDiversityRelaxation = v.alphaDiversityRelaxation;
      this.idPropertyName = v.idPropertyName;
      // Phase 2: New configuration options
      this.locationCacheSize = v.locationCacheSize;
      this.graphBuildCacheSize = v.graphBuildCacheSize;
      this.mutationsBeforeRebuild = v.mutationsBeforeRebuild;
      this.storeVectorsInGraph = v.storeVectorsInGraph;
    }
    return this;
  }

  public void withMetadata(final JSONObject metadata) {
    if (metadata.has("dimensions"))
      this.dimensions = metadata.getInt("dimensions");

    if (metadata.has("similarity"))
      withSimilarity(metadata.getString("similarity"));

    if (metadata.has("quantization"))
      withQuantization(metadata.getString("quantization"));

    if (metadata.has("maxConnections"))
      this.maxConnections = metadata.getInt("maxConnections");

    if (metadata.has("beamWidth"))
      this.beamWidth = metadata.getInt("beamWidth");

    if (metadata.has("neighborOverflowFactor"))
      this.neighborOverflowFactor = ((Number) metadata.get("neighborOverflowFactor")).floatValue();

    if (metadata.has("alphaDiversityRelaxation"))
      this.alphaDiversityRelaxation = ((Number) metadata.get("alphaDiversityRelaxation")).floatValue();

    if (metadata.has("idPropertyName"))
      this.idPropertyName = metadata.getString("idPropertyName");

    // Phase 2: New configuration options
    if (metadata.has("locationCacheSize"))
      this.locationCacheSize = metadata.getInt("locationCacheSize");

    if (metadata.has("graphBuildCacheSize"))
      this.graphBuildCacheSize = metadata.getInt("graphBuildCacheSize");

    if (metadata.has("mutationsBeforeRebuild"))
      this.mutationsBeforeRebuild = metadata.getInt("mutationsBeforeRebuild");

    if (metadata.has("storeVectorsInGraph"))
      this.storeVectorsInGraph = metadata.getBoolean("storeVectorsInGraph");
  }
}
