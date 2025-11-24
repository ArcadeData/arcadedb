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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.io.*;
import java.util.*;

/**
 * Builder class for LSM-based vector indexes using JVector.
 * Creates one index instance per bucket, following the same pattern as LSMTreeIndex.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexBuilder extends TypeIndexBuilder {
  private       int                      dimensions;
  private       VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.COSINE;
  private       int                      maxConnections     = 16;
  private       int                      beamWidth          = 100;
  private       String                   idPropertyName     = "id";

  public LSMVectorIndexBuilder(final DatabaseInternal database, final String typeName, final String[] propertyNames) {
    super(database, typeName, propertyNames);
    this.indexType = Schema.INDEX_TYPE.LSM_VECTOR;
    // Generate default index name from type and properties
    this.indexName = typeName + "[" + String.join(",", propertyNames) + "]";
  }

  /**
   * Sets the index name.
   *
   * @param indexName the index name
   *
   * @return this builder
   */
  public LSMVectorIndexBuilder withIndexName(final String indexName) {
    this.indexName = indexName;
    return this;
  }

  @Override
  public LSMVectorIndexBuilder withFilePath(String path) {
    super.withFilePath(path);
    return this;
  }

  public TypeIndex create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (database.isAsyncProcessing())
      throw new NeedRetryException("Cannot create a new index while asynchronous tasks are running");

    if (indexName == null || indexName.isEmpty())
      throw new IndexException("Index name is required");

    if (typeName == null || typeName.isEmpty())
      throw new IndexException("Type name is required");

    if (propertyNames == null || propertyNames.length == 0)
      throw new IndexException("Property names are required");

    if (dimensions <= 0)
      throw new IndexException("Dimensions must be greater than 0");

    final LocalSchema schema = database.getSchema().getEmbedded();
    final LocalDocumentType type = schema.getType(typeName);

    // Check if index already exists
    if (ignoreIfExists) {
      final TypeIndex existingTypeIndex = type.getPolymorphicIndexByProperties(Arrays.asList(propertyNames));
      if (existingTypeIndex != null)
        return existingTypeIndex;
    }

    // Create one LSMVectorIndex per bucket (like LSMTreeIndex does)
    final List<Bucket> buckets = type.getBuckets(true);
    final Index[] indexes = new Index[buckets.size()];

    try {
      schema.recordFileChanges(() -> {
        for (int idx = 0; idx < buckets.size(); ++idx) {
          final int finalIdx = idx;
          database.transaction(() -> {
            final LocalBucket bucket = (LocalBucket) buckets.get(finalIdx);

            // Create unique index name for this bucket
            final String bucketIndexName = bucket.getName() + "_" + System.nanoTime();

            // Temporarily set the indexName to the bucket-specific name for this index instance
            final String savedIndexName = indexName;
            indexName = bucketIndexName;

            // Create file path for this bucket's index
            // PaginatedComponent will append .{fileId}.{pageSize}.v{version}.{ext}, so we just provide the base name
            filePath = database.getDatabasePath() + File.separator + bucketIndexName;

            // Create the index for this bucket
            final LSMVectorIndex index = (LSMVectorIndex) schema.indexFactory.createIndex(this);

            // Restore the original indexName for the next iteration
            indexName = savedIndexName;

            // Register with schema (register the component, not the index wrapper)
            schema.registerFile(index.getComponent());
            schema.indexMap.put(bucketIndexName, index);

            // Register with DocumentType for this specific bucket
            type.addIndexInternal(index, bucket.getFileId(), propertyNames, null);

            // Build the index (this is the critical step that LSMVectorIndexBuilder was missing)
            index.build(batchSize, callback);

            indexes[finalIdx] = index;

          }, false, 3, null, (error) -> {
            // Cleanup on error
            for (int j = 0; j < indexes.length; j++) {
              final IndexInternal indexToRemove = (IndexInternal) indexes[j];
              if (indexToRemove != null)
                indexToRemove.drop();
            }
          });
        }

        schema.saveConfiguration();
        return null;
      });

      return type.getPolymorphicIndexByProperties(Arrays.asList(propertyNames));

    } catch (final NeedRetryException e) {
      schema.dropIndex(typeName + Arrays.toString(propertyNames));
      throw e;
    } catch (final Throwable e) {
      schema.dropIndex(typeName + Arrays.toString(propertyNames));
      throw new IndexException("Error creating LSM vector index '" + indexName + "'", e);
    }
  }

  /**
   * Sets the number of dimensions for the vectors.
   *
   * @param dimensions the number of dimensions
   *
   * @return this builder
   */
  @Override
  public LSMVectorIndexBuilder withDimensions(final int dimensions) {
    this.dimensions = dimensions;
    return this;
  }

  /**
   * Sets the similarity function to use for vector comparison.
   * Supported values: COSINE, DOT_PRODUCT, EUCLIDEAN
   *
   * @param similarity the similarity function name
   *
   * @return this builder
   */
  @Override
  public LSMVectorIndexBuilder withSimilarity(final String similarity) {
    try {
      this.similarityFunction = VectorSimilarityFunction.valueOf(similarity.toUpperCase());
    } catch (final IllegalArgumentException e) {
      throw new IndexException("Invalid similarity function: " + similarity + ". Supported values: COSINE, DOT_PRODUCT, EUCLIDEAN");
    }
    return this;
  }

  /**
   * Sets the maximum number of connections per node in the HNSW graph.
   * Higher values improve recall but increase memory usage and build time.
   * Typical range: 8-64, default: 16
   *
   * @param maxConnections the maximum number of connections
   *
   * @return this builder
   */
  @Override
  public LSMVectorIndexBuilder withMaxConnections(final int maxConnections) {
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
   *
   * @return this builder
   */
  @Override
  public LSMVectorIndexBuilder withBeamWidth(final int beamWidth) {
    if (beamWidth < 1)
      throw new IllegalArgumentException("beamWidth must be at least 1");
    this.beamWidth = beamWidth;
    return this;
  }

  /**
   * Sets the ID property name used to identify vertices.
   * This property is used when searching for vertices by ID.
   * Default is "id".
   *
   * @param idPropertyName the ID property name
   *
   * @return this builder
   */
  @Override
  public LSMVectorIndexBuilder withIdProperty(final String idPropertyName) {
    this.idPropertyName = idPropertyName;
    return this;
  }

  /**
   * Configures the index from a metadata JSON object.
   * Expected keys:
   * - dimensions (required): number of vector dimensions
   * - similarity (optional): similarity function (COSINE, DOT_PRODUCT, EUCLIDEAN), defaults to COSINE
   * - maxConnections (optional): max connections per node in HNSW graph, defaults to 16
   * - beamWidth (optional): beam width for search operations, defaults to 100
   * - idPropertyName (optional): property name used to identify vertices, defaults to "id"
   *
   * @param metadata the metadata JSON
   *
   * @return this builder
   */
  public LSMVectorIndexBuilder withMetadata(final JSONObject metadata) {
    if (metadata.has("dimensions"))
      this.dimensions = metadata.getInt("dimensions");

    if (metadata.has("similarity"))
      withSimilarity(metadata.getString("similarity"));

    if (metadata.has("maxConnections"))
      this.maxConnections = metadata.getInt("maxConnections");

    if (metadata.has("beamWidth"))
      this.beamWidth = metadata.getInt("beamWidth");

    if (metadata.has("idPropertyName"))
      this.idPropertyName = metadata.getString("idPropertyName");

    return this;
  }

  // Getters (typeName and propertyNames inherited from TypeIndexBuilder)
  public int getDimensions() {
    return dimensions;
  }

  public VectorSimilarityFunction getSimilarityFunction() {
    return similarityFunction;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public int getBeamWidth() {
    return beamWidth;
  }

  public String getIdPropertyName() {
    return idPropertyName;
  }
}
