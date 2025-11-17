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
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMVectorIndex;
import com.arcadedb.security.SecurityDatabaseUser;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Builder for creating LSM-based Vector indexes (LSM_VECTOR).
 *
 * <p>LSM Vector indexes combine JVector's HNSW-Vamana algorithm with ArcadeDB's
 * page-based LSM storage, providing memory-efficient vector similarity search
 * with full ACID transaction support.
 *
 * <p><b>Configuration Parameters:</b>
 * <ul>
 *   <li>dimensions (required): Vector dimensionality (1-4096)</li>
 *   <li>similarity: COSINE, EUCLIDEAN, or DOT_PRODUCT (default: COSINE)</li>
 *   <li>maxConnections: HNSW maximum connections per node (default: 16, range: 2-100)</li>
 *   <li>beamWidth: HNSW beam width for construction/search (default: 100, range: 10-1000)</li>
 *   <li>alpha: Diversity parameter for HNSW (default: 1.2, range: 1.0-2.0)</li>
 * </ul>
 *
 * <p><b>Example:</b>
 * <pre>
 * schema.buildLSMVectorIndex(typeName, propertyNames)
 *   .withDimensions(512)
 *   .withSimilarity(VectorSimilarityFunction.COSINE)
 *   .withMaxConnections(32)
 *   .withBeamWidth(150)
 *   .withAlpha(1.2f)
 *   .withPageSize(65536)
 *   .create();
 * </pre>
 *
 * @author Arcade Data
 * @since 24.12.0
 */
public class LSMVectorIndexBuilder extends IndexBuilder<LSMVectorIndex> {

  // JVector HNSW parameters
  private int                      dimensions         = -1; // Required parameter
  private VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.COSINE;
  private int                      maxConnections     = 16;
  private int                      beamWidth          = 100;
  private float                    alpha              = 1.2f;
  private String                   typeName;
  private String                   propertyName;
  private Type                     propertyType;

  public String getTypeName() {
    return typeName;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public Type getPropertyType() {
    return propertyType;
  }
// Inherited LSM parameters:
  // - unique (inherited, set to true for LSM_VECTOR)
  // - pageSize (inherited, default DEF_PAGE_SIZE)
  // - nullStrategy (inherited, default SKIP)

  /**
   * Creates a new LSMVectorIndexBuilder for the given database.
   *
   * @param database the database instance
   *
   * @throws NullPointerException if database is null
   */
  public LSMVectorIndexBuilder(final DatabaseInternal database) {
    super(database, LSMVectorIndex.class);
    // LSM_VECTOR indexes are always unique
    super.withUnique(true);
    // Set default type
    super.withType(Schema.INDEX_TYPE.LSM_VECTOR);
  }

  /**
   * Sets the vector dimensionality (required).
   *
   * @param dimensions the number of dimensions (1-4096)
   *
   * @return this builder for chaining
   *
   * @throws ConfigurationException if dimensions is out of range
   */
  public LSMVectorIndexBuilder withDimensions(final int dimensions) {
    if (dimensions < 1 || dimensions > 4096) {
      throw new ConfigurationException(
          "Vector dimensions must be between 1 and 4096, got " + dimensions);
    }
    this.dimensions = dimensions;
    return this;
  }

  /**
   * Sets the vector similarity function.
   *
   * @param function the similarity function (COSINE, EUCLIDEAN, DOT_PRODUCT)
   *
   * @return this builder for chaining
   *
   * @throws ConfigurationException if function is invalid
   */
  public LSMVectorIndexBuilder withSimilarity(final VectorSimilarityFunction function) {
    if (function == null ) {
      throw new ConfigurationException("Similarity function cannot be null or empty");
    }
    this.similarityFunction = function;
    return this;
  }

  /**
   * Sets the maximum number of bi-directional connections per node in the HNSW graph.
   * Higher values improve recall but increase memory usage and construction time.
   *
   * @param maxConnections the maximum connections (2-100, default 16)
   *
   * @return this builder for chaining
   *
   * @throws ConfigurationException if maxConnections is out of range
   */
  public LSMVectorIndexBuilder withMaxConnections(final int maxConnections) {
    if (maxConnections < 2 || maxConnections > 100) {
      throw new ConfigurationException(
          "Max connections must be between 2 and 100, got " + maxConnections);
    }
    this.maxConnections = maxConnections;
    return this;
  }

  /**
   * Sets the beam width for HNSW construction and search.
   * Higher values improve search quality but reduce speed.
   *
   * @param beamWidth the beam width (10-1000, default 100)
   *
   * @return this builder for chaining
   *
   * @throws ConfigurationException if beamWidth is out of range
   */
  public LSMVectorIndexBuilder withBeamWidth(final int beamWidth) {
    if (beamWidth < 10 || beamWidth > 1000) {
      throw new ConfigurationException(
          "Beam width must be between 10 and 1000, got " + beamWidth);
    }
    this.beamWidth = beamWidth;
    return this;
  }

  /**
   * Sets the alpha parameter (diversity factor) for HNSW.
   * Higher values favor diversity over proximity in neighbor selection.
   * Typical ranges: 1.2 for high-dimensional, 2.0 for low-dimensional vectors.
   *
   * @param alpha the alpha parameter (1.0-2.0, default 1.2)
   *
   * @return this builder for chaining
   *
   * @throws ConfigurationException if alpha is out of range
   */
  public LSMVectorIndexBuilder withAlpha(final float alpha) {
    if (alpha < 1.0f || alpha > 2.0f) {
      throw new ConfigurationException(
          "Alpha must be between 1.0 and 2.0, got " + alpha);
    }
    this.alpha = alpha;
    return this;
  }

  /**
   * Validates the builder configuration before creating the index.
   *
   * @throws ConfigurationException if required parameters are missing or invalid
   */
  private void validate() {
    if (dimensions < 0) {
      throw new ConfigurationException("Vector dimensions must be specified (1-4096)");
    }
    if (beamWidth < maxConnections) {
      throw new ConfigurationException(
          "beamWidth (" + beamWidth + ") must be >= maxConnections (" + maxConnections + ")");
    }
  }

  /**
   * Creates the LSM Vector index with the current configuration.
   *
   * <p>This method:
   * <ol>
   *   <li>Validates the configuration</li>
   *   <li>Checks permissions</li>
   *   <li>Generates file path</li>
   *   <li>Delegates to factory handler to create the index</li>
   * </ol>
   *
   * @return the created LSM Vector index
   *
   * @throws ConfigurationException if configuration is invalid
   * @throws IllegalStateException  if the index already exists and ifNotExists is false
   */
  @Override
  public LSMVectorIndex create() {
    // Validate configuration first
    validate();

    // Check permissions
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    // Check for async processing
    if (database.isAsyncProcessing()) {
      throw new com.arcadedb.exception.NeedRetryException(
          "Cannot create a new index while asynchronous tasks are running");
    }

    // Get embedded schema
    final LocalSchema schema = database.getSchema().getEmbedded();

    if (indexName == null) {
      indexName = typeName + Arrays.toString(new Object[] { propertyName }).replace(" ", "");
    }
    // Handle ignoreIfExists
    if (ignoreIfExists) {
      final IndexInternal existingIndex = schema.indexMap.get(indexName);
      if (existingIndex != null) {
        if (existingIndex instanceof LSMVectorIndex) {
          return (LSMVectorIndex) existingIndex;
        }
      }
    } else if (schema.indexMap.containsKey(indexName)) {
      throw new com.arcadedb.exception.SchemaException(
          "Cannot create index '" + indexName + "' because already exists");
    }

    // Record file changes and create index within transaction
    return schema.recordFileChanges(() -> {
      final AtomicReference<LSMVectorIndex> result = new AtomicReference<>();

      database.transaction(() -> {
        // Set file path if not already set
        if (filePath == null) {
          filePath = database.getDatabasePath() + File.separator + indexName;
        }

        // Delegate to IndexFactory which will call IndexFactoryHandler
        final IndexInternal index = schema.indexFactory.createIndex(this);

        result.set((LSMVectorIndex) index);

        // Register file if it's a paginated component
        if (index instanceof PaginatedComponent component) {
          schema.registerFile(component);
        }

        // Register index directly with all buckets for automatic indexing
        // Note: We cannot use addIndexInternal() because it creates TypeIndex wrappers
        // that expect per-bucket component files, which LSMVectorIndex doesn't have
        if (database.getSchema().existsType(typeName)) {
          final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);

          // Set metadata on the index first
          final List<com.arcadedb.engine.Bucket> buckets = type.getBuckets(false);
          if (!buckets.isEmpty()) {
            final int firstBucketId = buckets.get(0).getFileId();
            index.setMetadata(typeName, new String[] { propertyName }, firstBucketId);
          }

          // Register with all buckets
          for (final com.arcadedb.engine.Bucket bucket : buckets) {
            final List<IndexInternal> bucketIndexes = type.bucketIndexesByBucket.computeIfAbsent(
                bucket.getFileId(), k -> new java.util.ArrayList<>());
            if (!bucketIndexes.contains(index)) {
              bucketIndexes.add(index);
            }
          }
        }

        // Register in schema's index map
        schema.indexMap.put(indexName, index);
      }, false, 1, null, (error) -> {
        // Cleanup on error
        final LSMVectorIndex indexToRemove = result.get();
        if (indexToRemove != null) {
          indexToRemove.drop();
        }
      });

      return result.get();
    });
  }

  /**
   * Gets the configured vector dimensionality.
   *
   * @return the dimensions, or -1 if not set
   */
  public int getDimensions() {
    return dimensions;
  }

  /**
   * Gets the configured similarity function.
   *
   * @return the similarity function name
   */
  public VectorSimilarityFunction getSimilarityFunction() {
    return similarityFunction;
  }

  /**
   * Gets the configured maximum connections per node.
   *
   * @return the maximum connections
   */
  public int getMaxConnections() {
    return maxConnections;
  }

  /**
   * Gets the configured beam width.
   *
   * @return the beam width
   */
  public int getBeamWidth() {
    return beamWidth;
  }

  /**
   * Gets the configured alpha parameter.
   *
   * @return the alpha value
   */
  public float getAlpha() {
    return alpha;
  }

  /**
   * Override to return LSMVectorIndexBuilder for proper chaining with vector-specific methods.
   *
   * @param indexName the index name
   *
   * @return this builder for chaining
   */
  @Override
  public LSMVectorIndexBuilder withIndexName(final String indexName) {
    super.withIndexName(indexName);
    return this;
  }

  public LSMVectorIndexBuilder withTypeName(final String typeName) {
    this.typeName = typeName;
    return this;
  }

  public LSMVectorIndexBuilder withProperty(final String propertyName, final Type propertyType) {
    if (propertyType != Type.ARRAY_OF_SHORTS && propertyType != Type.ARRAY_OF_INTEGERS
        && propertyType != Type.ARRAY_OF_LONGS
        && propertyType != Type.ARRAY_OF_FLOATS && propertyType != Type.ARRAY_OF_DOUBLES)
      throw new IllegalArgumentException("Vector property type '" + propertyType + "' not compatible with vectors");

    this.propertyName = propertyName;
    this.propertyType = propertyType;
    return this;
  }

}
