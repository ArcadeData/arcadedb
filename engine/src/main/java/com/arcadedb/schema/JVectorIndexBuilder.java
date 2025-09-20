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
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.vector.JVectorIndex;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.FileUtils;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.util.ArrayList;
import java.util.List;

import java.io.File;

/**
 * Builder class for JVector indexes.
 *
 * @author Claude Code AI Assistant
 */
public class JVectorIndexBuilder extends IndexBuilder<JVectorIndex> {
  public static final  int DEFAULT_MAX_CONNECTIONS = 16;
  public static final  int DEFAULT_BEAM_WIDTH      = 100;
  private static final int CURRENT_VERSION         = 1;

  private int                      dimensions;
  private VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
  private int                      maxConnections     = DEFAULT_MAX_CONNECTIONS;
  private int                      beamWidth          = DEFAULT_BEAM_WIDTH;
  private String                   vertexType;
  private String                   vectorPropertyName;
  private Type                     vectorPropertyType = Type.ARRAY_OF_FLOATS;

  JVectorIndexBuilder(final DatabaseInternal database) {
    super(database, JVectorIndex.class);
    this.indexType = Schema.INDEX_TYPE.JVECTOR;
  }

  public JVectorIndex create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (database.isAsyncProcessing())
      throw new NeedRetryException("Cannot create a new index while asynchronous tasks are running");

    if (vertexType == null)
      throw new IndexException("Vertex type is missing from vector index declaration");
    if (vectorPropertyName == null)
      throw new IndexException("Vertex vector property name is missing from vector index declaration");

    filePath = database.getDatabasePath() + File.separator + FileUtils.encode(vertexType, database.getSchema().getEncoding()) + "_"
        + System.nanoTime() + "."
        + database.getFileManager().newFileId() + ".v" + JVectorIndex.CURRENT_VERSION + "." + JVectorIndex.FILE_EXT;

    final LocalSchema schema = database.getSchema().getEmbedded();
    if (ignoreIfExists) {
      Index index = schema.getIndexByName(indexName);
      if (index instanceof JVectorIndex vectorIndex) {
        if (!index.getTypeName().equalsIgnoreCase(vertexType))
          throw new IndexException("Index '" + indexName + "' is already defined but on type '" + index.getTypeName() + "'");
        return vectorIndex;
      }
    }


    final JVectorIndex index = (JVectorIndex) schema.indexFactory.createIndex(this);

    schema.registerFile(index.getComponent());
    schema.indexMap.put(index.getName(), index);

    // JVectorIndex works as a container index. Register it with all buckets for automatic indexing.
    final LocalDocumentType type = schema.getType(vertexType);
    if (type != null) {
      // Add to bucket indexes for automatic indexing discovery by DocumentIndexer
      for (final Bucket bucket : type.getBuckets(false)) {
        final List<IndexInternal> bucketIndexes = type.bucketIndexesByBucket.computeIfAbsent(bucket.getFileId(), k -> new ArrayList<>());
        bucketIndexes.add(index);
      }
    }

    index.build(LocalSchema.BUILD_TX_BATCH_SIZE, callback);

    // Save configuration to ensure index persists
    schema.saveConfiguration();

    return index;
  }

  public JVectorIndexBuilder withDimensions(final int dimensions) {
    this.dimensions = dimensions;
    return this;
  }

  public JVectorIndexBuilder withSimilarityFunction(final VectorSimilarityFunction similarityFunction) {
    this.similarityFunction = similarityFunction;
    return this;
  }

  public JVectorIndexBuilder withMaxConnections(final int maxConnections) {
    this.maxConnections = maxConnections;
    return this;
  }

  public JVectorIndexBuilder withBeamWidth(final int beamWidth) {
    this.beamWidth = beamWidth;
    return this;
  }

  public JVectorIndexBuilder withVertexType(final String vertexType) {
    this.vertexType = vertexType;
    return this;
  }

  public JVectorIndexBuilder withVectorProperty(final String vectorPropertyName, final Type vectorPropertyType) {
    if (vectorPropertyType != Type.ARRAY_OF_SHORTS && vectorPropertyType != Type.ARRAY_OF_INTEGERS
        && vectorPropertyType != Type.ARRAY_OF_LONGS
        && vectorPropertyType != Type.ARRAY_OF_FLOATS && vectorPropertyType != Type.ARRAY_OF_DOUBLES)
      throw new IllegalArgumentException("Vector property type '" + vectorPropertyType + "' not compatible with vectors");

    this.vectorPropertyName = vectorPropertyName;
    this.vectorPropertyType = vectorPropertyType;
    return this;
  }


  // Getters
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

  public String getVertexType() {
    return vertexType;
  }


  public String getVectorPropertyName() {
    return vectorPropertyName;
  }

  public Type getVectorPropertyType() {
    return vectorPropertyType;
  }

  // Override parent methods to return correct type for method chaining
  @Override
  public JVectorIndexBuilder withIndexName(final String indexName) {
    super.withIndexName(indexName);
    return this;
  }

  @Override
  public JVectorIndexBuilder withFilePath(final String path) {
    super.withFilePath(path);
    return this;
  }

  @Override
  public JVectorIndexBuilder withIgnoreIfExists(final boolean ignoreIfExists) {
    super.withIgnoreIfExists(ignoreIfExists);
    return this;
  }

  @Override
  public JVectorIndexBuilder withCallback(final Index.BuildIndexCallback callback) {
    super.withCallback(callback);
    return this;
  }
}
