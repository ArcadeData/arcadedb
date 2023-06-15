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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.vector.HnswVectorIndex;
import com.arcadedb.index.vector.HnswVectorIndexRAM;
import com.arcadedb.security.SecurityDatabaseUser;
import com.github.jelmerk.knn.DistanceFunction;

import java.util.*;

/**
 * Builder class for vector indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorIndexBuilder extends IndexBuilder<HnswVectorIndex> {
  public static final  int DEFAULT_M               = 10;
  public static final  int DEFAULT_EF              = 10;
  public static final  int DEFAULT_EF_CONSTRUCTION = 200;
  private static final int CURRENT_VERSION         = 1;

  int              dimensions;
  DistanceFunction distanceFunction;
  Comparator       distanceComparator;
  int              maxItemCount;
  int              m              = DEFAULT_M;
  int              ef             = DEFAULT_EF;
  int              efConstruction = DEFAULT_EF_CONSTRUCTION;
  String           vertexType;
  String           edgeType;
  String           vectorPropertyName;
  String           idPropertyName;
  Map<RID, Vertex> cache;
  private HnswVectorIndexRAM origin;

  VectorIndexBuilder(final DatabaseInternal database) {
    super(database, HnswVectorIndex.class);
  }

  public VectorIndexBuilder(final Database database, final HnswVectorIndexRAM origin) {
    super((DatabaseInternal) database, HnswVectorIndex.class);
    this.origin = origin;
    this.dimensions = origin.getDimensions();
    this.distanceFunction = origin.getDistanceFunction();
    this.distanceComparator = origin.getDistanceComparator();
    this.maxItemCount = origin.getMaxItemCount();
    this.m = origin.getM();
    this.ef = origin.getEf();
    this.efConstruction = origin.getEfConstruction();
  }

  public HnswVectorIndex create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (database.async().isProcessing())
      throw new SchemaException("Cannot create a new index while asynchronous tasks are running");

    if (vertexType == null)
      throw new IndexException("Vertex type is missing from vector index declaration");
    if (edgeType == null)
      throw new IndexException("Edge type is missing from vector index declaration");
    if (idPropertyName == null)
      throw new IndexException("Vertex id property type is missing from vector index declaration");
    if (vectorPropertyName == null)
      throw new IndexException("Vertex vector property type is missing from vector index declaration");

    final EmbeddedSchema schema = database.getSchema().getEmbedded();
    if (ignoreIfExists) {
      Index index = schema.getIndexByName(indexName);
      if (index instanceof HnswVectorIndex) {
        if (!index.getTypeName().equalsIgnoreCase(vertexType))
          throw new IndexException("Index '" + indexName + "' is already defined but on type '" + index.getTypeName() + "'");
        return (HnswVectorIndex) index;
      }
    }

    final HnswVectorIndex index = (HnswVectorIndex) schema.indexFactory.createIndex(this);

    schema.registerFile(index.getComponent());

    index.build(origin, EmbeddedSchema.BUILD_TX_BATCH_SIZE, callback);

    return index;
  }

  public VectorIndexBuilder withDistanceFunction(final DistanceFunction distanceFunction) {
    this.distanceFunction = distanceFunction;
    return this;
  }

  public VectorIndexBuilder withDistanceComparator(final Comparator distanceComparator) {
    this.distanceComparator = distanceComparator;
    return this;
  }

  public VectorIndexBuilder withDimensions(final int dimensions) {
    this.dimensions = dimensions;
    return this;
  }

  public VectorIndexBuilder withMaxItemCount(final int maxItemCount) {
    this.maxItemCount = maxItemCount;
    return this;
  }

  /**
   * Sets the number of bi-directional links created for every new element during construction. Reasonable range
   * for m is 2-100. Higher m work better on datasets with high intrinsic dimensionality and/or high recall,
   * while low m work better for datasets with low intrinsic dimensionality and/or low recalls. The parameter
   * also determines the algorithm's memory consumption.
   * As an example for d = 4 random vectors optimal m for search is somewhere around 6, while for high dimensional
   * datasets (word embeddings, good face descriptors), higher M are required (e.g. m = 48, 64) for optimal
   * performance at high recall. The range mM = 12-48 is ok for the most of the use cases. When m is changed one
   * has to update the other parameters. Nonetheless, ef and efConstruction parameters can be roughly estimated by
   * assuming that m  efConstruction is a constant.
   *
   * @param m the number of bi-directional links created for every new element during construction
   *
   * @return the builder.
   */
  public VectorIndexBuilder withM(int m) {
    this.m = m;
    return this;
  }

  /**
   * `
   * The parameter has the same meaning as ef, but controls the index time / index precision. Bigger efConstruction
   * leads to longer construction, but better index quality. At some point, increasing efConstruction does not
   * improve the quality of the index. One way to check if the selection of ef_construction was ok is to measure
   * a recall for M nearest neighbor search when ef = efConstruction: if the recall is lower than 0.9, then
   * there is room for improvement.
   *
   * @param efConstruction controls the index time / index precision
   *
   * @return the builder
   */
  public VectorIndexBuilder withEfConstruction(int efConstruction) {
    this.efConstruction = efConstruction;
    return this;
  }

  /**
   * The size of the dynamic list for the nearest neighbors (used during the search). Higher ef leads to more
   * accurate but slower search. The value ef of can be anything between k and the size of the dataset.
   *
   * @param ef size of the dynamic list for the nearest neighbors
   *
   * @return the builder
   */
  public VectorIndexBuilder withEf(int ef) {
    this.ef = ef;
    return this;
  }

  public VectorIndexBuilder withVertexType(final String vertexType) {
    this.vertexType = vertexType;
    return this;
  }

  public VectorIndexBuilder withEdgeType(final String edgeType) {
    this.edgeType = edgeType;
    return this;
  }

  public VectorIndexBuilder withVectorPropertyName(final String vectorPropertyName) {
    this.vectorPropertyName = vectorPropertyName;
    return this;
  }

  public VectorIndexBuilder withIdProperty(final String idPropertyName) {
    this.idPropertyName = idPropertyName;
    return this;
  }

  public VectorIndexBuilder withCache(final Map<RID, Vertex> cache) {
    this.cache = cache;
    return this;
  }

  public int getDimensions() {
    return dimensions;
  }

  public DistanceFunction getDistanceFunction() {
    return distanceFunction;
  }

  public Comparator getDistanceComparator() {
    return distanceComparator;
  }

  public int getM() {
    return m;
  }

  public int getEf() {
    return ef;
  }

  public int getEfConstruction() {
    return efConstruction;
  }

  public int getMaxItemCount() {
    return maxItemCount;
  }

  public String getVertexType() {
    return vertexType;
  }

  public String getIdPropertyName() {
    return idPropertyName;
  }

  public String getEdgeType() {
    return edgeType;
  }

  public String getVectorPropertyName() {
    return vectorPropertyName;
  }

  public Map<RID, Vertex> getCache() {
    return cache;
  }
}
