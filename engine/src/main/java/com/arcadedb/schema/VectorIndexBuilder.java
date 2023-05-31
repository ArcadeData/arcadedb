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
import com.arcadedb.index.TypeIndex;
import com.github.jelmerk.knn.DistanceFunction;
import com.github.jelmerk.knn.hnsw.HnswIndex;

import java.util.*;

/**
 * Builder class for vector indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorIndexBuilder extends IndexBuilder<TypeIndex> {
  int              dimensions;
  DistanceFunction distanceFunction;
  Comparator       distanceComparator;
  int              maxItemCount;
  int              m              = HnswIndex.BuilderBase.DEFAULT_M;
  int              ef             = HnswIndex.BuilderBase.DEFAULT_EF;
  int              efConstruction = HnswIndex.BuilderBase.DEFAULT_EF_CONSTRUCTION;

  protected VectorIndexBuilder(final DatabaseInternal database) {
    super(database, TypeIndex.class);
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

  @Override
  public TypeIndex create() {
    return null;
  }
}
