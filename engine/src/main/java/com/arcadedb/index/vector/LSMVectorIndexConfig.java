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
package com.arcadedb.index.vector;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

/**
 * Construction-time configuration for {@link LSMVectorIndex}. Replaces the historical 17-positional
 * primary constructor (issue #4134): the factory built the index with positional args and then
 * reached in to mutate {@code metadata.encoding} after the fact, which only happened to be safe
 * because the new instance had not been published yet. Carrying every field on a single value
 * object lets the constructor copy them all into the metadata atomically and removes the implicit
 * "do not publish before encoding is set" contract.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record LSMVectorIndexConfig(
    String typeName,
    String[] propertyNames,
    int dimensions,
    VectorSimilarityFunction similarityFunction,
    VectorEncoding encoding,
    VectorQuantizationType quantizationType,
    int maxConnections,
    int beamWidth,
    String idPropertyName,
    int locationCacheSize,
    int graphBuildCacheSize,
    int mutationsBeforeRebuild,
    boolean storeVectorsInGraph,
    boolean addHierarchy,
    int pqSubspaces,
    int pqClusters,
    boolean pqCenterGlobally,
    int pqTrainingLimit) {
}
