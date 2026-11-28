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
package com.arcadedb.function.vector;

import com.arcadedb.function.StatelessFunction;
import com.arcadedb.index.vector.VectorArgumentCache;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * vector.similarity.cosine(a, b) - computes cosine similarity between two vectors.
 * Delegates to {@link VectorUtils#cosineSimilarity(float[], float[])} for SIMD-optimized computation.
 */
public class VectorSimilarityCosineFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "vector.similarity.cosine";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    if (args[0] == null || args[1] == null)
      return null;
    // The two arguments are prepared through the command's cache: a scan scores every row against
    // the same query vector, and preparing that vector is the same work every row (issue #12568)
    final VectorUtils.PreparedVector v1 = VectorArgumentCache.prepareCoordinates(args[0], context);
    final VectorUtils.PreparedVector v2 = VectorArgumentCache.prepareCoordinates(args[1], context);
    VectorUtils.validateSameDimension(v1.values(), v2.values());
    return (double) VectorUtils.cosineSimilarity(v1, v2);
  }
}
