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
package com.arcadedb.query.sql.function.vector;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;

import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * Aggregate function that computes element-wise average of vectors (centroid).
 * Returns a vector where each component is the average of corresponding components across all input vectors.
 * <p>
 * Uses JVector's SIMD-optimized VectorUtil.scale() for the final averaging operation,
 * providing up to 2-3x performance improvement for large vectors on Java 20+ with Vector API enabled.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorAvg extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorAvg";

  private float[] sumVector;
  private long    count;
  private int     dimensions;

  public SQLFunctionVectorAvg() {
    super(NAME);
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length == 0)
      throw new CommandSQLParsingException(getSyntax());

    final Object vector = params[0];

    if (vector == null)
      return null;

    final float[] v = toFloatArray(vector);

    // Initialize on first call
    if (sumVector == null) {
      dimensions = v.length;
      sumVector = new float[dimensions];
      count = 0;
    } else if (sumVector.length != v.length) {
      throw new CommandSQLParsingException("All vectors must have the same dimension");
    }

    // Add to sum
    for (int i = 0; i < v.length; i++) {
      sumVector[i] += v[i];
    }
    count++;

    return null; // Aggregate functions don't return per-row, only at the end
  }

  @Override
  public Object getResult() {
    if (sumVector == null || count == 0)
      return null;

    // Use JVector's SIMD-optimized scaling for averaging (2-3x faster with Vector API)
    try {
      final VectorizationProvider vp = VectorizationProvider.getInstance();

      // Create result as copy of sum vector
      final float[] avgVector = sumVector.clone();
      final VectorFloat<?> avgVec = vp.getVectorTypeSupport().createFloatVector(avgVector);

      // Scale by 1/count to get average
      VectorUtil.scale(avgVec, 1.0f / count);

      // Extract result
      for (int i = 0; i < avgVector.length; i++) {
        avgVector[i] = avgVec.get(i);
      }
      return avgVector;
    } catch (final Exception e) {
      // Fallback to scalar implementation
      final float[] avgVector = new float[sumVector.length];
      for (int i = 0; i < sumVector.length; i++) {
        avgVector[i] = sumVector[i] / count;
      }
      return avgVector;
    }
  }

  public String getSyntax() {
    return NAME + "(<vector_column>)";
  }
}
