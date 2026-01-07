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
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;

import static io.github.jbellis.jvector.vector.VectorUtil.squareL2Distance;

/**
 * Calculates the Euclidean (L2) distance between two vectors.
 * This is the straight-line distance in multi-dimensional space.
 * Lower values indicate greater similarity.
 * <p>
 * Uses JVector's SIMD-optimized VectorUtil.squareL2Distance() for up to 5-6x performance improvement
 * when running on Java 20+ with Panama Vector API enabled (--add-modules jdk.incubator.vector).
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorL2Distance extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorL2Distance";

  public SQLFunctionVectorL2Distance() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object vector1 = params[0];
    final Object vector2 = params[1];

    if (vector1 == null || vector2 == null)
      throw new CommandSQLParsingException("Vectors cannot be null");

    final float[] v1 = toFloatArray(vector1);
    final float[] v2 = toFloatArray(vector2);

    if (v1.length != v2.length)
      throw new CommandSQLParsingException("Vectors must have the same dimension");

    // Use JVector's SIMD-optimized L2 distance (5-6x faster with Vector API)
    try {
      final VectorizationProvider vp = VectorizationProvider.getInstance();
      final VectorFloat<?> jv1 = vp.getVectorTypeSupport().createFloatVector(v1);
      final VectorFloat<?> jv2 = vp.getVectorTypeSupport().createFloatVector(v2);
      final float squaredDistance = squareL2Distance(jv1, jv2);
      return (float) Math.sqrt(Math.max(0.0f, squaredDistance)); // Max to avoid NaN from floating point errors
    } catch (final Exception e) {
      // Fallback to scalar implementation
      double sumSquaredDiff = 0.0;
      for (int i = 0; i < v1.length; i++) {
        final double diff = v1[i] - v2[i];
        sumSquaredDiff += diff * diff;
      }
      return (float) Math.sqrt(sumSquaredDiff);
    }
  }

  public String getSyntax() {
    return NAME + "(<vector1>, <vector2>)";
  }
}
