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

import static io.github.jbellis.jvector.vector.VectorUtil.cosine;

/**
 * Calculates the cosine similarity between two vectors.
 * Returns a value between -1 and 1, where 1 means identical direction,
 * 0 means perpendicular, and -1 means opposite direction.
 * Useful for comparing normalized embeddings.
 * <p>
 * Uses JVector's SIMD-optimized VectorUtil.cosine() for up to 6-7x performance improvement
 * when running on Java 20+ with Panama Vector API enabled (--add-modules jdk.incubator.vector).
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorCosineSimilarity extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorCosineSimilarity";

  public SQLFunctionVectorCosineSimilarity() {
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

    if (v1.length == 0)
      throw new CommandSQLParsingException("Vectors cannot be empty");

    // Use JVector's SIMD-optimized cosine similarity (6-7x faster with Vector API)
    try {
      final VectorizationProvider vp = VectorizationProvider.getInstance();
      final VectorFloat<?> jv1 = vp.getVectorTypeSupport().createFloatVector(v1);
      final VectorFloat<?> jv2 = vp.getVectorTypeSupport().createFloatVector(v2);
      return cosine(jv1, jv2);
    } catch (final Exception e) {
      // Fallback to scalar implementation
      // Calculate dot product
      double dotProduct = 0.0;
      for (int i = 0; i < v1.length; i++) {
        dotProduct += v1[i] * v2[i];
      }

      // Calculate magnitudes
      double magnitude1 = 0.0;
      double magnitude2 = 0.0;
      for (int i = 0; i < v1.length; i++) {
        magnitude1 += v1[i] * v1[i];
        magnitude2 += v2[i] * v2[i];
      }
      magnitude1 = Math.sqrt(magnitude1);
      magnitude2 = Math.sqrt(magnitude2);

      // Handle zero vectors
      if (magnitude1 == 0.0 || magnitude2 == 0.0)
        return 0.0f;

      // Cosine similarity = dot product / (mag1 * mag2)
      return (float) (dotProduct / (magnitude1 * magnitude2));
    }
  }

  public String getSyntax() {
    return NAME + "(<vector1>, <vector2>)";
  }
}
