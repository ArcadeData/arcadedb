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
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * Calculates the dot product (inner product) of two vectors.
 * The dot product is the sum of element-wise products.
 * Note: For use with normalized vectors, this is equivalent to cosine similarity.
 *
 * Uses JVector's SIMD-optimized VectorUtil.dotProduct() for 7-8x performance improvement
 * when running on Java 20+ with Panama Vector API enabled (--add-modules jdk.incubator.vector).
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorDotProduct extends SQLFunctionAbstract {
  public static final String NAME = "vectorDotProduct";

  public SQLFunctionVectorDotProduct() {
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

    // Use JVector's SIMD-optimized dotProduct (7-8x faster with Vector API)
    try {
      final VectorFloat<?> jv1 = VectorizationProvider.getInstance().getVectorTypeSupport().createFloatVector(v1);
      final VectorFloat<?> jv2 = VectorizationProvider.getInstance().getVectorTypeSupport().createFloatVector(v2);
      return VectorUtil.dotProduct(jv1, jv2);
    } catch (final Exception e) {
      // Fallback to scalar implementation if JVector fails
      double result = 0.0;
      for (int i = 0; i < v1.length; i++) {
        result += v1[i] * v2[i];
      }
      return (float) result;
    }
  }

  private float[] toFloatArray(final Object vector) {
    if (vector instanceof float[] floatArray) {
      return floatArray;
    } else if (vector instanceof Object[] objArray) {
      final float[] result = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Vector elements must be numbers, found: " + objArray[i].getClass().getSimpleName());
        }
      }
      return result;
    } else if (vector instanceof java.util.List<?> list) {
      final float[] result = new float[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (elem instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Vector elements must be numbers, found: " + elem.getClass().getSimpleName());
        }
      }
      return result;
    } else {
      throw new CommandSQLParsingException("Vector must be an array or list, found: " + vector.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return "vectorDotProduct(<vector1>, <vector2>)";
  }
}
