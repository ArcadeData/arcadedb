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

/**
 * Multiplies a vector by a scalar value.
 * Returns a new vector where each component is multiplied by the scalar.
 *
 * Uses scalar implementation which is 3-12x faster than JVector for typical vector sizes (< 4096).
 * JVector overhead from object allocation and conversion dominates actual computation cost.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorScale extends SQLFunctionAbstract {
  public static final String NAME = "vectorScale";

  public SQLFunctionVectorScale() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object vector = params[0];
    final Object scalarObj = params[1];

    if (vector == null || scalarObj == null)
      throw new CommandSQLParsingException("Vector and scalar cannot be null");

    final float[] v = toFloatArray(vector);

    final float scalar;
    if (scalarObj instanceof Number num) {
      scalar = num.floatValue();
    } else {
      throw new CommandSQLParsingException("Scalar must be a number, found: " + scalarObj.getClass().getSimpleName());
    }

    // Scalar implementation - significantly faster than JVector for typical sizes
    final float[] result = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      result[i] = v[i] * scalar;
    }
    return result;
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
    return "vectorScale(<vector>, <scalar>)";
  }
}
