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
 * Aggregate function that computes element-wise minimum of vectors.
 * Returns a vector where each component is the minimum value of corresponding components across all input vectors.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorMin extends SQLFunctionAbstract {
  public static final String NAME = "vectorMin";

  private float[] minVector;
  private int dimensions;

  public SQLFunctionVectorMin() {
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
    if (minVector == null) {
      dimensions = v.length;
      minVector = v.clone();
    } else if (minVector.length != v.length) {
      throw new CommandSQLParsingException("All vectors must have the same dimension");
    } else {
      // Update minimums
      for (int i = 0; i < v.length; i++) {
        minVector[i] = Math.min(minVector[i], v[i]);
      }
    }

    return null; // Aggregate functions don't return per-row, only at the end
  }

  @Override
  public Object getResult() {
    if (minVector == null)
      return null;

    return minVector.clone();
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
    return NAME + "(<vector_column>)";
  }
}
