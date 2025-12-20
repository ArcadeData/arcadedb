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
import java.util.List;

/**
 * Calculates the variance of vector elements.
 * Measures how spread out the values are from the mean.
 *
 * Formula: Variance = (1/n) * Σ(x_i - mean)^2
 *
 * Example: vectorVariance([1, 2, 3, 4, 5]) = 2.0
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorVariance extends SQLFunctionAbstract {
  public static final String NAME = "VECTOR_VARIANCE";

  public SQLFunctionVectorVariance() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 1)
      throw new CommandSQLParsingException(getSyntax());

    final Object vectorObj = params[0];
    if (vectorObj == null)
      return null;

    final float[] vector = toFloatArray(vectorObj);

    if (vector.length == 0)
      throw new CommandSQLParsingException("Vector cannot be empty");

    // Calculate mean
    double sum = 0.0;
    for (final float value : vector) {
      sum += value;
    }
    final double mean = sum / vector.length;

    // Calculate variance: average of squared differences from mean
    double varianceSum = 0.0;
    for (final float value : vector) {
      final double diff = value - mean;
      varianceSum += diff * diff;
    }

    return (float) (varianceSum / vector.length);
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
    } else if (vector instanceof List<?> list) {
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
    return "VECTOR_VARIANCE(<vector>)";
  }
}
