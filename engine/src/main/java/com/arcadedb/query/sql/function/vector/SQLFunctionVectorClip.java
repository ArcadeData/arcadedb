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
 * Clips (clamps) vector elements to a specified range.
 * Any value below min becomes min, any value above max becomes max.
 *
 * Formula: clipped[i] = max(min, min(max, value[i]))
 *
 * Example: vectorClip([1, 5, 10], 2, 8) = [2, 5, 8]
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorClip extends SQLFunctionAbstract {
  public static final String NAME = "vectorClip";

  public SQLFunctionVectorClip() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final Object vectorObj = params[0];
    final Object minObj = params[1];
    final Object maxObj = params[2];

    if (vectorObj == null || minObj == null || maxObj == null)
      return null;

    final float[] vector = toFloatArray(vectorObj);

    if (vector.length == 0)
      throw new CommandSQLParsingException("Vector cannot be empty");

    // Parse min and max
    final float min;
    if (minObj instanceof Number num1) {
      min = num1.floatValue();
    } else {
      throw new CommandSQLParsingException("Min must be a number, found: " + minObj.getClass().getSimpleName());
    }

    final float max;
    if (maxObj instanceof Number num2) {
      max = num2.floatValue();
    } else {
      throw new CommandSQLParsingException("Max must be a number, found: " + maxObj.getClass().getSimpleName());
    }

    if (min > max)
      throw new CommandSQLParsingException("Min (" + min + ") must be <= max (" + max + ")");

    // Clip each element
    final float[] result = new float[vector.length];
    for (int i = 0; i < vector.length; i++) {
      result[i] = Math.max(min, Math.min(max, vector[i]));
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
    return NAME + "(<vector>, <min>, <max>)";
  }
}
