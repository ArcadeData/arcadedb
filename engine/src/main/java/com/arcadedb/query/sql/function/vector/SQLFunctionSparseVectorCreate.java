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
 * Creates a sparse vector from indices and values arrays.
 * Supports two signatures:
 * - sparseVectorCreate(indices_array, values_array) - infers dimensions
 * - sparseVectorCreate(indices_array, values_array, dimensions) - explicit dimensions
 *
 * Example: sparseVectorCreate([0, 2, 5], [0.5, 0.3, 0.8]) → SparseVector
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionSparseVectorCreate extends SQLFunctionAbstract {
  public static final String NAME = "SPARSE_VECTOR_CREATE";

  public SQLFunctionSparseVectorCreate() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 2 || params.length > 3)
      throw new CommandSQLParsingException(getSyntax());

    final Object indicesObj = params[0];
    final Object valuesObj = params[1];

    if (indicesObj == null || valuesObj == null)
      return null;

    final int[] indices = toIntArray(indicesObj);
    final float[] values = toFloatArray(valuesObj);

    if (indices.length != values.length)
      throw new CommandSQLParsingException("Indices and values arrays must have same length");

    if (params.length == 2) {
      // Infer dimensions from max index
      return new SparseVector(indices, values);
    } else {
      // Explicit dimensions
      final Object dimsObj = params[2];
      if (dimsObj == null)
        throw new CommandSQLParsingException("Dimensions cannot be null");

      int dimensions;
      if (dimsObj instanceof Number num) {
        dimensions = num.intValue();
      } else {
        throw new CommandSQLParsingException("Dimensions must be a number, found: " + dimsObj.getClass().getSimpleName());
      }

      return new SparseVector(indices, values, dimensions);
    }
  }

  private int[] toIntArray(final Object indices) {
    if (indices instanceof int[] intArray) {
      return intArray;
    } else if (indices instanceof Object[] objArray) {
      final int[] result = new int[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num) {
          result[i] = num.intValue();
        } else {
          throw new CommandSQLParsingException("Index values must be integers, found: " + objArray[i].getClass().getSimpleName());
        }
      }
      return result;
    } else if (indices instanceof List<?> list) {
      final int[] result = new int[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (elem instanceof Number num) {
          result[i] = num.intValue();
        } else {
          throw new CommandSQLParsingException("Index values must be integers, found: " + elem.getClass().getSimpleName());
        }
      }
      return result;
    } else {
      throw new CommandSQLParsingException("Indices must be an array or list, found: " + indices.getClass().getSimpleName());
    }
  }

  private float[] toFloatArray(final Object values) {
    if (values instanceof float[] floatArray) {
      return floatArray;
    } else if (values instanceof Object[] objArray) {
      final float[] result = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Value elements must be numbers, found: " + objArray[i].getClass().getSimpleName());
        }
      }
      return result;
    } else if (values instanceof List<?> list) {
      final float[] result = new float[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (elem instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Value elements must be numbers, found: " + elem.getClass().getSimpleName());
        }
      }
      return result;
    } else {
      throw new CommandSQLParsingException("Values must be an array or list, found: " + values.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return "SPARSE_VECTOR_CREATE(<indices_array>, <values_array> [, <dimensions>])";
  }
}
