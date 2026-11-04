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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Creates a sparse vector from indices and values arrays.
 * Supports two signatures:
 * - sparseVectorCreate(indices_array, values_array) - infers dimensions
 * - sparseVectorCreate(indices_array, values_array, dimensions) - explicit dimensions
 *
 * Example: sparseVectorCreate([0, 2, 5], [0.5, 0.3, 0.8]) → SparseVector
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorSparseCreate extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.sparseCreate";

  public SQLFunctionVectorSparseCreate() {
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

    final int[] indices;
    try {
      indices = VectorUtils.toIntArray(indicesObj);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException(NAME + " indices: " + e.getMessage(), e);
    }
    final float[] values = toFloatArray(valuesObj);

    if (indices.length != values.length)
      throw new CommandSQLParsingException("Indices and values arrays must have same length");

    for (final int index : indices)
      if (index < 0)
        throw new CommandSQLParsingException(NAME + " index must be non-negative, found: " + index);

    if (params.length == 2) {
      // Infer dimensions from max index
      return new SparseVector(indices, values);
    } else {
      // Explicit dimensions. intValue() READ 2.9 AS 2 - A VECTOR OF A DIMENSION THE CALLER DID NOT ASK FOR, AND EVERY
      // LATER sparseDot/sparseNeighbors COMPUTED ON IT - AND 3000000000 AS A NEGATIVE INT (issue #10699)
      final Object dimsObj = params[2];
      if (dimsObj != null && !(dimsObj instanceof Number))
        throw new CommandSQLParsingException(NAME + " dimensions must be a number, found: " + dimsObj.getClass().getSimpleName());

      final int dimensions = parseInteger(dimsObj, "dimensions");
      if (dimensions <= 0)
        throw new CommandSQLParsingException(NAME + " dimensions must be positive, got " + dimensions);

      for (final int index : indices)
        if (index >= dimensions)
          throw new CommandSQLParsingException(
              NAME + " index " + index + " is out of bounds for dimensions " + dimensions + ": dimensions must be greater than the largest index");

      return new SparseVector(indices, values, dimensions);
    }
  }

  public String getSyntax() {
    return NAME + "(<indices_array>, <values_array> [, <dimensions>])";
  }
}
