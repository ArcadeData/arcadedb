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
import com.arcadedb.query.sql.executor.CommandContext;
import java.util.List;

/**
 * Checks if a vector is normalized (has unit length).
 * A normalized vector has L2 norm equal to 1 within a tolerance.
 *
 * Example: vectorIsNormalized([0.6, 0.8]) = true (norm = 1.0)
 *          vectorIsNormalized([1, 1]) = false (norm ≈ 1.414)
 *          vectorIsNormalized([0.6, 0.8], 0.01) = true (norm within tolerance)
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorIsNormalized extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.isNormalized";

  public SQLFunctionVectorIsNormalized() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || (params.length != 1 && params.length != 2))
      throw new CommandSQLParsingException(getSyntax());

    final Object vectorObj = params[0];
    if (vectorObj == null)
      return null;

    final float[] vector = toFloatArray(vectorObj);

    if (vector.length == 0)
      throw new CommandSQLParsingException("Vector cannot be empty");

    // Default tolerance: 0.001
    final float tolerance = (params.length == 2 && params[1] != null)
        ? (params[1] instanceof Number num ? num.floatValue() : 0.001f)
        : 0.001f;

    if (tolerance < 0)
      throw new CommandSQLParsingException("Tolerance must be >= 0, found: " + tolerance);

    // Calculate L2 norm
    double sumSquares = 0.0;
    for (final float value : vector) {
      sumSquares += value * value;
    }
    final float norm = (float) Math.sqrt(sumSquares);

    // Check if norm is approximately 1.0 (within tolerance)
    return Math.abs(norm - 1.0f) <= tolerance;
  }


  public String getSyntax() {
    return NAME + "(<vector> [, <tolerance>])";
  }
}
