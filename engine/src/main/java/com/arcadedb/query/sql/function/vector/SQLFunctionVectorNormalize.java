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

/**
 * Normalizes a vector to unit length using L2 normalization (Euclidean norm).
 * The resulting vector has magnitude 1.0 and points in the same direction as the input.
 *
 * Uses scalar implementation which is significantly faster than JVector for typical vector sizes.
 * JVector overhead from object allocation and conversion dominates actual computation cost.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorNormalize extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorNormalize";

  public SQLFunctionVectorNormalize() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 1)
      throw new CommandSQLParsingException(getSyntax());

    final Object vector = params[0];

    if (vector == null)
      throw new CommandSQLParsingException("Vector cannot be null");

    final float[] v = toFloatArray(vector);

    if (v.length == 0)
      throw new CommandSQLParsingException("Vector cannot be empty");

    // Calculate L2 norm (magnitude)
    double magnitude = 0.0;
    for (final float component : v) {
      magnitude += component * component;
    }
    magnitude = Math.sqrt(magnitude);

    // Handle zero vector
    if (magnitude == 0.0)
      return v;

    // Normalize: divide each component by magnitude
    final float[] result = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      result[i] = (float) (v[i] / magnitude);
    }
    return result;
  }


  public String getSyntax() {
    return "vectorNormalize(<vector>)";
  }
}
