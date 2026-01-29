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
import java.util.List;

/**
 * Calculates the L∞ norm (Chebyshev norm or infinity norm) of a vector.
 * Returns the maximum absolute value of any element.
 *
 * Formula: L∞ = max(|x_i|)
 *
 * Example: vectorLInfNorm([3, 4, 2]) = 4
 *          vectorLInfNorm([-1, -5, 3]) = 5
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorLInfNorm extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.lInfNorm";

  public SQLFunctionVectorLInfNorm() {
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

    // Calculate L∞ norm: max absolute value
    float maxAbs = 0.0f;
    for (final float value : vector) {
      final float abs = Math.abs(value);
      if (abs > maxAbs) {
        maxAbs = abs;
      }
    }

    return maxAbs;
  }


  public String getSyntax() {
    return NAME + "(<vector>)";
  }
}
