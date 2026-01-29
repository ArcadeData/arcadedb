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
 * Aggregate function that computes element-wise maximum of vectors.
 * Returns a vector where each component is the maximum value of corresponding components across all input vectors.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorMax extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorMax";

  private float[] maxVector;
  private int dimensions;

  public SQLFunctionVectorMax() {
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
    if (maxVector == null) {
      dimensions = v.length;
      maxVector = v.clone();
    } else if (maxVector.length != v.length) {
      throw new CommandSQLParsingException("All vectors must have the same dimension");
    } else {
      // Update maximums
      for (int i = 0; i < v.length; i++) {
        maxVector[i] = Math.max(maxVector[i], v[i]);
      }
    }

    return null; // Aggregate functions don't return per-row, only at the end
  }

  @Override
  public Object getResult() {
    if (maxVector == null)
      return null;

    return maxVector.clone();
  }


  public String getSyntax() {
    return NAME + "(<vector_column>)";
  }
}
