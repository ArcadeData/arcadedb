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

/**
 * Aggregate function that computes element-wise sum of vectors.
 * Returns a vector where each component is the sum of corresponding components across all input vectors.
 * <p>
 * The sum is accumulated in double and narrowed once: a float running sum stops growing next to its addends (issue #12852).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorSum extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.sum";

  private double[] sumVector;

  public SQLFunctionVectorSum() {
    super(NAME);
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    checkArity(params);

    final Object vector = params[0];

    if (vector == null)
      return null;

    final float[] v = toFloatArray(vector);

    // Initialize on first call
    if (sumVector == null)
      sumVector = new double[v.length];
    else if (sumVector.length != v.length)
      throw new CommandSQLParsingException("All vectors must have the same dimension");

    // Add to sum
    for (int i = 0; i < v.length; i++)
      sumVector[i] += v[i];

    return null; // Aggregate functions don't return per-row, only at the end
  }

  @Override
  public Object getResult() {
    if (sumVector == null)
      return null;

    // Narrowed once: a sum beyond the float range is Infinity, as the true sum is
    final float[] result = new float[sumVector.length];
    for (int i = 0; i < result.length; i++)
      result[i] = (float) sumVector[i];
    return result;
  }

  public String getSyntax() {
    return NAME + "(<vector_column>)";
  }
}
