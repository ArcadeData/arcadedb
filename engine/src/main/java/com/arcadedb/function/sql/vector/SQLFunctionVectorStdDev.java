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
 * Calculates the standard deviation of vector elements.
 * Square root of variance.
 *
 * Formula: StdDev = sqrt((1/n) * Σ(x_i - mean)^2)
 *
 * Example: vectorStdDev([1, 2, 3, 4, 5]) ≈ 1.414
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorStdDev extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.stdDev";

  public SQLFunctionVectorStdDev() {
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
    final double variance = varianceSum / vector.length;

    // Standard deviation is square root of variance
    return (float) Math.sqrt(variance);
  }


  public String getSyntax() {
    return NAME + "(<vector>)";
  }
}
