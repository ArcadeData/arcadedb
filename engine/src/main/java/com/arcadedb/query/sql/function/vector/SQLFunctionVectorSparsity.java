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
 * Calculates the sparsity of a vector.
 * Returns the percentage of elements with absolute value below a threshold.
 *
 * Formula: Sparsity = (count of |x_i| < threshold) / n
 *
 * Example: vectorSparsity([0.01, 0.1, 0.05, 0.02], 0.06) = 0.75 (3 out of 4 below threshold)
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorSparsity extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorSparsity";

  public SQLFunctionVectorSparsity() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object vectorObj = params[0];
    final Object thresholdObj = params[1];

    if (vectorObj == null || thresholdObj == null)
      return null;

    final float[] vector = toFloatArray(vectorObj);

    if (vector.length == 0)
      throw new CommandSQLParsingException("Vector cannot be empty");

    // Parse threshold
    final float threshold;
    if (thresholdObj instanceof Number num) {
      threshold = num.floatValue();
    } else {
      throw new CommandSQLParsingException("Threshold must be a number, found: " + thresholdObj.getClass().getSimpleName());
    }

    if (threshold < 0)
      throw new CommandSQLParsingException("Threshold must be >= 0, found: " + threshold);

    // Count elements below threshold
    int sparseCount = 0;
    for (final float value : vector) {
      if (Math.abs(value) < threshold) {
        sparseCount++;
      }
    }

    // Return percentage as [0, 1]
    return (float) sparseCount / vector.length;
  }


  public String getSyntax() {
    return NAME + "(<vector>, <threshold>)";
  }
}
