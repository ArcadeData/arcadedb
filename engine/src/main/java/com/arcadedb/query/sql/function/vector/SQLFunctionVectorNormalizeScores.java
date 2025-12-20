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
 * Normalizes scores using min-max normalization to [0, 1] range.
 * Formula: normalized = (value - min) / (max - min)
 *
 * Accepts array-like input: float[], Object[], or List.
 * Returns float[] with all values normalized to [0, 1] range.
 *
 * Edge case: If all values are the same, returns array of 0.5 (midpoint).
 * If only one value, returns array of [1.0].
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorNormalizeScores extends SQLFunctionAbstract {
  public static final String NAME = "vectorNormalizeScores";

  public SQLFunctionVectorNormalizeScores() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length == 0)
      throw new CommandSQLParsingException(getSyntax());

    final Object scoresObj = params[0];
    if (scoresObj == null)
      return null;

    final float[] scores = toFloatArray(scoresObj);

    if (scores.length == 0)
      return scores;

    // Find min and max values
    float min = scores[0];
    float max = scores[0];
    for (final float score : scores) {
      if (score < min)
        min = score;
      if (score > max)
        max = score;
    }

    // Handle edge case: all values are the same
    if (min == max) {
      final float[] result = new float[scores.length];
      for (int i = 0; i < scores.length; i++) {
        result[i] = 0.5f; // Midpoint for uniform values
      }
      return result;
    }

    // Normalize to [0, 1]
    final float[] normalized = new float[scores.length];
    final float range = max - min;
    for (int i = 0; i < scores.length; i++) {
      normalized[i] = (scores[i] - min) / range;
    }

    return normalized;
  }

  private float[] toFloatArray(final Object scores) {
    if (scores instanceof float[] floatArray) {
      return floatArray;
    } else if (scores instanceof Object[] objArray) {
      final float[] result = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Score values must be numbers, found: " + objArray[i].getClass().getSimpleName());
        }
      }
      return result;
    } else if (scores instanceof List<?> list) {
      final float[] result = new float[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (elem instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Score values must be numbers, found: " + elem.getClass().getSimpleName());
        }
      }
      return result;
    } else {
      throw new CommandSQLParsingException("Scores must be an array or list, found: " + scores.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return NAME + "(<scores_array>)";
  }
}
