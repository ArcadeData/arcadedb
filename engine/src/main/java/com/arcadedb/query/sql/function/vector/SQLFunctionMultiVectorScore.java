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
 * Combines multiple scores from different vectors using a fusion method.
 * Supports ColBERT-style multi-vector scoring where each token contributes independently.
 *
 * Fusion methods:
 * - MAX: Returns maximum score (ColBERT style)
 * - AVG: Returns average of all scores
 * - MIN: Returns minimum score
 * - WEIGHTED: Weighted average (requires equal-length arrays for weights and scores)
 *
 * Example (ColBERT): multiVectorScore([0.9, 0.7, 0.8], 'MAX') → 0.9
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionMultiVectorScore extends SQLFunctionAbstract {
  public static final String NAME = "MULTI_VECTOR_SCORE";

  public enum FusionMethod {
    MAX,
    AVG,
    MIN,
    WEIGHTED
  }

  public SQLFunctionMultiVectorScore() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object scoresObj = params[0];
    final Object methodObj = params[1];

    if (scoresObj == null || methodObj == null)
      return null;

    final float[] scores = toFloatArray(scoresObj);

    if (scores.length == 0)
      throw new CommandSQLParsingException("Scores array cannot be empty");

    // Parse method
    final String methodStr;
    if (methodObj instanceof String str) {
      methodStr = str.toUpperCase();
    } else {
      throw new CommandSQLParsingException("Method must be a string, found: " + methodObj.getClass().getSimpleName());
    }

    // Parse fusion method
    FusionMethod method;
    try {
      method = FusionMethod.valueOf(methodStr);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException("Unknown fusion method: " + methodStr + ". Supported: MAX, AVG, MIN, WEIGHTED");
    }

    // Handle WEIGHTED separately (needs weight array)
    if (method == FusionMethod.WEIGHTED) {
      if (params.length < 3)
        throw new CommandSQLParsingException("WEIGHTED method requires weights array: multiVectorScore(scores, 'WEIGHTED', weights)");

      final Object weightsObj = params[2];
      if (weightsObj == null)
        return null;

      final float[] weights = toFloatArray(weightsObj);
      if (weights.length != scores.length)
        throw new CommandSQLParsingException("Scores and weights arrays must have same length");

      return weightedAverage(scores, weights);
    }

    // Regular fusion methods
    return switch (method) {
      case MAX -> max(scores);
      case AVG -> avg(scores);
      case MIN -> min(scores);
      case WEIGHTED -> throw new CommandSQLParsingException("WEIGHTED requires weights parameter");
    };
  }

  private float max(final float[] scores) {
    float result = scores[0];
    for (final float score : scores) {
      if (score > result)
        result = score;
    }
    return result;
  }

  private float min(final float[] scores) {
    float result = scores[0];
    for (final float score : scores) {
      if (score < result)
        result = score;
    }
    return result;
  }

  private float avg(final float[] scores) {
    float sum = 0.0f;
    for (final float score : scores) {
      sum += score;
    }
    return sum / scores.length;
  }

  private float weightedAverage(final float[] scores, final float[] weights) {
    float weightedSum = 0.0f;
    float weightSum = 0.0f;

    for (int i = 0; i < scores.length; i++) {
      weightedSum += scores[i] * weights[i];
      weightSum += weights[i];
    }

    if (weightSum == 0.0f)
      throw new CommandSQLParsingException("Sum of weights cannot be zero");

    return weightedSum / weightSum;
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
    return "MULTI_VECTOR_SCORE(<scores_array>, <method> [, <weights_array>])";
  }
}
