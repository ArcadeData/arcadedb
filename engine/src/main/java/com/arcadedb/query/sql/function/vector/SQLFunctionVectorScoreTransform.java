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

/**
 * Applies score transformation functions to reshape score distributions.
 * Supported transformations:
 * - LINEAR: No transformation (identity)
 * - SIGMOID: S-shaped curve to [0, 1], maps 0 to 0.5
 * - LOG: Natural logarithm (must be positive)
 * - EXP: Exponential function
 *
 * Usage: vectorScoreTransform(score, 'method')
 * Example: vectorScoreTransform(0.5, 'SIGMOID') → 0.6225
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorScoreTransform extends SQLFunctionAbstract {
  public static final String NAME = "vectorScoreTransform";

  public enum TransformMethod {
    LINEAR,
    SIGMOID,
    LOG,
    EXP
  }

  public SQLFunctionVectorScoreTransform() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object scoreObj = params[0];
    final Object methodObj = params[1];

    if (scoreObj == null || methodObj == null)
      return null;

    // Parse score
    final float score;
    if (scoreObj instanceof Number num) {
      score = num.floatValue();
    } else {
      throw new CommandSQLParsingException("Score must be a number, found: " + scoreObj.getClass().getSimpleName());
    }

    // Parse transformation method
    final String methodStr;
    if (methodObj instanceof String str) {
      methodStr = str.toUpperCase();
    } else {
      throw new CommandSQLParsingException("Method must be a string, found: " + methodObj.getClass().getSimpleName());
    }

    // Apply transformation
    try {
      final TransformMethod method = TransformMethod.valueOf(methodStr);
      return applyTransform(score, method);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException("Unknown transform method: " + methodStr + ". Supported: LINEAR, SIGMOID, LOG, EXP");
    }
  }

  private float applyTransform(final float score, final TransformMethod method) {
    return switch (method) {
      case LINEAR -> score;
      case SIGMOID -> sigmoid(score);
      case LOG -> {
        if (score <= 0)
          throw new CommandSQLParsingException("LOG transform requires positive score, found: " + score);
        yield (float) Math.log(score);
      }
      case EXP -> (float) Math.exp(score);
    };
  }

  /**
   * Sigmoid function: 1 / (1 + e^(-x))
   * Maps from (-∞, ∞) to (0, 1)
   * f(0) = 0.5, f(x) increases with x
   */
  private float sigmoid(final float x) {
    return (float) (1.0 / (1.0 + Math.exp(-x)));
  }

  public String getSyntax() {
    return NAME + "(<score>, <method>)";
  }
}
