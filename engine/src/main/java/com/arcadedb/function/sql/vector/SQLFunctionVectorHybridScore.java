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
 * Combines vector similarity score with keyword search score (BM25) using weighted average.
 * Formula: hybrid = (vector_score * alpha) + (keyword_score * (1 - alpha))
 *
 * Alpha parameter controls the weight:
 * - alpha = 0.0: Pure keyword search (returns keyword_score)
 * - alpha = 0.5: Equal weight on both modalities
 * - alpha = 1.0: Pure vector search (returns vector_score)
 *
 * Both scores are expected to be in [0, 1] range for meaningful results.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorHybridScore extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.hybridScore";

  public SQLFunctionVectorHybridScore() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final Object vectorScoreObj = params[0];
    final Object keywordScoreObj = params[1];
    final Object alphaObj = params[2];

    if (vectorScoreObj == null || keywordScoreObj == null || alphaObj == null)
      return null;

    // Parse vector score
    final float vectorScore;
    if (vectorScoreObj instanceof Number num1) {
      vectorScore = num1.floatValue();
    } else {
      throw new CommandSQLParsingException("Vector score must be a number, found: " + vectorScoreObj.getClass().getSimpleName());
    }

    // Parse keyword score
    final float keywordScore;
    if (keywordScoreObj instanceof Number num2) {
      keywordScore = num2.floatValue();
    } else {
      throw new CommandSQLParsingException("Keyword score must be a number, found: " + keywordScoreObj.getClass().getSimpleName());
    }

    // Parse alpha weight
    final float alpha;
    if (alphaObj instanceof Number num3) {
      alpha = num3.floatValue();
    } else {
      throw new CommandSQLParsingException("Alpha weight must be a number, found: " + alphaObj.getClass().getSimpleName());
    }

    // Validate alpha is in [0, 1]
    if (alpha < 0.0f || alpha > 1.0f)
      throw new CommandSQLParsingException("Alpha weight must be in [0.0, 1.0], found: " + alpha);

    // Calculate hybrid score: (vector_score * alpha) + (keyword_score * (1 - alpha))
    final float hybridScore = (vectorScore * alpha) + (keywordScore * (1.0f - alpha));

    return hybridScore;
  }

  public String getSyntax() {
    return NAME + "(<vector_score>, <keyword_score>, <alpha>)";
  }
}
