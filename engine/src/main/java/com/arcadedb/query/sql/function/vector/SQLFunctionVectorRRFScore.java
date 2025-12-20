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
 * Reciprocal Rank Fusion (RRF) scoring function for combining multiple ranking lists.
 * Computes: RRF = Σ (1 / (k + rank_i)) for each ranking provided.
 *
 * Usage: vectorRRFScore(rank1, rank2, rank3, ..., k)
 * where k is the constant (default 60), and ranks are integer positions.
 *
 * Example: vectorRRFScore(1, 5, 10, 60) = 1/61 + 1/65 + 1/70 ≈ 0.0456
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorRRFScore extends SQLFunctionAbstract {
  public static final String NAME = "vectorRRFScore";
  private static final long DEFAULT_K = 60;

  public SQLFunctionVectorRRFScore() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 1)
      throw new CommandSQLParsingException(getSyntax());

    long k = DEFAULT_K;
    int rankCount = params.length;

    // Check if last parameter is 'k' constant (optional)
    if (params.length >= 2) {
      final Object lastParam = params[params.length - 1];
      if (lastParam instanceof Number num) {
        // If the last parameter is significantly larger than typical ranks (> 100), treat it as k
        final long val = num.longValue();
        if (val >= 60) {
          k = val;
          rankCount = params.length - 1;
        }
      }
    }

    // Calculate RRF score
    double rrfScore = 0.0;
    for (int i = 0; i < rankCount; i++) {
      final Object rankObj = params[i];
      if (rankObj == null)
        continue;

      long rank;
      if (rankObj instanceof Number num) {
        rank = num.longValue();
      } else {
        throw new CommandSQLParsingException("Rank values must be numbers, found: " + rankObj.getClass().getSimpleName());
      }

      if (rank <= 0)
        throw new CommandSQLParsingException("Rank values must be positive integers, found: " + rank);

      rrfScore += 1.0 / (k + rank);
    }

    return (float) rrfScore;
  }

  public String getSyntax() {
    return NAME + "(<rank1>, <rank2>, ..., [<k>])";
  }
}
