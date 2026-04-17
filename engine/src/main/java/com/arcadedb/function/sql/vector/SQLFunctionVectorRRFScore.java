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
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Map;
import java.util.Set;

/**
 * Reciprocal Rank Fusion (RRF) scoring function for combining multiple ranking lists.
 * Computes: RRF = Σ (1 / (k + rank_i)) for each ranking provided.
 *
 * Usage: vectorRRFScore(rank1, rank2, rank3, ..., k)
 * where k is the constant (default 60), and ranks are integer positions.
 *
 * Example: vectorRRFScore(1, 5, 10, 60) = 1/61 + 1/65 + 1/70 ≈ 0.0456
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorRRFScore extends SQLFunctionVectorAbstract {
  public static final String NAME      = "vector.rrfScore";
  private static final long  DEFAULT_K = 60;

  private static final Set<String> OPTIONS = Set.of("k");

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

    // Trailing options map takes precedence over the legacy "last Number >= 60 is k" heuristic.
    final Object lastParam = params[params.length - 1];
    if (lastParam instanceof Map<?, ?> rawMap) {
      final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTIONS);
      k = opts.getLong("k", DEFAULT_K);
      rankCount = params.length - 1;
    } else if (params.length >= 2 && lastParam instanceof Number num) {
      // Legacy disambiguation: treat a large trailing number as k.
      final long val = num.longValue();
      if (val >= 60) {
        k = val;
        rankCount = params.length - 1;
      }
    }

    // Calculate RRF score
    double rrfScore = 0.0;
    for (int i = 0; i < rankCount; i++) {
      final Object rankObj = params[i];
      if (rankObj == null)
        continue;

      final long rank;
      if (rankObj instanceof Number num)
        rank = num.longValue();
      else
        throw new CommandSQLParsingException("Rank values must be numbers, found: " + rankObj.getClass().getSimpleName());

      if (rank <= 0)
        throw new CommandSQLParsingException("Rank values must be positive integers, found: " + rank);

      rrfScore += 1.0 / (k + rank);
    }

    return (float) rrfScore;
  }

  public String getSyntax() {
    return NAME + "(<rank1>, <rank2>, ..., [<k> | { k: <long> }])";
  }
}
